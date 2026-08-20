import json

import pandas as pd
import requests
from kafka import KafkaConsumer, TopicPartition

from batch_manager.processing.api_source_loader import clean_record
from batch_manager.utils.spark_utils import get_spark_session


DEFAULT_EXCLUDE_KEYS = [
    "remarks", "notes", "description", "comments", "showLetter", "portraitURL",
    "color", "displayStatus", "topTag", "topTagTip", "newUserTag",
    "newUserTagTip", "legal", "retry", "msg", "code", "additionalAd",
    "tradeLimitDialogType", "self", "tradeLimitTip",
]


def records_to_dataframe(data, use_spark=False, items_key="items", exclude_keys=None, capitalized_keys_only=False):
    records = _records_from_payload(data, items_key, capitalized_keys_only=capitalized_keys_only)
    exclude_keys = exclude_keys or DEFAULT_EXCLUDE_KEYS
    cleaned_records = [
        clean_record(record, exclude_keys) if isinstance(record, dict) else {"value": record}
        for record in records
    ]

    if use_spark:
        spark = get_spark_session()
        return spark.createDataFrame(cleaned_records)

    return pd.DataFrame(cleaned_records)


def load_realtime_api(url, session_id=None, params=None, headers=None, use_spark=False, items_key="items"):
    headers = headers or {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    response = requests.get(url, headers=headers, params=params or {}, timeout=15)
    response.raise_for_status()
    data = response.json()
    return records_to_dataframe(data, use_spark=use_spark, items_key=items_key)


def load_kafka_batch_messages(
    broker_url,
    topic,
    session_id=None,
    use_spark=False,
    max_messages=1000,
    max_rows=None,
    timeout_ms=10000,
    from_beginning=False,
):
    messages = read_kafka_batch_messages(
        broker_url,
        topic,
        max_messages=max_messages,
        timeout_ms=timeout_ms,
        from_beginning=from_beginning,
    )
    if not messages:
        return None
    records = []
    for message in messages:
        records.extend(_records_from_payload(message, capitalized_keys_only=True))
    if max_rows:
        records = records[-max(1, int(max_rows)):]
    return records_to_dataframe(records, use_spark=use_spark, capitalized_keys_only=True)


def read_kafka_batch_messages(broker_url, topic, max_messages=1000, timeout_ms=10000, from_beginning=False):
    max_messages = max(1, int(max_messages or 1000))
    timeout_ms = max(1000, int(timeout_ms or 10000))
    print(f"[kafka_batch] connecting broker={broker_url} topic={topic} max_messages={max_messages} timeout_ms={timeout_ms}", flush=True)
    consumer = KafkaConsumer(
        bootstrap_servers=[broker_url],
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_ms,
        request_timeout_ms=max(timeout_ms + 5000, 10000),
        metadata_max_age_ms=5000,
        value_deserializer=_deserialize_value,
    )
    try:
        partitions = consumer.partitions_for_topic(topic)
        retries = 0
        while not partitions and retries < 5:
            import time
            time.sleep(0.5)
            consumer.topics()
            partitions = consumer.partitions_for_topic(topic)
            retries += 1
            
        if not partitions:
            print(f"[kafka_batch] no partitions found for topic={topic}", flush=True)
            return []
        print(f"[kafka_batch] partitions={sorted(partitions)}", flush=True)

        topic_partitions = [TopicPartition(topic, partition) for partition in partitions]
        consumer.assign(topic_partitions)
        beginning_offsets = consumer.beginning_offsets(topic_partitions, timeout_ms=timeout_ms)
        end_offsets = consumer.end_offsets(topic_partitions, timeout_ms=timeout_ms)
        print(f"[kafka_batch] beginning_offsets={beginning_offsets} end_offsets={end_offsets}", flush=True)

        for topic_partition in topic_partitions:
            beginning = beginning_offsets.get(topic_partition, 0)
            end = end_offsets.get(topic_partition, 0)
            if from_beginning:
                consumer.seek(topic_partition, beginning)
            else:
                consumer.seek(topic_partition, max(beginning, end - max_messages))

        collected = []
        empty_polls = 0
        while len(collected) < max_messages and empty_polls < 3:
            polled = consumer.poll(timeout_ms=timeout_ms, max_records=max_messages - len(collected))
            if not polled:
                empty_polls += 1
                continue
            for records in polled.values():
                collected.extend(records)
                if len(collected) >= max_messages:
                    break

        collected.sort(key=lambda record: (record.timestamp or 0, record.partition, record.offset))
        print(f"[kafka_batch] collected={len(collected)}", flush=True)
        return [record.value for record in collected[-max_messages:]]
    finally:
        consumer.close()


def iter_kafka_messages(broker_url, topic, stop_event=None, poll_timeout_ms=1000):
    consumer = KafkaConsumer(
        bootstrap_servers=[broker_url],
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
        value_deserializer=_deserialize_value,
    )
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            consumer.topics()
            partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return

        topic_partitions = [TopicPartition(topic, partition) for partition in partitions]
        consumer.assign(topic_partitions)
        consumer.seek_to_end(*topic_partitions)

        while not (stop_event and stop_event.is_set()):
            polled = consumer.poll(timeout_ms=poll_timeout_ms)
            for records in polled.values():
                for record in records:
                    yield record.value
    finally:
        consumer.close()


def iter_api_messages(url, stop_event=None, interval_seconds=5, headers=None, params=None):
    last_fingerprint = None
    headers = headers or {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    while not (stop_event and stop_event.is_set()):
        response = requests.get(url, headers=headers, params=params or {}, timeout=15)
        response.raise_for_status()
        data = response.json()
        fingerprint = json.dumps(data, sort_keys=True, default=str)
        if fingerprint != last_fingerprint:
            last_fingerprint = fingerprint
            yield data
        if stop_event and stop_event.wait(interval_seconds):
            break


def load_latest_kafka_message(broker_url, topic, session_id=None, use_spark=False, timeout_ms=5000):
    message = read_latest_kafka_message(broker_url, topic, timeout_ms=timeout_ms)
    if message is None:
        return None
    return records_to_dataframe(message, use_spark=use_spark, capitalized_keys_only=True)


def read_latest_kafka_message(broker_url, topic, timeout_ms=5000):
    consumer = KafkaConsumer(
        bootstrap_servers=[broker_url],
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_ms,
        request_timeout_ms=max(timeout_ms, 3000),
        value_deserializer=_deserialize_value,
    )
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            consumer.topics()
            partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return None

        topic_partitions = [TopicPartition(topic, partition) for partition in partitions]
        consumer.assign(topic_partitions)
        end_offsets = consumer.end_offsets(topic_partitions)

        readable_partitions = [
            topic_partition
            for topic_partition in topic_partitions
            if end_offsets.get(topic_partition, 0) > 0
        ]
        if not readable_partitions:
            return None

        for topic_partition in readable_partitions:
            consumer.seek(topic_partition, end_offsets[topic_partition] - 1)

        polled = consumer.poll(timeout_ms=timeout_ms, max_records=len(readable_partitions))
        messages = [
            record
            for records in polled.values()
            for record in records
        ]
        if not messages:
            return None

        latest = max(messages, key=lambda record: (record.timestamp or 0, record.offset))
        return latest.value
    finally:
        consumer.close()


def _records_from_payload(data, items_key="items", capitalized_keys_only=False):
    if isinstance(data, dict):
        records = data.get("transactions") or data.get(items_key, data)
    else:
        records = data

    if isinstance(records, list):
        output = records
    elif isinstance(records, dict):
        output = [records]
    else:
        output = [{"message": records}]

    if capitalized_keys_only:
        return [
            record
            for record in (_capitalized_record(record) for record in output if isinstance(record, dict))
            if record
        ]
    return output


def _capitalized_record(record):
    return {
        key: value
        for key, value in record.items()
        if isinstance(key, str)
        and key.upper() == key
        and isinstance(value, (str, int, float, bool, type(None)))
    }


def _deserialize_value(value):
    text = value.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"message": text}
