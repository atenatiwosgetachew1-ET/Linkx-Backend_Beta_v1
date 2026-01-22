from pyhive import hive
import re
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import reduce
from pyspark.sql.functions import col, lit
from TCLIService.ttypes import TOperationState
import logging



def get_hive_connection(host, port=10000):
    return hive.Connection(host=host, port=port)

def run_hive_query(host, query):
    conn = hive.Connection(host=host, port=10000)
    cursor = conn.cursor()

    # extract column label
    match_labels = re.search(r"select\s+(.*?)\s+from", query, re.IGNORECASE)
    label = match_labels.group(1).strip() if match_labels else "query"

    try:
        cursor.execute(query)
        results = cursor.fetchall()
        return [{"response": f"({len(results)}) query results for '{label}'", "status": "success"}]
    except Exception as e:
        return [{"response": str(e), "status": "failed"}]
    finally:
        cursor.close()
        conn.close()

def hive_dynamic_search(host, keyword):
    """
    Equivalent to your original column scan logic.
    """
    conn = hive.Connection(host=host, port=10000)
    cursor = conn.cursor()

    table = "bank_db.ctr_cleaned"

    try:
        # Load all columns & types
        cursor.execute(f"DESCRIBE {table}")
        columns_info = cursor.fetchall()

        text_columns = [
            row[0].strip()
            for row in columns_info
            if row[0] and any(t in row[1].lower() for t in ["string", "varchar", "char"])
        ]

        if not text_columns:
            return [{"response": "No text columns found.", "status": "failed"}]

        # fetch latest partitions
        cursor.execute(f"SHOW PARTITIONS {table}")
        partitions = [p[0] for p in cursor.fetchall()]

        if partitions:
            latest = max(partitions)
            partition_filter = " AND ".join(
                p.replace("=", "='") + "'" for p in latest.split("/")
            )
        else:
            partition_filter = "1=1"

        like_conditions = [f"INSTR(`{col}`, '{keyword}') > 0" for col in text_columns]
        where = " OR ".join(like_conditions)

        query = f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE {partition_filter}
              AND ({where})
        """

        cursor.execute(query)
        count = cursor.fetchone()[0]

        return [{
            "response": f"({count}) results for '{keyword}'",
            "status": "success",
        }]

    except Exception as e:
        return [{"response": str(e), "status": "failed"}]

    finally:
        cursor.close()
        conn.close()

def count_column(host, table, column, keyword, date):
    storage_ip = host.replace(":9870", "")
    conn = hive.Connection(host=storage_ip, port=10000)
    cursor = conn.cursor()
    partition_column = "transactiondate_partition"
    
    where_clause = f"INSTR(LOWER(CAST(`{column}` AS STRING)), LOWER('{keyword}')) > 0"
    if date:
        where_clause = f"{partition_column} = '{date}' AND ({where_clause})"
    
    count_query = f"SELECT COUNT(*) FROM {table} WHERE {where_clause}"
    cursor.execute(count_query)
    result = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    return result

def hive_keyword_count(host,table,keyword,date,limit=1):
    # --- FIX: Ensure table is a string ---
    print("hive count called")
    if isinstance(table, (list, tuple)):
        table = table[0]
    table = str(table).strip()

    search_columns = [
        "accownername",
        "transactionid",
        "businessmobileno",
        "accountno",
        "benfullname",
        "benaccountno",
        "bentelno"
    ]

    storage_ip = host.replace(":9870", "")
    conn = hive.Connection(host=storage_ip, port=10000)
    cursor = conn.cursor()

    # ---- DATE FILTER ----
    if date:
        date_filter = f"SUBSTRING(transactiondate, 1, 10) = '{date}'"
    else:
        date_filter = "1=1"

    # ---- COLUMN SCAN (Use LIKE for speed) ----
    keyword = keyword.lower()
    keyword_conditions = " OR ".join(
        [f"LOWER({col}) LIKE '%{keyword}%'" for col in search_columns]
    )

    where_clause = f"{date_filter} AND ({keyword_conditions})"

    # ---- LIMIT (Allowed on COUNT via subquery) ----
    limit_clause = f" LIMIT {limit}" if limit and limit > 0 else ""

    # ---- FINAL QUERY ----
    count_query = f"""
        SELECT COUNT(*) FROM (
            SELECT 1
            FROM {table}
            WHERE {where_clause}
            {limit_clause}
        ) t
    """

    try:
        cursor.execute(count_query)
        count = cursor.fetchone()[0]

        return {
            "keyword": keyword,
            "date": date,
            "results": count,
            "columns": search_columns,
            "limit_used": limit,
            "status": "success"
        }

    except Exception as e:
        print("except:", e)
        return {
            "keyword": keyword,
            "date": date,
            "error": str(e),
            "status": "failed"
        }

    finally:
        cursor.close()
        conn.close()

def hive_keyword_search(host, tables, search_rows, date=None):
    """
    Returns only the total number of rows matching the search criteria (keyword search).
    Does not return actual result rows.
    """

    # -------------------- Validate tables --------------------
    if isinstance(tables, str):
        tables = [tables]
    elif not isinstance(tables, (list, tuple)) or not tables:
        return {
            "status": "failed",
            "results": 0,
            "has_more": 0,
            "offset": 0,
            "limit": 0,
            "error": "tables must be a non-empty list or string",
            "tables_searched": tables,
            "search_params": {}
        }

    # -------------------- Flatten search_rows --------------------
    payload = {
        row["field"].strip(): str(row["value"]).strip()
        for row in search_rows
        if row.get("field") and row.get("value")
    }

    if not payload:
        return {
            "status": "failed",
            "results": 0,
            "has_more": 0,
            "offset": 0,
            "limit": 0,
            "error": "No valid search parameters",
            "tables_searched": tables,
            "search_params": payload
        }

    # Add date filter if provided
    if date:
        payload["date"] = date  # API must support this

    # -------------------- API endpoint --------------------
    API_URL = "http://172.20.137.129:5000/api/search"

    try:
        # Request search from API
        response = requests.post(API_URL, json=payload, timeout=30)
        response.raise_for_status()
        api_result = response.json()

        # -------------------- Return only the count --------------------
        return {
            "status": "success",
            "results": api_result.get("total", 0),
            "has_more": 0,
            "offset": 0,
            "limit": 0,
            "messsage": "Results found",
            "tables_searched": tables,
            "search_params": payload
        }

    except requests.exceptions.Timeout:
        return {
            "status": "failed",
            "results": 0,
            "has_more": 0,
            "offset": 0,
            "limit": 0,
            "error": "API request timed out",
            "tables_searched": tables,
            "search_params": payload
        }

    except requests.exceptions.ConnectionError:
        return {
            "status": "failed",
            "results": 0,
            "has_more": 0,
            "offset": 0,
            "limit": 0,
            "error": "API server not reachable",
            "tables_searched": tables,
            "search_params": payload
        }

    except requests.exceptions.HTTPError as e:
        return {
            "status": "failed",
            "results": 0,
            "has_more": 0,
            "offset": 0,
            "limit": 0,
            "error": f"API returned error: {str(e)}",
            "tables_searched": tables,
            "search_params": payload
        }

# def hive_keyword_search_count(storage_ip, tables, search_rows, date=None, offset=0):
#     """
#     Count matching records in Hive tables using the Spark session already running on the API server.
    
#     :param storage_ip: optional, for logging or reference
#     :param tables: list of Hive table names
#     :param search_rows: list of {"field": <field>, "value": <keyword>}
#     :param date: optional date string to filter TRANSACTIONDATE
#     :param offset: for API response structure
#     :return: dict with API-compatible structure
#     """
#     # Use the Spark session that is already initialized on the API server
#     global spark  # assuming your API has a global Spark session called `spark`
    
#     combined_results = []

#     for table in tables:
#         try:
#             df = spark.table(table)  # use the existing Spark session

#             # Filter by date if provided
#             if date:
#                 df = df.filter(col("TRANSACTIONDATE") == lit(date))

#             # Apply keyword filters
#             if search_rows:
#                 keyword_filter = None
#                 for row in search_rows:
#                     field = row.get("field")
#                     value = row.get("value")
#                     if field and value:
#                         condition = col(field).contains(value)
#                         keyword_filter = condition if keyword_filter is None else (keyword_filter | condition)

#                 if keyword_filter is not None:
#                     df = df.filter(keyword_filter)

#             # Collect all matching rows
#             result_rows = [row.asDict() for row in df.collect()]
#             combined_results.extend(result_rows)

#         except Exception as e:
#             combined_results.append({"error": f"Error querying table {table} (storage {storage_ip}): {str(e)}"})

#     response = {
#         "results": combined_results,
#         "has_more": False,
#         "offset": offset,
#         "limit": len(combined_results),
#         "message": f"Found {len(combined_results)} matching records"
#     }

#     return response

def hive_keyword_search_count(storage_ip, keyword, search_rows, hive_port, tables, date=None, max_limit=1000000):
    """
    Count matching transactions using Hive (direct HQL, no Spark),
    limiting the count to `max_limit`.

    :param storage_ip: Hive server host/IP
    :param search_rows: list of {"field": ..., "value": ...}
    :param tables: list of Hive table names
    :param date: transactiondate_partition (YYYY-MM-DD)
    :param max_limit: maximum number of rows to count
    :return: dict with count result
    """
    print("hive search count called")
    if not isinstance(search_rows, list) or not search_rows:
        return {"error": "search_rows must be a non-empty list"}

    total_count = 0

    # Connect to Hive server (Thrift)
    try:
        conn = hive.Connection(host=storage_ip, port=hive_port, username='hive')
        cursor = conn.cursor()
    except Exception as e:
        logging.error(f"Failed to connect to Hive at {storage_ip}: {e}")
        return {"error": f"Could not connect to Hive at {storage_ip}"}

    for table in tables:
        where_clauses = []

        # Partition filter
        if date:
            where_clauses.append(f"transactiondate_partition = '{date}'")

        # Keyword OR conditions
        or_conditions = []
        for row in search_rows:
            field = row.get("field")
            value = row.get("value")
            if field and value:
                or_conditions.append(f"{field} LIKE '%{value}%'")
        if or_conditions:
            where_clauses.append("(" + " OR ".join(or_conditions) + ")")

        if not where_clauses:
            continue

        # Build HQL query to fetch up to max_limit rows
        hql_query = f"""
        SELECT 1 FROM {table}
        WHERE {" AND ".join(where_clauses)}
        LIMIT {max_limit}
        """
        logging.info(f"Executing Hive query: {hql_query}")

        try:
            cursor.execute(hql_query)
            rows = cursor.fetchall()
            count = len(rows)
            total_count += count
        except Exception as e:
            logging.error(f"Failed to execute query on {table}: {e}")
            continue

    # Close connection
    cursor.close()
    conn.close()

    return {
        "results": [
            {
                "name": f"Results found for '{keyword}'",                        
                "size": total_count,
            }
        ],
        "has_more": 1 if total_count >= max_limit else 0,
        "offset": 0,
        "limit": max_limit,
        "message": f"{total_count} results found (limited to {max_limit})"
    }

def load_hive_rows(storage_ip,hive_port,spark,search_columns,tables,hive_keywords,date=None,limit=1000000):
    """
    Load data from Hive using Spark SQL (Spark-native, no pyhive).

    :param storage_ip: Hive server IP (kept for compatibility, not used)
    :param hive_port: Hive port (kept for compatibility, not used)
    :param spark: SparkSession
    :param search_columns: list of dicts {"field": ...}
    :param tables: list of Hive table names
    :param hive_keywords: list of search keywords
    :param date: optional partition date filter (YYYY-MM-DD)
    :param limit: maximum rows per table to fetch
    :return: Spark DataFrame or None if no data
    """
    if not tables or not hive_keywords or not search_columns:
        print("No tables, keywords, or search columns provided")
        return None

    spark_dfs = []

    for table in tables:
        # Partition filter
        filters = []
        if date:
            filters.append(f"transactiondate_partition = '{date}'")

        # Keyword search across columns
        keyword_conditions = []
        for keyword in hive_keywords:
            col_conditions = [
                f"`{col['field']}` LIKE '%{keyword}%'" for col in search_columns if col.get("field")
            ]
            if col_conditions:
                keyword_conditions.append("(" + " OR ".join(col_conditions) + ")")

        if keyword_conditions:
            filters.append("(" + " OR ".join(keyword_conditions) + ")")

        if not filters:
            continue

        query = f"""
        SELECT *
        FROM {table}
        WHERE {" AND ".join(filters)}
        LIMIT {limit}
        """

        try:
            print(f"Executing Spark SQL on {table}")
            df = spark.sql(query)
            if df.count() > 0:
                spark_dfs.append(df)
            else:
                print(f"No rows returned for table {table}")

        except Exception as e:
            print(f"Hive Spark SQL failed ({table}): {e}")

    if not spark_dfs:
        print("No data frames generated")
        return None

    print("Merging", len(spark_dfs), "DataFrames")

    merged_df = reduce(
        lambda d1, d2: d1.unionByName(d2, allowMissingColumns=True),
        spark_dfs
    )

    return merged_df
