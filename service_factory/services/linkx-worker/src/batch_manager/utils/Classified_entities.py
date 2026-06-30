from session_config_store import load_session_config


TRUSTED_CATALOG_MAX_ENTRIES = 500
TRUSTED_CATALOG_MAX_FIELDS_PER_ENTRY = 20
TRUSTED_CATALOG_MAX_KEY_LENGTH = 128
TRUSTED_CATALOG_MAX_STRING_VALUE_LENGTH = 512


class TrustedEntitiesValidationError(ValueError):
    pass


def _is_scalar(value):
    return isinstance(value, (str, int, float, bool))


def normalize_entity_catalog(value, field_name):
    if value in (None, '', []):
        return []
    if not isinstance(value, list):
        raise TrustedEntitiesValidationError(f'{field_name} must be a list of objects')
    if len(value) > TRUSTED_CATALOG_MAX_ENTRIES:
        raise TrustedEntitiesValidationError(
            f'{field_name} exceeds maximum of {TRUSTED_CATALOG_MAX_ENTRIES} entries'
        )

    normalized = []
    for index, entry in enumerate(value):
        if not isinstance(entry, dict):
            raise TrustedEntitiesValidationError(f'{field_name}[{index}] must be an object')
        if not entry:
            raise TrustedEntitiesValidationError(f'{field_name}[{index}] must not be empty')
        if len(entry) > TRUSTED_CATALOG_MAX_FIELDS_PER_ENTRY:
            raise TrustedEntitiesValidationError(
                f'{field_name}[{index}] exceeds maximum of {TRUSTED_CATALOG_MAX_FIELDS_PER_ENTRY} fields'
            )

        normalized_entry = {}
        for raw_key, raw_value in entry.items():
            key = str(raw_key or '').strip()
            if not key:
                raise TrustedEntitiesValidationError(f'{field_name}[{index}] contains an empty key')
            if len(key) > TRUSTED_CATALOG_MAX_KEY_LENGTH:
                raise TrustedEntitiesValidationError(
                    f"{field_name}[{index}] key '{key[:32]}' exceeds maximum length"
                )
            if not _is_scalar(raw_value):
                raise TrustedEntitiesValidationError(
                    f'{field_name}[{index}].{key} must be a scalar value'
                )
            if isinstance(raw_value, str):
                cleaned_value = raw_value.strip()
                if not cleaned_value:
                    raise TrustedEntitiesValidationError(
                        f'{field_name}[{index}].{key} must not be empty'
                    )
                if len(cleaned_value) > TRUSTED_CATALOG_MAX_STRING_VALUE_LENGTH:
                    raise TrustedEntitiesValidationError(
                        f'{field_name}[{index}].{key} exceeds maximum length'
                    )
                normalized_entry[key] = cleaned_value
            else:
                normalized_entry[key] = raw_value
        normalized.append(normalized_entry)
    return normalized


def normalize_trusted_entities(value):
    return normalize_entity_catalog(value, 'trusted_entities')


def normalize_risk_entities(value):
    return normalize_entity_catalog(value, 'risk_entities')


def load_session_trusted_entities(session_id):
    config = load_session_config(session_id) or {}
    value = config.get('trusted_entities')
    if value in (None, '', []):
        value = config.get('trusted_catalog')
    return normalize_trusted_entities(value)


def load_session_risk_entities(session_id):
    config = load_session_config(session_id) or {}
    return normalize_risk_entities(config.get('risk_entities'))


def entity_catalog_cypher_entries(value, field_name):
    entries = normalize_entity_catalog(value, field_name)
    return [{key: str(item) for key, item in entry.items()} for entry in entries]


def trusted_entities_cypher_entries(value):
    return entity_catalog_cypher_entries(value, 'trusted_entities')


def risk_entities_cypher_entries(value):
    return entity_catalog_cypher_entries(value, 'risk_entities')
