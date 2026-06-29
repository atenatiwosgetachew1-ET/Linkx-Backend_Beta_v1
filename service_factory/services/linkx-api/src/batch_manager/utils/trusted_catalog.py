from session_config_store import load_session_config

TRUSTED_CATALOG_MAX_ENTRIES = 500
TRUSTED_CATALOG_MAX_FIELDS_PER_ENTRY = 20
TRUSTED_CATALOG_MAX_KEY_LENGTH = 128
TRUSTED_CATALOG_MAX_STRING_VALUE_LENGTH = 512


class TrustedCatalogValidationError(ValueError):
    pass


def _is_scalar(value):
    return isinstance(value, (str, int, float, bool))


def normalize_trusted_catalog(value):
    if value in (None, '', []):
        return []
    if not isinstance(value, list):
        raise TrustedCatalogValidationError('trusted_catalog must be a list of objects')
    if len(value) > TRUSTED_CATALOG_MAX_ENTRIES:
        raise TrustedCatalogValidationError(
            f'trusted_catalog exceeds maximum of {TRUSTED_CATALOG_MAX_ENTRIES} entries'
        )

    normalized = []
    for index, entry in enumerate(value):
        if not isinstance(entry, dict):
            raise TrustedCatalogValidationError(f'trusted_catalog[{index}] must be an object')
        if not entry:
            raise TrustedCatalogValidationError(f'trusted_catalog[{index}] must not be empty')
        if len(entry) > TRUSTED_CATALOG_MAX_FIELDS_PER_ENTRY:
            raise TrustedCatalogValidationError(
                f'trusted_catalog[{index}] exceeds maximum of {TRUSTED_CATALOG_MAX_FIELDS_PER_ENTRY} fields'
            )

        normalized_entry = {}
        for raw_key, raw_value in entry.items():
            key = str(raw_key or '').strip()
            if not key:
                raise TrustedCatalogValidationError(f'trusted_catalog[{index}] contains an empty key')
            if len(key) > TRUSTED_CATALOG_MAX_KEY_LENGTH:
                raise TrustedCatalogValidationError(
                    f"trusted_catalog[{index}] key '{key[:32]}' exceeds maximum length"
                )
            if not _is_scalar(raw_value):
                raise TrustedCatalogValidationError(
                    f'trusted_catalog[{index}].{key} must be a scalar value'
                )
            if isinstance(raw_value, str):
                cleaned_value = raw_value.strip()
                if not cleaned_value:
                    raise TrustedCatalogValidationError(
                        f'trusted_catalog[{index}].{key} must not be empty'
                    )
                if len(cleaned_value) > TRUSTED_CATALOG_MAX_STRING_VALUE_LENGTH:
                    raise TrustedCatalogValidationError(
                        f'trusted_catalog[{index}].{key} exceeds maximum length'
                    )
                normalized_entry[key] = cleaned_value
            else:
                normalized_entry[key] = raw_value
        normalized.append(normalized_entry)
    return normalized


def load_session_trusted_catalog(session_id):
    config = load_session_config(session_id) or {}
    return normalize_trusted_catalog(config.get('trusted_catalog'))


def trusted_catalog_cypher_entries(value):
    entries = normalize_trusted_catalog(value)
    return [{key: str(item) for key, item in entry.items()} for entry in entries]
