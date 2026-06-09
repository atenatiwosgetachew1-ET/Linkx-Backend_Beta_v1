import json
import re
from jsonschema import validate, ValidationError

SUPPORTED_OPERATORS = {
    "EQUALS",
    "NOT_EQUALS",
    "GREATER_THAN",
    "GREATER_THAN_OR_EQUALS",
    "LESS_THAN",
    "LESS_THAN_OR_EQUALS",
    "CONTAINS",
    "STARTS_WITH",
    "ENDS_WITH",
}

SUPPORTED_RULE_TYPES = {"SEQUENTIAL", "PREVIOUS", "SELF", "CIRCULAR", "WINDOWED", "PAIRWISE"}
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
COLOR_PATTERN = re.compile(r"^#[0-9A-Fa-f]{3}(?:[0-9A-Fa-f]{3})?$")
MAX_RULES = 50

RULE_SCHEMA = {
    "type": "object",
    "properties": {
        "rule_name": {"type": "string"},
        "node_label": {"type": "string"},
        "rules": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["id", "type", "match", "relationship"],
                "properties": {
                    "id": {"type": "string"},
                    "description": {"type": "string"},
                    "type": {"type": "string", "enum": sorted(SUPPORTED_RULE_TYPES)},
                    "match": {"type": "object"},
                    "filter": {"type": "object"},
                    "relationship": {"type": "object"},
                    "thresholds": {"type": "object"},
                    "advanced": {"type": "object"}
                }
            }
        }
    },
    "required": ["rule_name", "rules"]
}


def _validate_identifier(value, field):
    if value is None:
        return
    if not isinstance(value, str) or not IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"Invalid {field}: {value}")


def _validate_identifier_list(values, field):
    if values is None:
        return
    if not isinstance(values, list):
        raise ValueError(f"{field} must be a list")
    for value in values:
        _validate_identifier(value, field)


def _validate_operator(value, field):
    if value is None:
        return
    if str(value).upper() not in SUPPORTED_OPERATORS:
        raise ValueError(f"Invalid {field}: {value}")


def _validate_scalar(value, field):
    if isinstance(value, (dict, list)):
        raise ValueError(f"{field} must be a scalar value")


def _validate_match(rule):
    match = rule.get("match", {}) or {}
    _validate_identifier(match.get("node_label"), "match.node_label")
    _validate_identifier_list(match.get("group_by"), "match.group_by")
    _validate_identifier_list(match.get("order_by"), "match.order_by")
    _validate_identifier(match.get("source_property"), "match.source_property")
    _validate_identifier(match.get("target_property"), "match.target_property")

    window = match.get("window", {}) or {}
    if "size" in window:
        try:
            size = int(window["size"])
        except (TypeError, ValueError):
            raise ValueError(f"Rule '{rule.get('id', '')}' has invalid window.size")
        if size < 1 or size > 100:
            raise ValueError(f"Rule '{rule.get('id', '')}' window.size must be between 1 and 100")
    if "direction" in window and str(window["direction"]).upper() not in {"FORWARD", "BACKWARD"}:
        raise ValueError(f"Rule '{rule.get('id', '')}' has invalid window.direction")


def _validate_filter(rule):
    filter_config = rule.get("filter", {}) or {}
    mode = str(filter_config.get("mode", "OPTIONAL")).upper()
    if mode not in {"OPTIONAL", "REQUIRED", "DISABLED"}:
        raise ValueError(f"Rule '{rule.get('id', '')}' has invalid filter.mode")

    for condition in filter_config.get("conditions", []) or []:
        _validate_identifier(condition.get("property"), "filter.conditions.property")
        _validate_operator(condition.get("operator", "EQUALS"), "filter.conditions.operator")
        _validate_scalar(condition.get("value"), "filter.conditions.value")


def _validate_thresholds(rule):
    thresholds = rule.get("thresholds", {}) or {}
    if not thresholds:
        return
    if thresholds.get("enabled"):
        _validate_identifier(thresholds.get("metric"), "thresholds.metric")
        _validate_operator(thresholds.get("operator", "GREATER_THAN"), "thresholds.operator")
        if str(thresholds.get("operator", "GREATER_THAN")).upper() in {"CONTAINS", "STARTS_WITH", "ENDS_WITH"}:
            raise ValueError(f"Rule '{rule.get('id', '')}' threshold operator must be numeric")
        try:
            float(thresholds.get("value"))
        except (TypeError, ValueError):
            raise ValueError(f"Rule '{rule.get('id', '')}' thresholds.value must be numeric")


def _validate_relationship(rule):
    relationship = rule.get("relationship", {}) or {}
    rel_name = relationship.get("name")
    _validate_identifier(rel_name, "relationship.name")
    direction = str(relationship.get("direction", "OUT")).upper()
    if direction not in {"OUT", "IN"}:
        raise ValueError(f"Rule '{rule.get('id', '')}' has invalid relationship.direction")

    props = relationship.get("properties", {}) or {}
    if not isinstance(props, dict):
        raise ValueError(f"Rule '{rule.get('id', '')}' relationship.properties must be an object")
    for key, value in props.items():
        if str(key).startswith("_comment"):
            continue
        _validate_identifier(key, "relationship.properties key")
        _validate_scalar(value, f"relationship.properties.{key}")
        if key in {"bgcolor", "color", "textcolor"} and isinstance(value, str) and not COLOR_PATTERN.match(value):
            raise ValueError(f"Rule '{rule.get('id', '')}' has invalid color value for {key}")


def _validate_advanced(rule):
    advanced = rule.get("advanced", {}) or {}
    for key in ["allow_self_link", "deduplicate", "session_scoped"]:
        if key in advanced and not isinstance(advanced[key], bool):
            raise ValueError(f"Rule '{rule.get('id', '')}' advanced.{key} must be boolean")


def validate_rules_json(json_file_path: str):
    with open(json_file_path, encoding="utf-8") as f:
        data = json.load(f)
    try:
        validate(instance=data, schema=RULE_SCHEMA)
        if len(data.get("rules", [])) > MAX_RULES:
            raise ValueError(f"Rule file can contain at most {MAX_RULES} rules")
        for rule in data.get("rules", []):
            _validate_identifier(rule.get("id"), "rule.id")
            if str(rule.get("type", "")).upper() not in SUPPORTED_RULE_TYPES:
                raise ValueError(f"Rule '{rule.get('id', '')}' has unsupported type")
            _validate_match(rule)
            _validate_filter(rule)
            _validate_thresholds(rule)
            _validate_relationship(rule)
            _validate_advanced(rule)
        print("JSON is valid ")
        return data
    except ValidationError as e:
        raise ValueError(f"Invalid JSON: {e.message}")
