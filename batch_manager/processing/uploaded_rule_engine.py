from datetime import datetime

from logger import log_writer


SUPPORTED_OPERATORS = {
    "EQUALS": "=",
    "NOT_EQUALS": "<>",
    "GREATER_THAN": ">",
    "GREATER_THAN_OR_EQUALS": ">=",
    "LESS_THAN": "<",
    "LESS_THAN_OR_EQUALS": "<=",
    "CONTAINS": "CONTAINS",
    "STARTS_WITH": "STARTS WITH",
    "ENDS_WITH": "ENDS WITH",
}


def _safe_identifier(value):
    text = str(value or "").replace("`", "").strip()
    if not text:
        raise ValueError("Empty identifier is not allowed in uploaded rule")
    return text


def _label(value):
    return f"`{_safe_identifier(value)}`"


def _prop(value):
    return f"`{_safe_identifier(value)}`"


def _relationship(value):
    rel = _safe_identifier(value)
    if not rel.replace("_", "a").isalnum() or rel[0].isdigit():
        raise ValueError(f"Invalid relationship name: {value}")
    return rel


def _session_scope(alias, incremental_batch_id=None):
    clauses = [f"({alias}.batch_id STARTS WITH $session_id OR {alias}.session_id = $session_id)"]
    if incremental_batch_id:
        clauses.append(f"{alias}.batch_id = $batch_id")
    return " AND ".join(clauses)


def _relationship_props(rule, provisional):
    props = dict(rule.get("relationship", {}).get("properties", {}) or {})
    props["provisional"] = bool(provisional)
    props["rule_id"] = rule.get("id", "")
    props["reason"] = rule.get("description", "")
    return props


def _condition_clause(alias, condition, params, prefix):
    prop = _prop(condition.get("property"))
    operator_name = str(condition.get("operator", "EQUALS")).upper()
    operator = SUPPORTED_OPERATORS.get(operator_name)
    if not operator:
        raise ValueError(f"Unsupported operator: {operator_name}")

    param_name = f"{prefix}_{len(params)}"
    params[param_name] = condition.get("value")

    if operator_name in {"CONTAINS", "STARTS_WITH", "ENDS_WITH"}:
        return f"toString({alias}.{prop}) {operator} toString(${param_name})"
    return f"{alias}.{prop} {operator} ${param_name}"


def _threshold_clause(alias, thresholds, params, prefix):
    if not thresholds or not thresholds.get("enabled"):
        return None
    metric = _prop(thresholds.get("metric"))
    operator_name = str(thresholds.get("operator", "GREATER_THAN")).upper()
    operator = SUPPORTED_OPERATORS.get(operator_name)
    if not operator or operator_name in {"CONTAINS", "STARTS_WITH", "ENDS_WITH"}:
        raise ValueError(f"Unsupported threshold operator: {operator_name}")

    param_name = f"{prefix}_threshold"
    params[param_name] = thresholds.get("value")
    return f"toFloat({alias}.{metric}) {operator} toFloat(${param_name})"


def _where_clauses(alias, rule, params, prefix, incremental_batch_id=None):
    clauses = [_session_scope(alias, incremental_batch_id)]
    filter_config = rule.get("filter", {}) or {}
    if str(filter_config.get("mode", "OPTIONAL")).upper() != "DISABLED":
        for condition in filter_config.get("conditions", []) or []:
            clauses.append(_condition_clause(alias, condition, params, prefix))
    threshold = _threshold_clause(alias, rule.get("thresholds", {}) or {}, params, prefix)
    if threshold:
        clauses.append(threshold)
    return clauses


def _set_relationship_clause():
    return """
    SET r += $rel_props,
        r.session_id = $session_id,
        r.bgcolor = coalesce($rel_props.bgcolor, '#CCC')
    """


def _merge_relationship_clause(start_alias, end_alias, rel, rule):
    direction = str(rule.get("relationship", {}).get("direction", "OUT")).upper()
    if direction == "IN":
        start_alias, end_alias = end_alias, start_alias
    return f"MERGE ({start_alias})-[r:{rel} {{session_id: $session_id}}]->({end_alias})"


def _count_relationships(session, session_id, relationship_names):
    if not relationship_names:
        return {}
    result = session.run(
        """
        MATCH ()-[r]->()
        WHERE r.session_id = $session_id AND type(r) IN $relationship_names
        RETURN type(r) AS relationship_type, count(r) AS count
        """,
        session_id=str(session_id),
        relationship_names=relationship_names,
    )
    return {record["relationship_type"]: record["count"] for record in result}


def _clear_relationships(session, session_id, relationship_names):
    if not relationship_names:
        return
    session.run(
        """
        MATCH ()-[r]->()
        WHERE r.session_id = $session_id AND type(r) IN $relationship_names
        DELETE r
        """,
        session_id=str(session_id),
        relationship_names=relationship_names,
    )


def _create_indexes(session, label):
    safe = _safe_identifier(label).replace(" ", "_").replace("-", "_").lower()
    for prop in ["batch_id", "session_id", "NodeId"]:
        session.run(f"CREATE INDEX idx_{safe}_{prop} IF NOT EXISTS FOR (n:{_label(label)}) ON (n.{_prop(prop)})")


def _run_self_rule(session, label, rule, params, incremental_batch_id):
    rel = _relationship(rule["relationship"]["name"])
    where = " AND ".join(_where_clauses("t", rule, params, "t", incremental_batch_id))
    query = f"""
    MATCH (t:{_label(label)})
    WHERE {where}
    {_merge_relationship_clause("t", "t", rel, rule)}
    {_set_relationship_clause()}
    """
    session.run(query, **params)


def _run_sequence_rule(session, label, rule, params, incremental_batch_id):
    rel = _relationship(rule["relationship"]["name"])
    match = rule.get("match", {}) or {}
    advanced = rule.get("advanced", {}) or {}
    group_by = [_safe_identifier(p) for p in match.get("group_by", []) or []]
    order_by = [_safe_identifier(p) for p in match.get("order_by", []) or []] or ["NodeId"]
    window = max(int((match.get("window", {}) or {}).get("size", 1) or 1), 1)
    rule_type = str(rule.get("type", "SEQUENTIAL")).upper()
    allow_self_link = bool(advanced.get("allow_self_link", False))

    where = " AND ".join(_where_clauses("t", rule, params, "t"))
    group_expr = "[" + ", ".join([f"t.{_prop(p)}" for p in group_by]) + "]" if group_by else "['__all__']"
    order_clause = ", ".join([f"t.{_prop(p)}" for p in order_by])

    if rule_type == "PREVIOUS":
        range_expr = f"range({window}, size(nodes)-1)"
        pair_expr = f"nodes[i] AS a, nodes[i-{window}] AS b"
    else:
        range_expr = f"range(0, size(nodes)-{window + 1})"
        pair_expr = f"nodes[i] AS a, nodes[i+{window}] AS b"

    incremental_clause = ""
    if incremental_batch_id:
        incremental_clause = "AND (a.batch_id = $batch_id OR b.batch_id = $batch_id)"
    self_clause = "" if allow_self_link else "AND id(a) <> id(b)"

    query = f"""
    MATCH (t:{_label(label)})
    WHERE {where}
    WITH {group_expr} AS group_key, t
    ORDER BY {order_clause}
    WITH group_key, collect(t) AS nodes
    WHERE size(nodes) > {window}
    UNWIND {range_expr} AS i
    WITH {pair_expr}
    WHERE true {self_clause} {incremental_clause}
    {_merge_relationship_clause("a", "b", rel, rule)}
    {_set_relationship_clause()}
    """
    session.run(query, **params)


def _run_pairwise_rule(session, label, rule, params, incremental_batch_id):
    rel = _relationship(rule["relationship"]["name"])
    match = rule.get("match", {}) or {}
    advanced = rule.get("advanced", {}) or {}
    group_by = [_safe_identifier(p) for p in match.get("group_by", []) or []]
    allow_self_link = bool(advanced.get("allow_self_link", False))
    if not group_by:
        raise ValueError(f"PAIRWISE rule '{rule.get('id')}' requires match.group_by")

    a_where = _where_clauses("a", rule, params, "a")
    b_where = _where_clauses("b", rule, params, "b")
    equality = [f"a.{_prop(p)} = b.{_prop(p)}" for p in group_by]
    if incremental_batch_id:
        equality.append("(a.batch_id = $batch_id OR b.batch_id = $batch_id)")
    if not allow_self_link:
        equality.append("id(a) < id(b)")

    query = f"""
    MATCH (a:{_label(label)}), (b:{_label(label)})
    WHERE {" AND ".join(a_where + b_where + equality)}
    {_merge_relationship_clause("a", "b", rel, rule)}
    {_set_relationship_clause()}
    """
    session.run(query, **params)


def _run_circular_rule(session, label, rule, params, incremental_batch_id):
    rel = _relationship(rule["relationship"]["name"])
    match = rule.get("match", {}) or {}
    source_prop = _safe_identifier(match.get("source_property", "ACCOUNTNO"))
    target_prop = _safe_identifier(match.get("target_property", "BENACCOUNTNO"))
    same_day = bool(match.get("same_day", True))

    clauses = _where_clauses("a", rule, params, "a") + _where_clauses("b", rule, params, "b")
    clauses.extend([
        f"a.{_prop(source_prop)} = b.{_prop(target_prop)}",
        f"a.{_prop(target_prop)} = b.{_prop(source_prop)}",
        f"a.{_prop(source_prop)} IS NOT NULL",
        f"a.{_prop(target_prop)} IS NOT NULL",
        "id(a) < id(b)",
    ])
    if same_day:
        clauses.append("coalesce(a.TRANSACTIONDATE, '') = coalesce(b.TRANSACTIONDATE, '')")
    if incremental_batch_id:
        clauses.append("(a.batch_id = $batch_id OR b.batch_id = $batch_id)")

    query = f"""
    MATCH (a:{_label(label)}), (b:{_label(label)})
    WHERE {" AND ".join(clauses)}
    {_merge_relationship_clause("a", "b", rel, rule)}
    {_set_relationship_clause()}
    """
    session.run(query, **params)


def _run_rule(session, label, rule, session_id, batch_id=None, provisional=False):
    params = {
        "session_id": str(session_id),
        "rel_props": _relationship_props(rule, provisional),
    }
    if batch_id:
        params["batch_id"] = batch_id

    rule_type = str(rule.get("type", "SEQUENTIAL")).upper()
    if rule_type == "SELF":
        _run_self_rule(session, label, rule, params, batch_id)
    elif rule_type in {"SEQUENTIAL", "PREVIOUS", "WINDOWED"}:
        _run_sequence_rule(session, label, rule, params, batch_id)
    elif rule_type == "PAIRWISE":
        _run_pairwise_rule(session, label, rule, params, batch_id)
    elif rule_type == "CIRCULAR":
        _run_circular_rule(session, label, rule, params, batch_id)
    else:
        raise ValueError(f"Unsupported rule type: {rule_type}")


def run_uploaded_rules(driver, session_id, nodes_label, log_file, rule_set, batch_id=None, incremental=False):
    rule_name = rule_set.get("rule_name", "Uploaded Rule")
    rules = rule_set.get("rules", [])
    label = nodes_label or rule_set.get("node_label", "Node")
    relationship_names = [_relationship(rule["relationship"]["name"]) for rule in rules]

    mode = "incremental" if incremental else "final"
    log_writer(log_file, f"[{datetime.now()}] [Info] Starting {mode} uploaded-rule analysis: {rule_name}")

    with driver.session() as session:
        _create_indexes(session, label)
        if not incremental:
            _clear_relationships(session, session_id, relationship_names)

        for rule in rules:
            _run_rule(
                session,
                label,
                rule,
                session_id,
                batch_id=batch_id if incremental else None,
                provisional=incremental,
            )

        counts = _count_relationships(session, session_id, relationship_names)

    log_writer(log_file, f"[{datetime.now()}] [Success] Uploaded-rule analysis completed: {counts}")
    return counts
