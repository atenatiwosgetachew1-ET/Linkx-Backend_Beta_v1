import re

RULES_FILE = "/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py"

with open(RULES_FILE, "r") as f:
    content = f.read()

new_circular_flow = '''def get_circular_flow_query(
    label,
    scope_clause_t,
    trusted_pair_clause,
    is_provisional=False,
    incremental_batch_id=None
):
    prov_str = "true" if is_provisional else "false"

    # In incremental mode, only accounts touched by the new batch
    # are used as seeds. This prevents scanning every possible pair.
    seed_block = ""
    seed_filter_a = ""
    seed_filter_b = ""

    if incremental_batch_id:
        seed_block = f"""
        MATCH (seed:{label})
        WHERE seed.batch_id = {incremental_batch_id}
          AND coalesce(seed.IGNORE_LOGICAL, false) = false
          AND seed.LOGICAL_ACCOUNTNO IS NOT NULL
          AND seed.LOGICAL_ACCOUNTNO <> ''
          AND seed.LOGICAL_BENACCOUNTNO IS NOT NULL
          AND seed.LOGICAL_BENACCOUNTNO <> ''
        WITH DISTINCT
            seed.LOGICAL_ACCOUNTNO AS seed_sender,
            seed.LOGICAL_BENACCOUNTNO AS seed_receiver,
            seed.TRANSACTIONDATE AS seed_day

        WHERE seed_day IS NOT NULL
          AND seed_day <> ''
          AND seed_sender <> seed_receiver
          AND NOT seed_sender IN $pt
          AND NOT seed_receiver IN $pt
        """

        seed_filter_a = """
          AND a.LOGICAL_ACCOUNTNO = seed_sender
          AND a.LOGICAL_BENACCOUNTNO = seed_receiver
          AND a.TRANSACTIONDATE = seed_day
        """

        seed_filter_b = """
          AND b.LOGICAL_ACCOUNTNO = seed_receiver
          AND b.LOGICAL_BENACCOUNTNO = seed_sender
          AND b.TRANSACTIONDATE = seed_day
        """

    else:
        seed_block = """
        MATCH (seed:{label})
        WHERE ({scope_clause_t})
          AND coalesce(seed.IGNORE_LOGICAL, false) = false
          AND seed.LOGICAL_ACCOUNTNO IS NOT NULL
          AND seed.LOGICAL_ACCOUNTNO <> ''
          AND seed.LOGICAL_BENACCOUNTNO IS NOT NULL
          AND seed.LOGICAL_BENACCOUNTNO <> ''
        WITH DISTINCT
            seed.LOGICAL_ACCOUNTNO AS seed_sender,
            seed.LOGICAL_BENACCOUNTNO AS seed_receiver,
            seed.TRANSACTIONDATE AS seed_day

        WHERE seed_day IS NOT NULL
          AND seed_day <> ''
          AND seed_sender <> seed_receiver
          AND NOT seed_sender IN $pt
          AND NOT seed_receiver IN $pt
        """.format(label=label, scope_clause_t=scope_clause_t)

        seed_filter_a = """
          AND a.LOGICAL_ACCOUNTNO = seed_sender
          AND a.LOGICAL_BENACCOUNTNO = seed_receiver
          AND a.TRANSACTIONDATE = seed_day
        """

        seed_filter_b = """
          AND b.LOGICAL_ACCOUNTNO = seed_receiver
          AND b.LOGICAL_BENACCOUNTNO = seed_sender
          AND b.TRANSACTIONDATE = seed_day
        """

    return f"""
    {{seed_block}}

    MATCH (a:{label})
    WHERE ({scope_clause_t.replace("t.", "a.")})
      AND coalesce(a.IGNORE_LOGICAL, false) = false
      AND a.LOGICAL_ACCOUNTNO IS NOT NULL
      AND a.LOGICAL_ACCOUNTNO <> ''
      AND a.LOGICAL_BENACCOUNTNO IS NOT NULL
      AND a.LOGICAL_BENACCOUNTNO <> ''
      {{seed_filter_a}}

    MATCH (b:{label})
    WHERE ({scope_clause_t.replace("t.", "b.")})
      AND coalesce(b.IGNORE_LOGICAL, false) = false
      AND b.LOGICAL_ACCOUNTNO IS NOT NULL
      AND b.LOGICAL_ACCOUNTNO <> ''
      AND b.LOGICAL_BENACCOUNTNO IS NOT NULL
      AND b.LOGICAL_BENACCOUNTNO <> ''
      {{seed_filter_b}}

      AND elementId(a) < elementId(b)

      AND a.LOGICAL_ACCOUNTNO <> a.LOGICAL_BENACCOUNTNO

      AND coalesce(
          toFloat(a.AMOUNTINBIRR),
          toFloat(a.AMOUNT),
          toFloat(a.amount),
          toFloat(a.LOCAL_AMOUNT),
          0.0
      ) > 0

      AND coalesce(
          toFloat(b.AMOUNTINBIRR),
          toFloat(b.AMOUNT),
          toFloat(b.amount),
          toFloat(b.LOCAL_AMOUNT),
          0.0
      ) > 0

    WITH
        a,
        b,
        a.LOGICAL_ACCOUNTNO AS sender_a,
        a.LOGICAL_BENACCOUNTNO AS receiver_a,
        b.LOGICAL_ACCOUNTNO AS sender_b,
        b.LOGICAL_BENACCOUNTNO AS receiver_b,

        coalesce(
            toFloat(a.AMOUNTINBIRR),
            toFloat(a.AMOUNT),
            toFloat(a.amount),
            toFloat(a.LOCAL_AMOUNT),
            0.0
        ) AS amt_a,

        coalesce(
            toFloat(b.AMOUNTINBIRR),
            toFloat(b.AMOUNT),
            toFloat(b.amount),
            toFloat(b.LOCAL_AMOUNT),
            0.0
        ) AS amt_b

    WHERE
        sender_a = receiver_b
        AND receiver_a = sender_b

        AND amt_a > 0
        AND amt_b > 0

        AND abs(amt_a - amt_b)
            <= (CASE
                    WHEN amt_a < amt_b THEN amt_a
                    ELSE amt_b
                END * 0.05)

        AND {trusted_pair_clause}

    CALL {{
        WITH a, b, sender_a, receiver_a, amt_a, amt_b

        MERGE (a)-[r1:CIRCULAR_FLOW {{session_id:$session_id}}]->(b)

        SET
            r1.is_evidence = true,
            r1.anomaly_score = 0.6,
            r1.bgcolor = '#e6e6e6',
            r1.provisional = {prov_str},

            r1.reason =
                CASE
                    WHEN coalesce(a.PASSTHROUGH_HOPS, 0) > 0
                      OR coalesce(b.PASSTHROUGH_HOPS, 0) > 0
                    THEN 'logical same-day reverse flow with pass-through intermediary'
                    ELSE 'same-day direct reverse transfer'
                END,

            r1.logical_sender = sender_a,
            r1.logical_receiver = receiver_a,

            r1.reverse_sender = receiver_a,
            r1.reverse_receiver = sender_a,

            r1.amount_a = amt_a,
            r1.amount_b = amt_b,

            r1.passthrough_a =
                coalesce(a.PASSTHROUGH_HOPS, 0),

            r1.passthrough_b =
                coalesce(b.PASSTHROUGH_HOPS, 0),

            r1.logical_path_a =
                coalesce(a.LOGICAL_PATH, ''),

            r1.logical_path_b =
                coalesce(b.LOGICAL_PATH, ''),

            r1.edge_semantic = 'DERIVED_LOGICAL_FLOW',
            r1.financial_flow = true,
            r1.directed_display = true

        MERGE (b)-[r2:CIRCULAR_FLOW {{session_id:$session_id}}]->(a)

        SET
            r2.is_evidence = true,
            r2.anomaly_score = 0.6,
            r2.bgcolor = '#e6e6e6',
            r2.provisional = {prov_str},

            r2.reason =
                CASE
                    WHEN coalesce(a.PASSTHROUGH_HOPS, 0) > 0
                      OR coalesce(b.PASSTHROUGH_HOPS, 0) > 0
                    THEN 'logical same-day reverse flow with pass-through intermediary'
                    ELSE 'same-day direct reverse transfer'
                END,

            r2.logical_sender = receiver_a,
            r2.logical_receiver = sender_a,

            r2.reverse_sender = sender_a,
            r2.reverse_receiver = receiver_a,

            r2.amount_a = amt_a,
            r2.amount_b = amt_b,

            r2.passthrough_a =
                coalesce(a.PASSTHROUGH_HOPS, 0),

            r2.passthrough_b =
                coalesce(b.PASSTHROUGH_HOPS, 0),

            r2.logical_path_a =
                coalesce(a.LOGICAL_PATH, ''),

            r2.logical_path_b =
                coalesce(b.LOGICAL_PATH, ''),

            r2.edge_semantic = 'DERIVED_LOGICAL_FLOW',
            r2.financial_flow = true,
            r2.directed_display = true
    }}
    IN TRANSACTIONS OF 5000 ROWS
    """
'''

old_fund_flow = '''def get_fund_flow_query(label, scope_clause_t, scope_clause_a, scope_clause_b, trusted_pair_clause, is_provisional=False):
    prov_str = "true" if is_provisional else "false"
    return f"""
    MATCH (t:{label})
    WHERE ({scope_clause_t})
      AND coalesce(t.IGNORE_LOGICAL, false) = false
      AND t.LOGICAL_ACCOUNTNO IS NOT NULL AND t.LOGICAL_ACCOUNTNO <> ''
    WITH t.LOGICAL_ACCOUNTNO AS acc, count(t) AS out_count
    WHERE out_count < 1000 AND NOT acc IN $pt

    MATCH (a:{label})
    WHERE ({scope_clause_a}) AND a.LOGICAL_BENACCOUNTNO = acc
    
    // Force planner to resolve 'a' before scanning 'b'
    WITH acc, a

    MATCH (b:{label})
    WHERE ({scope_clause_b}) AND b.LOGICAL_ACCOUNTNO = acc
      AND elementId(a) <> elementId(b)
      AND (
        coalesce(a.TRANSACTIONDATE, '') < coalesce(b.TRANSACTIONDATE, '')
        OR (
          coalesce(a.TRANSACTIONDATE, '') = coalesce(b.TRANSACTIONDATE, '')
          AND coalesce(a.TRANSACTIONTIME, '') < coalesce(b.TRANSACTIONTIME, '')
        )
      )
      AND {trusted_pair_clause}

    WITH a, b
    ORDER BY b.TRANSACTIONDATE ASC, b.TRANSACTIONTIME ASC
    WITH a, collect(b) AS downstream
    WITH a, downstream[..5] AS limited_downstream
    UNWIND limited_downstream AS b

    CALL {{
      WITH a, b
      MERGE (a)-[r:FUND_FLOW {{session_id:$session_id}}]->(b)
      SET r.is_evidence = true, r.anomaly_score = 0.5, r.bgcolor = '#d8a822', r.provisional = {prov_str},
          r.reason = 'beneficiary later acts as sender',
          r.edge_semantic = 'TEMPORAL_SEQUENCE', r.financial_flow = false, r.directed_display = true
    }} IN TRANSACTIONS OF 5000 ROWS
    """
'''

# We will carefully isolate the exact block to replace.
# The block spans from `def get_circular_flow_query` up to `def get_dormant_to_active_query` 
# because get_fund_flow is between them.

pattern = re.compile(r'def get_circular_flow_query.*?(?=def get_dormant_to_active_query)', re.DOTALL)
if pattern.search(content):
    content = pattern.sub(new_circular_flow + '\n\n' + old_fund_flow + '\n', content)
    with open(RULES_FILE, "w") as f:
        f.write(content)
    print("Successfully replaced CIRCULAR_FLOW and restored FUND_FLOW.")
else:
    print("Could not find the function block.")
