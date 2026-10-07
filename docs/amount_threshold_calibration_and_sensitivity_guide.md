# LinkX Anomaly Amount Threshold Calibration & Sensitivity Guide

**Last Updated**: 2026-10-07  
**Cluster Evaluated**: `bigdata-es-cluster` (`172.27.23.43:9200`)  
**Scope**: Empirical Distribution Analysis over **1.359 Billion Transactions** & Rule Sensitivity Settings  

---

## 1. Executive Summary

Previous fraud detection rules lacked financial volume floors, evaluating only graph topological connectivity or temporal flags (e.g. `amt > 0` or midnight transactions). Consequently, everyday micro-transfers (1 Birr, 4 Birr, 20 Birr airtime, bus fares) were flagged as high-risk anomalies such as `CIRCULAR_FLOW`, `LATE_NIGHT_TX`, `HUB_AND_SPOKE`, `RAPID_WITHDRAWAL`, and `ACCOUNT_ACTIVITY_SPIKE`.

To eliminate these false positives without compromising analytical integrity, we analyzed the empirical financial distribution directly from the Elasticsearch cluster across all **1,359,607,480 live mobile banking transactions** and **4,708,181 core banking transactions**. This document provides the empirical findings and calibrated sensitivity profiles.

---

## 2. Empirical Transaction Distribution from Elasticsearch

### 2.1 Mobile Banking Transactions (`mobile_banking_transactions` — 1,359,607,480 Records)

- **Total Transaction Count**: `1,359,607,480`
- **Minimum Positive Amount**: `0.001 Birr` (0.1 Santim)
- **Maximum Positive Amount**: `49,990,040.0 Birr` (~50 Million Birr)
- **Arithmetic Mean (Average)**: **`1,869.23 Birr`**
- **Median (50th Percentile)**: **`107.59 Birr`**
- **Cumulative Turnover**: **`2,541,412,893,938.41 Birr`** (~2.54 Trillion Birr)

#### Percentile Distribution Table

| Percentile | Amount (Birr) | Operational Context in Ethiopian Retail Banking |
| :--- | :---: | :--- |
| **P1** | **`5.00`** | Micro-transfers, airtime, test transactions |
| **P5** | **`9.19`** | Small local transportation (bus/bajaj) |
| **P10** | **`14.05`** | Routine daily consumer spending |
| **P25 (Q1)** | **`32.52`** | Standard peer-to-peer micro-transfers |
| **P50 (Median)** | **`107.59`** | **50% of all national mobile transfers are $\le 108$ Birr** |
| **P75 (Q3)** | **`453.97`** | Routine household, utility, and merchant transfers |
| **P90** | **`2,050.09`** | Substantial retail payments / personal transfers |
| **P95** | **`4,769.32`** | Salary payments, rent, commercial inventory |
| **P99** | **`30,674.18`** | High-value commercial / wholesale transfers |

#### Volume Breakdown by Financial Tiers

| Bucket | Range (Birr) | Transaction Count | % of National Volume | Cumulative % |
| :--- | :--- | :---: | :---: | :---: |
| **Micro / Cents** | `< 10.0` | `57,314,759` | 4.22% | 4.22% |
| **Very Small** | `10.0 – 100.0` | `583,640,960` | 42.93% | **47.15%** |
| **Small** | `100.0 – 500.0` | `411,651,857` | 30.28% | **77.43%** |
| **Everyday Mid-Range** | `500.0 – 2,000.0` | `176,886,461` | 13.01% | 90.44% |
| **Medium Commercial** | `2,000.0 – 10,000.0` | `95,974,013` | 7.06% | 97.50% |
| **Substantial Commercial** | `10,000.0 – 100,000.0` | `31,250,376` | 2.30% | 99.80% |
| **Near-CTR Threshold** | `100,000.0 – 300,000.0` | `2,086,581` | 0.15% | 99.94% |
| **Regulatory CTR Limit** | `$\ge$ 300,000.0` | `802,473` | 0.06% | 100.00% |

> [!IMPORTANT]
> **Key Analytical Takeaway**:
> - **47.15% of all mobile transactions in Ethiopia are under 100 Birr**.
> - **77.43% of all mobile transactions are under 500 Birr**.
> Without a minimum financial floor, LinkX was previously scanning over **1.05 Billion micro-transactions** for complex graph laundering patterns, resulting in massive false-positive fatigue.

---

### 2.2 Core Banking Transactions (`core_banking_transactions` — 4,708,181 Records)

- **Total Transaction Count**: `4,708,181`
- **Minimum Positive Amount**: `1.00 Birr`
- **Maximum Amount**: `100,000,000.00 Birr` (100 Million Birr)
- **Arithmetic Mean (Average)**: **`71,966.32 Birr`**
- **Median (50th Percentile)**: **`5,252.74 Birr`**
- **P75**: **`42,699.25 Birr`**
- **P90**: **`130,188.59 Birr`**
- **P95**: **`275,405.41 Birr`**
- **P99**: **`1,039,565.47 Birr`**

---

## 3. Calibrated Sensitivity Profiles

### Profile A: Balanced Production (Recommended)
*Cuts out the bottom ~77% of retail noise while ensuring that any financially meaningful fraud or layering activity is caught.*

```json
{
  "global_min_anomaly_amount": 500.0,
  "late_night_min_amount": 1000.0,
  "circular_flow_min_amount": 500.0,
  "circular_flow_check_amounts": true,
  "circular_flow_amount_tolerance": 0.05,
  "rapid_withdrawal_min_amount": 500.0,
  "rapid_withdrawal_amount_tolerance": 0.1,
  "hub_spoke_min_amount": 2000.0,
  "hub_spoke_min_counterparties": 3,
  "activity_spike_min_amount": 2000.0,
  "activity_spike_min_daily_count": 10,
  "activity_spike_multiplier": 3.0,
  "fund_flow_min_amount": 500.0,
  "fund_flow_hub_threshold": 1000.0,
  "fund_flow_max_downstream": 5,
  "abnormal_balance_min_change": 1000.0,
  "reporting_threshold": 300000.0,
  "smurfing_single_tx_threshold": 300000.0,
  "smurfing_cumulative_threshold": 900000.0,
  "smurfing_min_tx_count": 3,
  "late_night_start": 2300,
  "late_night_end": 400
}
```

#### Rule-by-Rule Rationale (Profile A)
1. **`global_min_anomaly_amount` (`500.0 Birr`)**: Universal floor. Transactions under 500 Birr are suppressed from being flagged unless a rule specifically overrides this floor.
2. **`late_night_min_amount` (`1,000.0 Birr`)**: Legitimate users purchase airtime and pay 50–200 Birr for night transport. Late-night fraud alerts only trigger for transfers $\ge 1,000$ Birr.
3. **`circular_flow_min_amount` (`500.0 Birr`) + `circular_flow_check_amounts: true`**: Small testing transfers between friends won't trigger round-trip flags. Furthermore, legs must match in value within 5% tolerance.
4. **`rapid_withdrawal_min_amount` (`500.0 Birr`)**: Prevents rapid pass-through flags on small cash-out transfers.
5. **`hub_spoke_min_amount` (`2,000.0 Birr`)**: Exceeds the national average (`1,869 Birr`). An account must distribute or aggregate $\ge 2,000$ Birr across 3+ counterparties to qualify as a hub.
6. **`activity_spike_min_amount` (`2,000.0 Birr`)**: An account conducting 10 small transfers (e.g. 5 Birr each) will NOT be flagged as an activity spike.
7. **`fund_flow_min_amount` (`500.0 Birr`)**: Layering chains only trace money transfers moving at least 500 Birr per leg.
8. **`abnormal_balance_min_change` (`1,000.0 Birr`)**: Normal balance fluctuations below 1,000 Birr do not trigger abnormal baseline shift warnings.

---

### Profile B: Generous / High-Signal (Near-Zero False Positives)
*Calibrated for high-volume operational teams focusing exclusively on commercially significant or structured syndicate activity.*

```json
{
  "global_min_anomaly_amount": 1000.0,
  "late_night_min_amount": 2000.0,
  "circular_flow_min_amount": 1000.0,
  "circular_flow_check_amounts": true,
  "circular_flow_amount_tolerance": 0.05,
  "rapid_withdrawal_min_amount": 1000.0,
  "rapid_withdrawal_amount_tolerance": 0.1,
  "hub_spoke_min_amount": 5000.0,
  "hub_spoke_min_counterparties": 3,
  "activity_spike_min_amount": 5000.0,
  "activity_spike_min_daily_count": 10,
  "activity_spike_multiplier": 3.0,
  "fund_flow_min_amount": 1000.0,
  "fund_flow_hub_threshold": 1000.0,
  "fund_flow_max_downstream": 5,
  "abnormal_balance_min_change": 2000.0,
  "reporting_threshold": 300000.0,
  "smurfing_single_tx_threshold": 300000.0,
  "smurfing_cumulative_threshold": 900000.0,
  "smurfing_min_tx_count": 3,
  "late_night_start": 2300,
  "late_night_end": 400
}
```

---

### Profile C: Reset to Baseline System Defaults

```json
{
  "global_min_anomaly_amount": 100.0,
  "late_night_min_amount": 500.0,
  "circular_flow_min_amount": 200.0,
  "circular_flow_check_amounts": false,
  "circular_flow_amount_tolerance": 0.05,
  "rapid_withdrawal_min_amount": 250.0,
  "rapid_withdrawal_amount_tolerance": 0.1,
  "hub_spoke_min_amount": 500.0,
  "hub_spoke_min_counterparties": 3,
  "activity_spike_min_amount": 500.0,
  "activity_spike_min_daily_count": 10,
  "activity_spike_multiplier": 3.0,
  "fund_flow_min_amount": 200.0,
  "fund_flow_hub_threshold": 1000.0,
  "fund_flow_max_downstream": 5,
  "abnormal_balance_min_change": 500.0,
  "reporting_threshold": 300000.0,
  "smurfing_single_tx_threshold": 300000.0,
  "smurfing_cumulative_threshold": 900000.0,
  "smurfing_min_tx_count": 3,
  "late_night_start": 2300,
  "late_night_end": 400
}
```

---

## 4. API Endpoints & Operational Verification

### 4.1 Read Active Configuration
- **Request**: `GET /rule-thresholds`
- **Headers**: `Authorization: Bearer <token>`
- **Response**: Returns the merged dictionary of active thresholds.

### 4.2 Save / Update Configuration
- **Request**: `POST /rule-thresholds`
- **Headers**: `Authorization: Bearer <token>`, `Content-Type: application/json`
- **Payload**: Full or partial dictionary of parameters.
- **Backend Enforced Guardrails**:
  - All numerical amounts must be positive ($\ge 0$ and $\le 10,000,000$).
  - Tolerances must be between $0.0$ and $1.0$.
  - Boolean fields (e.g. `circular_flow_check_amounts`) strictly reject numbers.
  - Updates are appended to PostgreSQL `global_rule_thresholds` with version auditing (`updated_by`, `created_at`).

### 4.3 Runtime Engine Consumption
Both execution pipelines pull dynamically from PostgreSQL on each run:
1. **Manual Link Analysis**: `batch_graph_analysis_transactions()` in `LA_rules_script.py`.
2. **Autonomous Detective Engine**: `run_full_graph_analysis()` in `xvigilance_consumer.py`.
No daemon restarts or code changes are required after updating thresholds via UI/API.
