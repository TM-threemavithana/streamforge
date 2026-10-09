# StreamForge Performance Benchmark & Invariant Reconciliation

**Timestamp:** 2026-10-09T08:09:29.549197+00:00  
**Status:** **PASS**  
**Dataset:** `slice_10000_yellow_tripdata_2024-01.parquet` (10,000 events, batch size 500)  

---

## 1. System & Hardware Specifications

- **OS / Platform:** Windows 11 (10.0.26300) (AMD64)
- **CPU Cores:** 16 logical cores
- **Memory:** 15.39 GB Total RAM
- **Python Version:** 3.13.1

---

## 2. Benchmark Results

| Metric | Target / Hypothesis | Measured Result | Status |
| --- | --- | --- | --- |
| **Sustained Ingestion Throughput** | >= 500 events/sec | **9,792.9 events/sec** | **PASS** |
| **Producer Ack Latency (p50)** | < 100 ms | **28.4 ms** | **PASS** |
| **Producer Ack Latency (p95)** | < 250 ms | **36.0 ms** | **PASS** |
| **Producer Ack Latency (p99)** | < 500 ms | **36.0 ms** | **PASS** |
| **REST Query Latency (p50)** | < 100 ms | **7.3 ms** | **PASS** |
| **REST Query Latency (p95)** | < 300 ms | **30.0 ms** | **PASS** |
| **Peak RAM (RSS)** | < 512 MB | **73.9 MB** | **PASS** |
| **Data Invariant Parity** | 100% exact match | **100% Reconciled** | **PASS** |

---

## 3. Independent PyArrow Baseline Reconciliation

| Dimension | StreamForge Measured | PyArrow In-Memory Baseline | Discrepancy |
| --- | --- | --- | --- |
| **Total Input Rows** | 10,000 | 10,000 | **0** |
| **Accepted Trips** | 10,000 | 10,000 | **0** |
| **Rejected Anomalies** | 0 | 0 | **0** |
| **Duplicate Events** | 0 | 0 | **0** |

*Verified: Zero data loss, zero duplicate aggregate increments, and zero discrepancy against independent PyArrow kernel validation.*
