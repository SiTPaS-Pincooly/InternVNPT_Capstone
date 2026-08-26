## Detection accuracy

Scored over 33 micro-batches — 1,610,000 records (1,529,500 normal, 80,500 attack).

| Metric | Value |
|---|---:|
| Precision | 89.8% |
| Recall | 98.5% |
| F1 | 93.9% |
| Accuracy | 99.36% |
| False-positive rate | 0.591% |

| Attack type | Seen | Caught | Missed | Recall | Precision | F1 |
|---|---:|---:|---:|---:|---:|---:|
| `brute_force` | 13,524 | 13,524 | 0 | 100.0% | 100.0% | 100.0% |
| `data_exfiltration` | 13,202 | 12,702 | 500 | 96.2% | 93.4% | 94.8% |
| `ddos` | 13,524 | 13,524 | 0 | 100.0% | 100.0% | 100.0% |
| `malicious_download` | 13,202 | 12,501 | 701 | 94.7% | 62.2% | 75.1% |
| `malware_c2` | 13,524 | 13,524 | 0 | 100.0% | 95.9% | 97.9% |
| `port_scan` | 13,524 | 13,524 | 0 | 100.0% | 100.0% | 100.0% |

## Detection latency

| Statistic | Seconds |
|---|---:|
| mean | 2444.647 |
| p50 | 2757.806 |
| p95 | 2780.890 |
| p99 | 12192.365 |
| max | 12202.370 |

0 of 89,543 (0.00%) under the 2s target.
