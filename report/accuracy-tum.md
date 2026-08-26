## Detection accuracy

Scored over 76 micro-batches — 1,595,000 records (1,515,250 normal, 79,750 attack).

| Metric | Value |
|---|---:|
| Precision | 89.8% |
| Recall | 98.6% |
| F1 | 94.0% |
| Accuracy | 99.37% |
| False-positive rate | 0.590% |

| Attack type | Seen | Caught | Missed | Recall | Precision | F1 |
|---|---:|---:|---:|---:|---:|---:|
| `brute_force` | 13,398 | 13,398 | 0 | 100.0% | 100.0% | 100.0% |
| `data_exfiltration` | 13,079 | 12,636 | 443 | 96.6% | 93.3% | 94.9% |
| `ddos` | 13,398 | 13,398 | 0 | 100.0% | 100.0% | 100.0% |
| `malicious_download` | 13,079 | 12,378 | 701 | 94.6% | 62.4% | 75.2% |
| `malware_c2` | 13,398 | 13,398 | 0 | 100.0% | 95.8% | 97.8% |
| `port_scan` | 13,398 | 13,398 | 0 | 100.0% | 100.0% | 100.0% |

## Detection latency

| Statistic | Seconds |
|---|---:|
| mean | 25.185 |
| p50 | 19.457 |
| p95 | 55.996 |
| p99 | 59.412 |
| max | 61.063 |

0 of 88,688 (0.00%) under the 2s target.
