## Detection accuracy

Scored over 15 micro-batches — 695,000 records (660,250 normal, 34,750 attack).

| Metric | Value |
|---|---:|
| Precision | 89.9% |
| Recall | 98.5% |
| F1 | 94.0% |
| Accuracy | 99.37% |
| False-positive rate | 0.584% |

| Attack type | Seen | Caught | Missed | Recall | Precision | F1 |
|---|---:|---:|---:|---:|---:|---:|
| `brute_force` | 5,838 | 5,838 | 0 | 100.0% | 100.0% | 100.0% |
| `data_exfiltration` | 5,699 | 5,508 | 191 | 96.6% | 93.0% | 94.8% |
| `ddos` | 5,838 | 5,838 | 0 | 100.0% | 100.0% | 100.0% |
| `malicious_download` | 5,699 | 5,374 | 325 | 94.3% | 62.7% | 75.3% |
| `malware_c2` | 5,838 | 5,838 | 0 | 100.0% | 95.8% | 97.9% |
| `port_scan` | 5,838 | 5,838 | 0 | 100.0% | 100.0% | 100.0% |

## Detection latency

| Statistic | Seconds |
|---|---:|
| mean | 12.513 |
| p50 | 10.928 |
| p95 | 20.592 |
| p99 | 22.672 |
| max | 23.420 |

0 of 38,607 (0.00%) under the 2s target.
