# Benchmarks

## Окружение

- MacBook Pro, Apple M2 Pro (10 cores), 16 GB RAM
- macOS, Go 1.26.3, Docker Desktop
- Приложение: контейнер из `deployments/docker-compose.yml`
- Kafka: один брокер в KRaft-режиме, топик `search.events` с 3 партициями
- Нагрузочный генератор: vegeta 12.13.0, single-process, keepalive

## Методология

Три HTTP-сценария:

1. **baseline-5k** - 5000 RPS на `/api/v1/trending`, 30 секунд, окно прогрето одним прогоном `make load`
2. **stress-10k** - то же самое на 10000 RPS
3. **attack-5k** - 5000 RPS на API параллельно с `load_producer -rps=2000 -bot-share=0.3` (30% событий был спам одной фиктивной query от 20 ботов). Проверяет, что писательская нагрузка с защитными слоями не ломает читательскую latency.

Цели vegeta распределяются равномерно по четырём URL с разными `limit`.

## HTTP load tests

| метрика | baseline-5k | stress-10k | attack-5k |
|---|---|---|---|
| RPS (фактически) | 5000 | 10000 | 5000 |
| Success | 100% | 100% | 100% |
| p50 | 226 µs | 268 µs | 233 µs |
| p90 | 311 µs | 423 µs | 375 µs |
| p95 | 393 µs | 583 µs | 502 µs |
| p99 | 871 µs | 1.10 ms | 1.36 ms |
| max | 40.8 ms | 62.7 ms | 60.5 ms |
| доля < 1ms | 99.21% | 98.77% | 98.70% |

- На 5k RPS p99 < 1ms. На 10k RPS p99 1.10 ms.
- **attack-5k vs baseline-5k**: параллельная Kafka-нагрузка с ботами не сдвигает p50/p90. p99 поднялся с 871µs до 1.36ms.
- 0.25% запросов в [10ms, 50ms] - хвост, несколько единичных выбросов до 60ms. Это из-за Docker Desktop на macOS.
- **Bloom-фильтр работает** - на Prometheus-графике видно зелёную зону `status=dropped_dedup` что примерно 30% от потока во время attack-сценария.

HTML-отчёты vegeta plot: `baseline-5k.html`, `stress-10k.html`, `attack-5k.html` в этой же папке.

## Micro-benchmarks: TopK

BenchmarkAdd_Existing-12             30 ns/op    48 B/op    1 alloc/op
BenchmarkAdd_NewWithCapacity-12     250 ns/op    86 B/op    2 allocs/op
BenchmarkAdd_Eviction-12            127 ns/op    24 B/op    2 allocs/op
BenchmarkAdd_Mixed_Zipf-12          606 ns/op    43 B/op    1 alloc/op
BenchmarkMerge_5kQueries-12         350 µs/op   758 KB/op   5065 allocs/op
BenchmarkWindow_Add-12               59 ns/op    48 B/op    1 alloc/op
BenchmarkWindow_AddConcurrent-12    264 ns/op    13 B/op    1 alloc/op
BenchmarkWindow_Snapshot-12         207 µs/op   757 KB/op   5036 allocs/op

- 48 B / 1 alloc в `Add_Existing` - это новый бакет при инкременте счётчика.
- `Window.Snapshot` - 207 µs на 5 минут * 30 бакетов * 10000s записей в каждом.

## Micro-benchmarks: pipeline

BenchmarkAnomaly_Observe_Existing-12     26 ns/op    0 B/op    0 allocs/op
BenchmarkAnomaly_Observe_New-12         251 ns/op  169 B/op    1 alloc/op
BenchmarkAnomaly_IsAnomaly-12            26 ns/op    0 B/op    0 allocs/op
BenchmarkBloom_Add-12                    20 ns/op    0 B/op    0 allocs/op
BenchmarkBloom_Contains-12               15 ns/op    0 B/op    0 allocs/op
BenchmarkBloom_AddParallel-12            47 ns/op    0 B/op    0 allocs/op
BenchmarkDeduper_SeenNew-12             127 ns/op    0 B/op    0 allocs/op
BenchmarkDeduper_SeenRepeated-12         61 ns/op    0 B/op    0 allocs/op
BenchmarkDeduper_SeenParallel-12        165 ns/op    0 B/op    0 allocs/op

- **Anomaly Observe Existing**: 26 ns - это обновление EWMA для уже отслеживаемой query.
- **Anomaly Observe New**: 251 ns - это создание новой `queryStats` структуры (одна аллокация).
- **Deduper.Seen**: 127 ns для нового ключа, 61 ns для повторного.

## Стоимость одного события (E2E)

Сложив всё на пути Kafka в Window:

| шаг | стоимость |
|---|---|
| JSON unmarshal | 1-2 µs |
| validate | 100 ns |
| dedup (Seen) | 127 ns |
| anomaly (Observe) | 26-251 ns |
| window (Add) | 59 ns |
| **итого** | **2-3 µs на событие** |

350-500k событий в секунду на одно ядро.

## Наблюдаемость во время прогонов

Скриншот Prometheus с метрикой `rate(trending_events_consumed_total[1m])` за период всех прогонов: `prometheus-events.png`.

На графике видны:

- Голубая зона `status=ok`: события, прошедшие все фильтры
- Зелёная зона `status=dropped_dedup`: события, отсеянные Bloom-фильтром.
- В `attack`-сценариях зелёная зона занимает примерно 30% потока (`-bot-share=0.3`) в нагрузочном генераторе.