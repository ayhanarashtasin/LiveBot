# Escanor project instructions

`Project.md` is the source of truth for scope, safety gates, strategy parity, and implementation order. Do not weaken its constraints to satisfy a skill's generic workflow.

## Automatic skill use

At the start of every task, compare the user's request with the descriptions of the skills under `.agents/skills`. When a request matches a skill, read that skill's complete `SKILL.md` before acting and follow its relevant instructions. Use every skill that materially applies, while avoiding unrelated skills. Briefly tell the user which skills are being used and why.

The user's explicit instructions take precedence over skill guidance. If skill guidance conflicts with `Project.md`, follow `Project.md` unless the user explicitly changes the project requirements.

Use these project skills as follows:

- `live-telemetry-alerting` (Top Priority): real-time observability, heartbeat watchdog supervision, multi-channel incident alerting (Telegram, Discord, webhooks), kill switch alerts, process liveness, and operational healthchecks. Trigger automatically whenever user mentions monitoring, alerts, Telegram, notifications, heartbeats, watchdogs, healthchecks, or crash detection.
- `execution-quality-profiler` (Top Priority): high-precision latency profiling (T0-T4 milestones), slippage tracking, implementation shortfall, spread crossing impact, and live vs benchmark fill drift. Trigger automatically whenever user mentions execution quality, slippage, latency, fill prices, order delay, or execution drift.
- `chaos-fault-injection` (Top Priority): network fault injection, dirty socket disconnects, Binance API rate limits (429/418), HTTP 502/504 retries, flapping websockets, and adversarial resilience testing. Trigger automatically whenever user mentions chaos testing, network failures, reconnect testing, stress testing, or API error handling.
- `python-testing`: pytest setup, unit or integration tests, fixtures, mocking, test design, coverage, and TDD.
- `property-based-testing`: invariants and generated cases for parsers, candle aggregation, timestamp boundaries, normalization, deduplication, deterministic hashes, replay, and idempotency.
- `websocket-development`: Binance WebSocket clients, subscriptions, reconnects, timeouts, heartbeat behavior, stream lifecycle, backpressure, and disconnect recovery.
- `event-sourcing`: durable market-event storage, immutable signals, audit trails, replay, projections, reconciliation history, and CQRS or event-store design.
- `code-security`: use alongside the relevant domain skill whenever code handles network data, credentials, authentication, files, databases, deserialization, hashing, subprocesses, exchange APIs, or order execution, and for security reviews.
- `backtesting-frameworks`: historical replay, benchmark validation, look-ahead prevention, fees, slippage, point-in-time data, and parity analysis. Do not optimize, replace, or silently change the approved strategy or benchmark parameters.

Common combinations:

- Live monitoring & incident routing: `live-telemetry-alerting`, `code-security`, and `python-testing`.
- Execution latency & slippage analysis: `execution-quality-profiler`, `backtesting-frameworks`, and `event-sourcing`.
- Resilience & chaos hardening: `chaos-fault-injection`, `websocket-development`, `python-testing`, and `property-based-testing`.
- Phase 1 market-data implementation: `websocket-development`, `python-testing`, `property-based-testing`, and `code-security`.
- Event persistence or replay: `event-sourcing`, `python-testing`, `property-based-testing`, and `code-security`.
- Historical benchmark parity: `backtesting-frameworks`, `python-testing`, and `property-based-testing`.
- Authenticated Binance execution: `code-security`, `python-testing`, and the most relevant networking or event-state skill.

Do not use trading-signal generation, strategy optimization, copy-trading, or discretionary trading workflows unless the user explicitly changes the non-goals in `Project.md`.
