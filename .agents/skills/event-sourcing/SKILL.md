---
name: event-sourcing
description: Durable market-event storage, immutable signals, audit trails, replay, projections, reconciliation history, and SQLite WAL event-store design in Escanor.
---

# Event Sourcing & Persistence Skill

Institutional guide for immutable event logging, state reconstruction, SQLite WAL optimization, and audit ledger integrity in Escanor.

## 1. When to Use This Skill

Activate this skill when:
- Designing, querying, or migrating tables in `live_engine/persistence/event_store.py`.
- Adding new structured audit events (`audit_events` table).
- Optimizing SQLite concurrency with Write-Ahead Logging (`PRAGMA journal_mode=WAL;`).
- Replaying events to reconstruct historical order states, positions, or cash balances.
- Enforcing database path isolation strictly underneath `data/`.

## 2. Core Principles of Escanor Event Sourcing

1. **Immutability of Historical Events**:
   - `raw_aggtrades`, `candles`, `signals`, and `audit_events` are append-only.
   - Never run `UPDATE` or `DELETE` on market data or signal records.
2. **Crash Tolerance via SQLite WAL**:
   - Every connection must execute:
     ```sql
     PRAGMA journal_mode = WAL;
     PRAGMA synchronous = NORMAL;
     PRAGMA foreign_keys = ON;
     PRAGMA busy_timeout = 5000;
     ```
3. **Database Isolation Under `data/`**:
   - Each running instance has its own isolated database (`data/btc_paper.db`, `data/lit_paper.db`, `data/zec_paper.db`).
   - Traversal attacks (`../`) and shared database collisions are strictly forbidden.
4. **Separation of Read vs. Write Connections**:
   - Web dashboards connect with `mode=ro` (read-only SQLite URI).
   - Engine processes own the exclusive write connection.

## 3. Standard Tables & Schema

- `raw_aggtrades`: All ingested raw Binance trades (ID, price, quantity, timestamp, is_buyer_maker).
- `candles`: Reconstructed 1m and higher-timeframe closed bars.
- `signals`: Generated `SignalEvent` records with reference prices and indicator snapshots.
- `orders`: Order lifecycle tracking (`client_order_id`, status, filled_quantity, avg_price, fee).
- `incidents`: Operational warnings, gap backfill failures, and exceptions.
- `audit_events`: Timestamped system milestones (`SYSTEM_INITIALIZED`, `POSITION_RECONCILED`, `KILL_SWITCH_TRIPPED`).
