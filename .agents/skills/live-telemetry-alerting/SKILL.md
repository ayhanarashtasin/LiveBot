---
name: live-telemetry-alerting
description: Real-time observability, heartbeat watchdog supervision, and multi-channel incident alerting for the Escanor trading engine. Trigger automatically whenever the user asks about live monitoring, process liveness watchdogs, incident dispatchers, Telegram bot notifications, Discord/Slack webhooks, operational healthchecks, kill switch notifications, system status alerts, or background process failures.
---

# Live Telemetry & Incident Alerting Skill

Institutional operational guide for real-time monitoring, heartbeat watchdogs, and multi-channel incident alerting across Escanor live trading instances.

## 1. When to Use This Skill

Activate this skill when:
- Designing or maintaining the `live_engine/monitoring` subsystem.
- Adding or configuring multi-channel alerts (Telegram, Discord, Slack webhooks).
- Implementing process liveness watchdogs and heartbeat checks for background engine instances.
- Routing operational incidents (`ORDER_UNKNOWN`, `GAP_RECOVERY_FAILED`, `KILL_SWITCH_ENGAGED`, `POSITION_DISCREPANCY`).
- Ensuring the operator receives instant mobile notifications upon critical events without blocking engine event loops.

## 2. Core Architecture & Operating Principles

1. **Zero Impact on Strategy Execution (Asynchronous & Non-Blocking)**:
   - Alert dispatching must never block market data processing, candle generation, or order placement.
   - Use background asyncio tasks (`asyncio.create_task`) or bounded in-memory queues with dropping semantics on queue overflow.
2. **Reuse Installed Dependencies (Ponytail Rule #2 & #5)**:
   - Use the pre-installed `python-telegram-bot` package (or stdlib `urllib.request` / `aiohttp` for webhooks).
   - Do NOT introduce heavy telemetry frameworks (e.g. Datadog agent, Prometheus server) when lightweight direct webhooks suffice.
3. **Fail-Safe & Throttled Dispatching**:
   - Prevent alert storms (e.g., thousands of aggTrade gap alerts during network flapping).
   - Deduplicate alerts with a sliding suppression window (e.g., maximum 1 identical alert per 5 minutes).
   - High-severity incidents (`CRITICAL`, `HIGH`) bypass throttling or use separate alert queues.

## 3. Incident Severity Levels & Routing Matrix

| Severity | Definition | Target Channel | Throttle Window |
|---|---|---|---|
| `CRITICAL` | Emergency kill switch engaged, unresolvable account position mismatch, live authentication failure. | Telegram Urgent + SMS/Webhook | 0s (Instant, Always) |
| `HIGH` | Order timeout (`UNKNOWN` status), REST backfill failure, order placement rejected by exchange. | Telegram Priority | 30s |
| `MEDIUM` | AggTrade sequence gap detected, kline validator OHLCV tolerance breach, websocket reconnect. | Telegram Standard | 300s (5 minutes) |
| `INFO` | Periodic health heartbeat (every 8h), trade execution fill confirmation, system startup/shutdown. | Telegram Digest / Silent | Scheduled |

## 4. Implementation Guidelines

### 4.1 Dispatcher Implementation (`live_engine/monitoring/alert_dispatcher.py`)

```python
import asyncio
import logging
import time
from typing import Dict, Optional
from telegram import Bot

logger = logging.getLogger("escanor.monitoring")

class AlertDispatcher:
    def __init__(self, token: Optional[str] = None, chat_id: Optional[str] = None):
        self.token = token
        self.chat_id = chat_id
        self.bot = Bot(token=token) if token else None
        self._last_sent: Dict[str, float] = {}

    async def send_alert(self, category: str, severity: str, message: str, throttle_seconds: int = 60) -> bool:
        if not self.bot or not self.chat_id:
            logger.debug(f"[ALERT-MOCK] [{severity}] {category}: {message}")
            return False

        dedup_key = f"{category}:{severity}"
        now = time.time()
        if severity != "CRITICAL" and (now - self._last_sent.get(dedup_key, 0) < throttle_seconds):
            logger.debug(f"Throttled alert {dedup_key}")
            return False

        icon = "🚨" if severity == "CRITICAL" else "⚠️" if severity == "HIGH" else "ℹ️"
        text = f"{icon} *[ESCANOR {severity}]* `{category}`\n\n{message}\n\n_Time: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}_"
        
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=text, parse_mode="Markdown")
            self._last_sent[dedup_key] = now
            return True
        except Exception as e:
            logger.error(f"Failed to dispatch Telegram alert: {e}")
            return False
```

### 4.2 Heartbeat Watchdog (`live_engine/monitoring/watchdog.py`)

A background coroutine checking market tick liveness:
- If no aggTrade has arrived for $> 120$ seconds during market hours, log an alert and set health status to `DEGRADED`.
- If the database file is locked or unresponsive for $> 15$ seconds, trigger an alert.

## 5. Verification & Testing

- Unit tests must mock Telegram API calls using `unittest.mock.AsyncMock`.
- Test alert throttling logic: send 5 identical alerts in 1 second and assert only 1 dispatch occurred.
- Test critical alert bypass: ensure `CRITICAL` severity bypasses throttling.
- All tests must pass within standard pytest execution (`py -m pytest tests/`).
