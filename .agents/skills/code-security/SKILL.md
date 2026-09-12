---
name: code-security
description: Security guidelines for credentials, HMAC-SHA256 authentication, file path traversal defense, fail-closed design, and exchange API safety in Escanor.
---

# Code Security & Hardening Skill

Security guidelines and defensive programming standards for the Escanor trading engine.

## 1. When to Use This Skill

Activate this skill when:
- Touching Binance API keys, secrets, or HMAC-SHA256 signature logic.
- Processing file paths, configuration files, database paths, or shell arguments.
- Handling deserialization of network payloads (JSON, WebSocket messages).
- Evaluating safety gates, pre-trade risk checks, or emergency kill switch mechanics.

## 2. Mandatory Security Rules

1. **Credential Protection**:
   - Never hardcode Binance API keys or secrets in source code, configuration files, or logs.
   - Load credentials strictly from environment variables (`BINANCE_API_KEY`, `BINANCE_API_SECRET`).
   - Redact credentials in all logging and error traces: display at most the first 4 characters.
2. **Fail-Closed Architecture**:
   - If an API key is invalid, network disconnect occurs, kill switch file is corrupt, or safety gates fail: **the engine must fail closed** (reject order submission, refuse to start in LIVE mode).
   - The default mode must ALWAYS be `SHADOW` or `PAPER`. Never default to `LIVE`.
3. **Database Path Containment (Anti-Traversal)**:
   - All database files must resolve strictly within the repository `data/` directory.
   - Prevent directory traversal (`..`) using `Path(p).resolve().relative_to(data_dir)`.
4. **HMAC-SHA256 Request Signing**:
   - All private Binance REST requests must be timestamped with millisecond precision and signed via HMAC-SHA256 using `hashlib.sha256`.
   - Prevent replay attacks by keeping local system clock synchronized with Binance server time via `/fapi/v1/time` (`recvWindow=5000`).
5. **Frozen Strategy Integrity**:
   - Cryptographically verify the SHA-256 hash of strategy Python files and helper modules against the frozen manifest before execution. Refuse startup if hashes do not match.
