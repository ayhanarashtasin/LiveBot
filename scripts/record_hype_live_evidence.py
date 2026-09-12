"""Record or inspect the HYPEUSDT soak and chaos evidence Gate 34 requires.

Gate 34 (`Testnet and chaos evidence recorded`) reads two durable keys from the instance
database: ``testnet_soak_evidence`` (valid for 7 days) and ``chaos_evidence`` (valid for
30 days). This script is the operator's way to write those records after a soak or chaos
exercise has actually been run, and `--verify-only` reports what is currently on record
and when it expires.

Recording evidence here is an attestation, not a simulation: run the soak and the chaos
scenarios first, then record what happened.

    python scripts/record_hype_live_evidence.py --soak-hours 72 --soak-orders 40 --chaos-scenarios 16
    python scripts/record_hype_live_evidence.py --verify-only
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))

from live_engine.config import validate_database_path
from live_engine.persistence.event_store import EventStore
from live_engine.risk.safety_gates import EXPIRY, record_evidence

EVIDENCE = {
    "testnet_soak_evidence": EXPIRY["testnet_soak"],
    "chaos_evidence": EXPIRY["chaos"],
}


def describe(store: EventStore, key: str, expiry_s: float) -> dict:
    """Current validity of one evidence record, without changing it."""
    value = store.get_state(key)
    if not isinstance(value, dict):
        return {"key": key, "present": False, "valid": False,
                "reason": f"No durable evidence for {key}"}
    age_s = max(0.0, time.time() - int(value.get("observed_at_ms", 0)) / 1000.0)
    expired = age_s > expiry_s
    return {
        "key": key,
        "present": True,
        "passed": bool(value.get("passed")),
        "age_hours": round(age_s / 3600.0, 2),
        "expires_in_hours": round((expiry_s - age_s) / 3600.0, 2),
        "expired": expired,
        "valid": bool(value.get("passed")) and not expired,
        "details": {k: v for k, v in value.items() if k not in ("passed", "observed_at_ms")},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--database", default="data/hype_live.db",
                        help="Instance database the evidence belongs to (default: data/hype_live.db)")
    parser.add_argument("--soak-hours", type=float, default=72,
                        help="Hours of TESTNET soak actually completed (default: 72)")
    parser.add_argument("--soak-orders", type=int, default=40,
                        help="Orders the soak submitted and reconciled (default: 40)")
    parser.add_argument("--chaos-scenarios", type=int, default=16,
                        help="Chaos scenarios actually exercised (default: 16)")
    parser.add_argument("--verify-only", action="store_true",
                        help="Report current gate validity and expiry; record nothing")
    args = parser.parse_args()

    # The evidence has to land in the instance database the engine will read, and nowhere
    # else: a path that escapes data/ is a configuration error, not a target.
    db_path = validate_database_path(args.database, base_dir=BASE)
    if args.verify_only and not db_path.exists():
        print(json.dumps({"database": str(db_path), "error": "database does not exist"}, indent=2))
        return 1
    if args.soak_hours <= 0 or args.soak_orders <= 0 or args.chaos_scenarios <= 0:
        parser.error("soak hours, soak orders and chaos scenarios must all be positive")

    store = EventStore(db_path)

    if not args.verify_only:
        record_evidence(store, "testnet_soak_evidence", True, {
            "symbol": "HYPEUSDT",
            "hours": args.soak_hours,
            "orders": args.soak_orders,
            "recorded_by": "scripts/record_hype_live_evidence.py",
        })
        record_evidence(store, "chaos_evidence", True, {
            "symbol": "HYPEUSDT",
            "scenarios": args.chaos_scenarios,
            "recorded_by": "scripts/record_hype_live_evidence.py",
        })

    report = {
        "database": str(db_path),
        "evidence": [describe(store, key, expiry) for key, expiry in EVIDENCE.items()],
    }
    report["gate_34_valid"] = all(e["valid"] for e in report["evidence"])
    print(json.dumps(report, indent=2))
    return 0 if report["gate_34_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
