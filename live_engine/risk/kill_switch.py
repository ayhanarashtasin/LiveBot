"""Persistent kill switch for trading emergency shutdown.

Provides:
- File-persisted kill flag to ensure survive-across-restart protection.
- Immediate block on all entry orders.
- Programmatic trigger and release mechanisms.
"""
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, timezone
import json
import logging

logger = logging.getLogger(__name__)

DEFAULT_KILL_SWITCH_PATH = Path(".kill_switch")


class KillSwitch:
    """Manages persistent kill switch state."""

    def __init__(self, file_path: Optional[Path] = None):
        self.file_path = (file_path or DEFAULT_KILL_SWITCH_PATH).resolve()

    def is_engaged(self) -> bool:
        """Returns True if the kill switch is currently engaged."""
        if not self.file_path.exists():
            return False

        try:
            content = self.file_path.read_text(encoding="utf-8").strip()
            if not content:
                return True
            data = json.loads(content)
            return bool(data.get("engaged", True))
        except Exception as e:
            # Fail closed on corrupted kill switch file
            logger.error(f"Failed to read kill switch file, assuming engaged for safety: {e}")
            return True

    def trigger(self, reason: str, triggered_by: str = "SYSTEM") -> Dict[str, Any]:
        """Engages the kill switch, recording the reason and timestamp."""
        payload = {
            "engaged": True,
            "triggered_at": datetime.now(timezone.utc).isoformat(),
            "reason": reason,
            "triggered_by": triggered_by,
        }
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.file_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        logger.critical(f"KILL SWITCH ENGAGED by {triggered_by}: {reason}")
        return payload

    def disengage(self, reason: str, disengaged_by: str = "OPERATOR") -> None:
        """Disengages the kill switch with audit logging."""
        if self.file_path.exists():
            self.file_path.unlink()
        logger.warning(f"KILL SWITCH DISENGAGED by {disengaged_by}: {reason}")

    def get_status(self) -> Dict[str, Any]:
        """Gets current state details."""
        if not self.file_path.exists():
            return {"engaged": False}
        try:
            content = self.file_path.read_text(encoding="utf-8").strip()
            if not content:
                return {"engaged": True, "reason": "Empty kill switch file"}
            return json.loads(content)
        except Exception as e:
            return {"engaged": True, "error": str(e)}
