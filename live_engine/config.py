"""Live Engine configuration models and loader.

Enforces:
- Safe default mode: SHADOW.
- Strict typing and validation.
- Environment variable overrides.
"""
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Optional, Dict, Any
import json
import os
import yaml


@dataclass
class LiveEngineConfig:
    mode: str = "SHADOW"
    symbol: str = "BTCUSDT"
    timeframe: str = "5m"
    manifest_path: str = "benchmarks/manifests/BTC_ST_09_5M.yaml"
    event_store_path: str = "data/btc_shadow.db"  # Changed to data/ directory for consistency
    warmup_candles: int = 250
    stake_amount: Decimal = Decimal("1000.0")
    stake_rule: str = "full_compounding"
    max_open_trades: int = 1
    canary_mode: bool = False
    max_canary_allocation_usd: Decimal = Decimal("100.0")
    kill_switch_path: str = ".kill_switch"
    binance_api_key: Optional[str] = None
    binance_api_secret: Optional[str] = None
    binance_testnet: bool = False
    log_level: str = "INFO"

    def is_live_mode(self) -> bool:
        return self.mode.upper() == "LIVE"


# Execution semantics every manifest must state explicitly. A missing value used to fall
# back to a runner default, which is how LIT/ZEC ended up replaying next-open fills while
# the runtime sent market orders on the signal candle.
REQUIRED_EXECUTION_FIELDS = (
    "entry_order_type",
    "exit_order_type",
    "entry_fill_model",
    "exit_fill_model",
    "stop_model",
    "take_profit_model",
)
SUPPORTED_FILL_MODELS = ("signal_close", "signal_reference", "next_open")
VALID_MODES = ("SHADOW", "PAPER", "TESTNET", "LIVE")

# Credential fields are never accepted from a configuration file. A config file is
# committed, copied and pasted into tickets; a secret in one is a secret in all of them.
FORBIDDEN_CREDENTIAL_FIELDS = (
    "binance_api_key", "binance_api_secret", "binance_secret_key",
    "api_key", "api_secret", "secret_key", "listen_key", "private_key",
)

_TRUE = frozenset({"true", "1", "yes", "on"})
_FALSE = frozenset({"false", "0", "no", "off", ""})


def parse_bool(value: Any, field: str) -> bool:
    """Strict boolean parsing.

    ``bool("false")`` is True in Python, which is how a config that said "false" ended up
    enabling a feature. Anything that is not an explicit truthy or falsy token is a
    configuration error, not a default.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    text = str(value).strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    raise ValueError(
        f"CONFIGURATION ERROR: {field}={value!r} is not a valid boolean. "
        f"Use one of {sorted(_TRUE)} or {sorted(_FALSE)}."
    )


def parse_mode(value: Any) -> str:
    """Validates the execution mode against the explicit enum.

    An unknown mode is refused outright; it is never quietly mapped to SHADOW, because a
    typo in a LIVE config must not run silently under different rules.
    """
    mode = str(value or "").strip().upper()
    if mode not in VALID_MODES:
        raise ValueError(
            f"CONFIGURATION ERROR: mode={value!r} is not a valid execution mode. "
            f"Valid modes: {', '.join(VALID_MODES)}."
        )
    return mode


def reject_config_credentials(cfg_data: Dict[str, Any], source: str) -> None:
    """Fails closed when a config file carries credential fields."""
    found = sorted({
        key for key in FORBIDDEN_CREDENTIAL_FIELDS
        if key in cfg_data or key in (cfg_data.get("execution") or {})
        or key in (cfg_data.get("binance") or {})
    })
    if found:
        raise ValueError(
            f"CREDENTIALS IN CONFIG: {source} contains {', '.join(found)}. "
            f"API credentials must come only from environment variables "
            f"(BINANCE_API_KEY / BINANCE_API_SECRET). Remove these fields."
        )


def validate_execution_model(manifest: Dict[str, Any]) -> Dict[str, Any]:
    """Returns the manifest's execution model, failing closed on missing semantics."""
    execution = manifest.get("execution_model") or {}
    benchmark_id = manifest.get("benchmark_id", "UNKNOWN")

    missing = [f for f in REQUIRED_EXECUTION_FIELDS if not execution.get(f)]
    if missing:
        raise ValueError(
            f"MANIFEST INCOMPLETE ({benchmark_id}): execution_model is missing "
            f"{', '.join(missing)}. Execution semantics must be stated explicitly; "
            f"they are never defaulted."
        )

    for field in ("entry_fill_model", "exit_fill_model"):
        value = str(execution[field])
        if value not in SUPPORTED_FILL_MODELS:
            raise ValueError(
                f"MANIFEST INVALID ({benchmark_id}): {field}={value!r} is not one of "
                f"{SUPPORTED_FILL_MODELS}."
            )
    return execution


def validate_config_against_manifest(config: LiveEngineConfig, manifest: Dict[str, Any]) -> None:
    """Validates that runtime config matches manifest requirements. Fails if mismatch."""
    manifest_data = manifest.get("data", {})
    manifest_symbol = manifest.get("symbol")
    manifest_timeframe = manifest_data.get("strategy_timeframe", "15m")
    manifest_signal = manifest.get("signal", {})
    manifest_warmup = manifest_signal.get("warmup_candles", 250)
    manifest_risk = manifest.get("risk", {})
    manifest_max_trades = manifest_risk.get("max_open_trades", 1)
    manifest_stake_rule = manifest_risk.get("stake_rule", "full_compounding")

    # Check allowed modes (fail-closed for LIT/ZEC with TESTNET/LIVE)
    allowed_modes = manifest.get("allowed_modes")
    if allowed_modes:
        if config.mode.upper() not in [m.upper() for m in allowed_modes]:
            raise ValueError(
                f"MODE NOT ALLOWED: Mode {config.mode} is not permitted for symbol {manifest_symbol}. "
                f"Allowed modes: {', '.join(allowed_modes)}. This restriction prevents accidental TESTNET/LIVE execution."
            )

    if manifest_symbol and config.symbol.upper() != manifest_symbol.upper():
        raise ValueError(
            f"CONFIGURATION MISMATCH: Runtime symbol {config.symbol} does not match manifest symbol {manifest_symbol}. "
            "Configuration parameters are frozen to the approved benchmark manifest."
        )

    if config.timeframe.lower() != manifest_timeframe.lower():
        raise ValueError(
            f"CONFIGURATION MISMATCH: Runtime timeframe {config.timeframe} does not match manifest timeframe {manifest_timeframe}. "
            "Configuration parameters are frozen to the approved benchmark manifest."
        )

    if config.warmup_candles != manifest_warmup:
        raise ValueError(
            f"CONFIGURATION MISMATCH: Runtime warmup candles ({config.warmup_candles}) does not match manifest ({manifest_warmup})."
        )

    if config.max_open_trades != manifest_max_trades:
        raise ValueError(
            f"CONFIGURATION MISMATCH: Runtime max open trades ({config.max_open_trades}) does not match manifest ({manifest_max_trades})."
        )

    if config.stake_rule != manifest_stake_rule:
        raise ValueError(
            f"CONFIGURATION MISMATCH: Runtime stake rule ({config.stake_rule}) does not match manifest ({manifest_stake_rule})."
        )

    # Execution semantics, fees and leverage are frozen inputs: an incomplete or
    # self-inconsistent manifest is a configuration error, never an invented rule.
    validate_execution_model(manifest)

    fees = manifest.get("fees") or {}
    if fees.get("taker") is None or fees.get("maker") is None:
        raise ValueError(
            f"MANIFEST INCOMPLETE: fees.maker and fees.taker are required for "
            f"{manifest.get('benchmark_id', 'UNKNOWN')}."
        )
    if manifest_risk.get("leverage") is None:
        raise ValueError(
            f"MANIFEST INCOMPLETE: risk.leverage is required for "
            f"{manifest.get('benchmark_id', 'UNKNOWN')}."
        )


def validate_database_path(event_store: str | Path, base_dir: Optional[Path | str] = None) -> Path:
    """Validates that a database path resolves strictly underneath the data/ directory.

    Rejects traversal attacks (e.g. data/../outside.db), directories escaping data
    (e.g. database/shared.db), pointing to data itself, and non-contained paths.
    """
    repo_root = Path(base_dir or Path.cwd()).resolve()
    data_dir = (repo_root / "data").resolve()
    p = Path(event_store)
    resolved_db = (repo_root / p).resolve() if not p.is_absolute() else p.resolve()

    try:
        rel = resolved_db.relative_to(data_dir)
    except ValueError:
        raise ValueError(
            f"DATABASE ISOLATION VIOLATION: Resolved path '{resolved_db}' is not under '{data_dir}'. "
            f"All instance databases must be in data/ for isolation. "
            f"Traversal attacks (../), symbolic links, and paths escaping data/ are not allowed."
        )

    if resolved_db == data_dir or str(rel) in (".", ""):
        raise ValueError(
            f"DATABASE ISOLATION VIOLATION: '{resolved_db}' cannot be the data directory itself. "
            f"A specific database file path is required."
        )

    return resolved_db


def validate_unique_databases(configs: list[LiveEngineConfig], base_dir: Optional[Path | str] = None) -> None:
    """Validates that all effective database paths are unique."""
    seen: dict[Path, str] = {}
    for cfg in configs:
        resolved = validate_database_path(cfg.event_store_path, base_dir=base_dir)
        if resolved in seen:
            raise ValueError(
                f"DATABASE ISOLATION VIOLATION: Duplicate database path '{resolved}' "
                f"used by symbols {seen[resolved]} and {cfg.symbol}."
            )
        seen[resolved] = cfg.symbol


def load_config(config_path: Optional[str] = None) -> LiveEngineConfig:
    """Loads and validates engine configuration."""
    cfg_data: Dict[str, Any] = {}

    target_path = config_path or os.environ.get("ESCANOR_CONFIG_PATH")
    if target_path:
        p = Path(target_path)
        if not p.exists():
            raise FileNotFoundError(f"Configuration file not found: {target_path}")
        text = p.read_text(encoding="utf-8")
        if p.suffix in (".yaml", ".yml"):
            cfg_data = yaml.safe_load(text) or {}
        else:
            cfg_data = json.loads(text)
        reject_config_credentials(cfg_data, str(p))

    # Extract nested sections if present
    persistence_cfg = cfg_data.get("persistence", {})
    execution_cfg = cfg_data.get("execution", {})

    manifest_val = (
        cfg_data.get("manifest_path")
        or cfg_data.get("benchmark_manifest_path")
        or "benchmarks/manifests/BTC_ST_09_5M.yaml"
    )
    event_store_val = (
        cfg_data.get("event_store_path")
        or persistence_cfg.get("db_path")
        or "data/btc_shadow.db"
    )

    canary_val = execution_cfg.get("canary_mode", cfg_data.get("canary_mode", False))
    canary_cap_val = execution_cfg.get("max_canary_allocation_usd", cfg_data.get("max_canary_allocation_usd", "100.0"))

    # Apply environment variable overrides
    mode = parse_mode(os.environ.get("ESCANOR_MODE", cfg_data.get("mode", "SHADOW")))
    symbol = os.environ.get("ESCANOR_SYMBOL", cfg_data.get("symbol", "BTCUSDT"))
    timeframe = os.environ.get("ESCANOR_TIMEFRAME", cfg_data.get("timeframe", "5m"))
    manifest = os.environ.get("ESCANOR_MANIFEST", manifest_val)
    # Prevent environment override of event store path for isolated instances
    if "ESCANOR_EVENT_STORE" in os.environ:
        raise ValueError(
            "DATABASE ISOLATION VIOLATION: ESCANOR_EVENT_STORE environment variable is not permitted. "
            "Each instance must use its dedicated database path from the configuration file. "
            "Unset ESCANOR_EVENT_STORE and use a configuration file to specify the database."
        )
    event_store = event_store_val

    # Validate database path is strictly under data/ directory using resolved paths (prevent traversal)
    validate_database_path(event_store)
    warmup = int(os.environ.get("ESCANOR_WARMUP", cfg_data.get("warmup_candles", 250)))
    stake = Decimal(str(os.environ.get("ESCANOR_STAKE", cfg_data.get("stake_amount", "1000.0"))))
    stake_rule = os.environ.get("ESCANOR_STAKE_RULE", cfg_data.get("stake_rule", "full_compounding"))
    max_trades = int(os.environ.get("ESCANOR_MAX_TRADES", cfg_data.get("max_open_trades", 1)))
    kill_switch = os.environ.get("ESCANOR_KILL_SWITCH", cfg_data.get("kill_switch_path", ".kill_switch"))
    # Credentials come from the environment only; reject_config_credentials has already
    # refused a config file that tried to supply them.
    api_key = os.environ.get("BINANCE_API_KEY")
    api_secret = os.environ.get("BINANCE_API_SECRET") or os.environ.get("BINANCE_SECRET_KEY")
    testnet = parse_bool(
        os.environ.get("BINANCE_TESTNET", cfg_data.get("binance_testnet", mode == "TESTNET")),
        "binance_testnet",
    )
    log_level = os.environ.get("ESCANOR_LOG_LEVEL", cfg_data.get("log_level", "INFO"))

    return LiveEngineConfig(
        mode=mode,
        symbol=symbol,
        timeframe=timeframe,
        manifest_path=manifest,
        event_store_path=event_store,
        warmup_candles=warmup,
        stake_amount=stake,
        stake_rule=stake_rule,
        max_open_trades=max_trades,
        canary_mode=parse_bool(canary_val, "canary_mode"),
        max_canary_allocation_usd=Decimal(str(canary_cap_val)),
        kill_switch_path=kill_switch,
        binance_api_key=api_key,
        binance_api_secret=api_secret,
        binance_testnet=testnet,
        log_level=log_level,
    )
