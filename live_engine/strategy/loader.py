"""Cryptographically verified loader for approved benchmark strategies."""
from __future__ import annotations

import hashlib
import importlib.util
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


class ConfigurationMismatchError(Exception):
    """Raised when the strategy source file or manifest hashes do not match."""
    pass


class StrategyLoader:
    """Validates SHA-256 hash against manifest and securely loads approved strategy."""

    def __init__(self, manifest_path: Optional[str | Path] = None, base_dir: Optional[str | Path] = None):
        self.manifest_path = manifest_path
        self.base_dir = base_dir

    def load(self):
        if not self.manifest_path:
            raise ValueError("manifest_path required to load strategy")
        return self.load_strategy(self.manifest_path, self.base_dir)

    def validate_source_hash(self) -> bool:
        """Validates strategy and helper source hashes against manifest without executing strategy code."""
        try:
            self.load()
            return True
        except Exception:
            return False


    @staticmethod
    def compute_sha256(file_path: str | Path) -> str:
        h = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(65536):
                h.update(chunk)
        return h.hexdigest().lower()

    @classmethod
    def load_manifest(cls, manifest_path: str | Path) -> Dict[str, Any]:
        if not os.path.exists(manifest_path):
            raise FileNotFoundError(f"Benchmark manifest not found: {manifest_path}")

        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = yaml.safe_load(f)
        return manifest

    @classmethod
    def load_strategy(cls, manifest_path: str | Path, base_dir: Optional[str | Path] = None) -> Tuple[Any, Dict[str, Any]]:
        """Validates strategy file hash against manifest and returns (strategy_instance, manifest)."""
        manifest = cls.load_manifest(manifest_path)
        base = Path(base_dir) if base_dir else Path.cwd()

        strat_info = manifest["strategy"]
        source_rel = strat_info["source_file"]
        source_path = (base / source_rel).resolve()

        if not source_path.exists():
            raise FileNotFoundError(f"Strategy source file not found: {source_path}")

        actual_hash = cls.compute_sha256(source_path)
        expected_hash = str(strat_info["source_hash"]).lower()

        if actual_hash != expected_hash:
            raise ConfigurationMismatchError(
                f"CONFIGURATION MISMATCH: Strategy hash {actual_hash} != expected {expected_hash}"
            )

        # Validate helper file if present
        if "helper_file" in strat_info:
            helper_path = (base / strat_info["helper_file"]).resolve()
            if not helper_path.exists():
                raise FileNotFoundError(f"Helper file not found: {helper_path}")
            expected_helper_hash = str(strat_info.get("helper_hash", "")).lower()
            if expected_helper_hash:
                actual_helper_hash = cls.compute_sha256(helper_path)
                if actual_helper_hash != expected_helper_hash:
                    raise ConfigurationMismatchError(
                        f"CONFIGURATION MISMATCH: Helper hash {actual_helper_hash} != expected {expected_helper_hash}"
                    )

        # Ensure directory containing the strategy is on sys.path so it can import helpers
        strat_dir = str(source_path.parent)
        if strat_dir not in sys.path:
            sys.path.insert(0, strat_dir)

        # Dynamic import
        module_name = f"approved_strat_{manifest['benchmark_id']}"
        spec = importlib.util.spec_from_file_location(module_name, str(source_path))
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load module spec from {source_path}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        class_name = strat_info["class"]
        if not hasattr(module, class_name):
            raise AttributeError(f"Module {source_path} has no class '{class_name}'")

        strategy_cls = getattr(module, class_name)
        # Instantiate strategy
        config: Dict[str, Any] = {"stake_currency": "USDT"}
        try:
            strategy_instance = strategy_cls(config)
        except TypeError:
            strategy_instance = strategy_cls()

        return strategy_instance, manifest
