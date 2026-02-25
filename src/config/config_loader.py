"""
config_loader.py
================
Central configuration loader for the Search Keyword Performance platform.

Design principles:
  - Single source of truth: all code reads config through this module
  - Environment-aware: base config.yaml is deep-merged with config.{env}.yaml
  - Environment variable substitution: ${VAR_NAME} in YAML values are
    replaced with actual environment variable values at load time
  - Fail-fast: missing required config keys raise ConfigError immediately
  - Immutable after load: ConfigLoader returns a read-only view

Usage:
    from config.config_loader import ConfigLoader

    cfg = ConfigLoader.load()                      # uses APP_ENV env var (default: dev)
    cfg = ConfigLoader.load(env="prod")            # explicit environment

    # Access values
    domains = cfg.get("business_rules.search_engine_domains")
    threshold = cfg.get("processing.small_file_threshold_mb")
    region = cfg.get("aws.region")

    # Access with default
    level = cfg.get("logging.level", default="INFO")

Version : 1.0
"""

import copy
import logging
import os
import re
from pathlib import Path
from typing import Any, Optional

import yaml

logger = logging.getLogger(__name__)

# Directory containing all config YAML files
CONFIG_DIR = Path(__file__).parent

# Environment variable that controls which env override is loaded
APP_ENV_VAR = "APP_ENV"

# Regex to match ${VAR_NAME} substitution placeholders in YAML values
ENV_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


class ConfigError(Exception):
    """Raised when configuration is invalid, missing, or cannot be loaded."""
    pass


class ConfigLoader:
    """
    Loads and merges configuration from base + environment-specific YAML files.

    Merge strategy (deep merge):
      1. Load config.yaml (base — all defaults)
      2. Load config.{env}.yaml (environment overrides)
      3. Deep-merge: environment values override base values
      4. Substitute ${ENV_VAR} placeholders with actual environment variables

    The result is a single merged dict accessible via dot-notation keys.
    """

    _instance: Optional["ConfigLoader"] = None
    _config: Optional[dict] = None

    def __init__(self, config: dict, env: str):
        self._config = config
        self._env = env

    # ------------------------------------------------------------------ #
    # Factory method
    # ------------------------------------------------------------------ #

    @classmethod
    def load(cls, env: Optional[str] = None) -> "ConfigLoader":
        """
        Load and return a ConfigLoader instance.

        Args:
            env: Environment name ('dev', 'staging', 'prod').
                 If None, reads APP_ENV environment variable (default: 'dev').

        Returns:
            ConfigLoader instance with merged configuration.

        Raises:
            ConfigError: If base config file is missing or YAML is invalid.
        """
        resolved_env = env or os.getenv(APP_ENV_VAR, "dev")
        logger.info("Loading configuration for environment: %s", resolved_env)

        base_config = cls._load_yaml(CONFIG_DIR / "config.yaml")
        env_override = cls._load_env_override(resolved_env)

        merged = cls._deep_merge(base_config, env_override)
        substituted = cls._substitute_env_vars(merged)

        # Inject resolved environment name into config
        substituted.setdefault("environment", {})["resolved"] = resolved_env

        instance = cls(substituted, resolved_env)
        logger.info(
            "Configuration loaded successfully. Environment: %s | "
            "Search engines: %s | Attribution: %s | Engine threshold: %s MB",
            resolved_env,
            substituted.get("business_rules", {}).get("search_engine_domains"),
            substituted.get("business_rules", {}).get("attribution_model"),
            substituted.get("processing", {}).get("small_file_threshold_mb"),
        )
        return instance

    # ------------------------------------------------------------------ #
    # Public access API
    # ------------------------------------------------------------------ #

    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Retrieve a config value using dot-notation key path.

        Args:
            key_path: Dot-separated key path, e.g. 'business_rules.purchase_event_id'
            default:  Value to return if the key does not exist.

        Returns:
            Config value at the key path, or default if not found.

        Example:
            cfg.get("business_rules.search_engine_domains")
            cfg.get("aws.s3.bucket_raw")
            cfg.get("processing.small_file_threshold_mb", default=1024)
        """
        keys = key_path.split(".")
        node = self._config
        for key in keys:
            if not isinstance(node, dict) or key not in node:
                if default is not None:
                    return default
                return None
            node = node[key]
        return node

    def require(self, key_path: str) -> Any:
        """
        Retrieve a config value, raising ConfigError if it does not exist.

        Use for values that are truly mandatory — no sensible default exists.

        Args:
            key_path: Dot-separated key path.

        Returns:
            Config value.

        Raises:
            ConfigError: If the key does not exist or its value is None.
        """
        value = self.get(key_path)
        if value is None:
            raise ConfigError(
                f"Required configuration key '{key_path}' is missing or null. "
                f"Check config.yaml or config.{self._env}.yaml."
            )
        return value

    @property
    def env(self) -> str:
        """Current environment name."""
        return self._env

    def as_dict(self) -> dict:
        """Return a deep copy of the full config as a plain dict."""
        return copy.deepcopy(self._config)

    def __repr__(self) -> str:
        return f"ConfigLoader(env={self._env})"

    # ------------------------------------------------------------------ #
    # Private helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _load_yaml(path: Path) -> dict:
        """
        Load a YAML file and return its contents as a dict.

        Args:
            path: Path to the YAML file.

        Returns:
            Parsed dict, or empty dict if file does not exist.

        Raises:
            ConfigError: If the file exists but cannot be parsed.
        """
        if not path.exists():
            return {}
        try:
            with open(path, encoding="utf-8") as fh:
                content = yaml.safe_load(fh)
                return content or {}
        except yaml.YAMLError as exc:
            raise ConfigError(f"Failed to parse YAML at {path}: {exc}") from exc

    @classmethod
    def _load_env_override(cls, env: str) -> dict:
        """Load the environment-specific override file if it exists."""
        override_path = CONFIG_DIR / f"config.{env}.yaml"
        if not override_path.exists():
            logger.warning(
                "No environment override file found at %s. "
                "Using base config.yaml defaults only.",
                override_path,
            )
            return {}
        logger.debug("Loading environment override: %s", override_path)
        return cls._load_yaml(override_path)

    @classmethod
    def _deep_merge(cls, base: dict, override: dict) -> dict:
        """
        Recursively merge override dict into base dict.

        Override values win at every level. Lists are replaced entirely
        (not appended), which allows env overrides to replace list values.

        Args:
            base:     Base configuration dict.
            override: Environment-specific override dict.

        Returns:
            New merged dict (base is not mutated).
        """
        result = copy.deepcopy(base)
        for key, override_value in override.items():
            base_value = result.get(key)
            if isinstance(base_value, dict) and isinstance(override_value, dict):
                result[key] = cls._deep_merge(base_value, override_value)
            else:
                result[key] = copy.deepcopy(override_value)
        return result

    @classmethod
    def _substitute_env_vars(cls, obj: Any) -> Any:
        """
        Recursively walk the config tree and replace ${VAR_NAME} placeholders
        with actual environment variable values.

        Args:
            obj: Config value (dict, list, str, or other).

        Returns:
            Config with all substitutions applied.

        Raises:
            ConfigError: If a required ${VAR_NAME} is not set in the environment.
        """
        if isinstance(obj, dict):
            return {k: cls._substitute_env_vars(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [cls._substitute_env_vars(item) for item in obj]
        if isinstance(obj, str):
            def replacer(match):
                var_name = match.group(1)
                value = os.getenv(var_name)
                if value is None:
                    logger.warning(
                        "Environment variable '${%s}' referenced in config "
                        "is not set. Leaving placeholder as-is.", var_name
                    )
                    return match.group(0)   # return original placeholder
                return value
            return ENV_VAR_PATTERN.sub(replacer, obj)
        return obj


# ------------------------------------------------------------------ #
# Module-level convenience function
# ------------------------------------------------------------------ #

def load_config(env: Optional[str] = None) -> ConfigLoader:
    """
    Module-level convenience wrapper for ConfigLoader.load().

    Usage:
        from config.config_loader import load_config
        cfg = load_config()
        cfg = load_config(env="prod")
    """
    return ConfigLoader.load(env=env)
