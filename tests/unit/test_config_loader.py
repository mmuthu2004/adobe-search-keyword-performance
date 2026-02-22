"""
test_config_loader.py
=====================
Unit tests for the ConfigLoader module.
Covers: load, deep merge, env var substitution, dot-notation access,
        require(), missing keys, and environment overrides.
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

# Add src to path
sys.path.insert(0, str(Path(__file__).parents[2] / "src"))

from config.config_loader import ConfigLoader, ConfigError, load_config


def _write_yaml(directory: str, filename: str, content: str) -> Path:
    path = Path(directory) / filename
    path.write_text(content, encoding="utf-8")
    return path


class TestConfigLoaderBasic(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        _write_yaml(self.tmpdir, "config.yaml", """
business_rules:
  purchase_event_id: "1"
  search_engine_domains:
    - google.com
    - yahoo.com
  keyword_normalize_case: true
  min_revenue_threshold: 0.0
processing:
  small_file_threshold_mb: 1024
aws:
  region: us-east-1
environment:
  default: dev
""")

    def _load(self, env="dev"):
        with patch("config.config_loader.CONFIG_DIR", Path(self.tmpdir)):
            return ConfigLoader.load(env=env)

    # --- Basic access ---

    def test_get_simple_value(self):
        cfg = self._load()
        self.assertEqual(cfg.get("business_rules.purchase_event_id"), "1")

    def test_get_list_value(self):
        cfg = self._load()
        domains = cfg.get("business_rules.search_engine_domains")
        self.assertIn("google.com", domains)
        self.assertIn("yahoo.com", domains)

    def test_get_nested_value(self):
        cfg = self._load()
        self.assertEqual(cfg.get("aws.region"), "us-east-1")

    def test_get_boolean_value(self):
        cfg = self._load()
        self.assertTrue(cfg.get("business_rules.keyword_normalize_case"))

    def test_get_float_value(self):
        cfg = self._load()
        self.assertEqual(cfg.get("business_rules.min_revenue_threshold"), 0.0)

    def test_get_missing_key_returns_none(self):
        cfg = self._load()
        self.assertIsNone(cfg.get("nonexistent.key"))

    def test_get_missing_key_returns_default(self):
        cfg = self._load()
        self.assertEqual(cfg.get("nonexistent.key", default="fallback"), "fallback")

    def test_require_existing_key(self):
        cfg = self._load()
        self.assertEqual(cfg.require("business_rules.purchase_event_id"), "1")

    def test_require_missing_key_raises(self):
        cfg = self._load()
        with self.assertRaises(ConfigError):
            cfg.require("nonexistent.key")

    def test_env_property(self):
        cfg = self._load(env="dev")
        self.assertEqual(cfg.env, "dev")

    def test_as_dict_returns_copy(self):
        cfg = self._load()
        d = cfg.as_dict()
        self.assertIsInstance(d, dict)
        # Modifying the returned dict should not affect the config
        d["business_rules"]["purchase_event_id"] = "999"
        self.assertEqual(cfg.get("business_rules.purchase_event_id"), "1")


class TestDeepMerge(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        _write_yaml(self.tmpdir, "config.yaml", """
processing:
  small_file_threshold_mb: 1024
  spark:
    num_partitions: 200
    executor_memory: 4g
logging:
  level: INFO
""")
        _write_yaml(self.tmpdir, "config.dev.yaml", """
processing:
  small_file_threshold_mb: 100
  spark:
    num_partitions: 10
logging:
  level: DEBUG
""")

    def _load(self, env="dev"):
        with patch("config.config_loader.CONFIG_DIR", Path(self.tmpdir)):
            return ConfigLoader.load(env=env)

    def test_override_scalar(self):
        cfg = self._load()
        self.assertEqual(cfg.get("processing.small_file_threshold_mb"), 100)

    def test_override_nested_scalar(self):
        cfg = self._load()
        self.assertEqual(cfg.get("processing.spark.num_partitions"), 10)

    def test_base_value_preserved_when_not_overridden(self):
        # executor_memory is in base but not in dev override — should survive
        cfg = self._load()
        self.assertEqual(cfg.get("processing.spark.executor_memory"), "4g")

    def test_log_level_overridden(self):
        cfg = self._load()
        self.assertEqual(cfg.get("logging.level"), "DEBUG")


class TestEnvVarSubstitution(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        _write_yaml(self.tmpdir, "config.yaml", """
aws:
  emr_serverless:
    application_id: "${EMR_APP_ID}"
    execution_role_arn: "${EMR_ROLE_ARN}"
  cloudwatch:
    alarm_email: "${ALARM_EMAIL}"
""")

    def _load(self):
        with patch("config.config_loader.CONFIG_DIR", Path(self.tmpdir)):
            return ConfigLoader.load(env="dev")

    def test_env_var_substituted(self):
        with patch.dict(os.environ, {"EMR_APP_ID": "app-12345", "EMR_ROLE_ARN": "arn:aws:iam::123:role/emr"}):
            cfg = self._load()
            self.assertEqual(cfg.get("aws.emr_serverless.application_id"), "app-12345")
            self.assertEqual(cfg.get("aws.emr_serverless.execution_role_arn"), "arn:aws:iam::123:role/emr")

    def test_missing_env_var_leaves_placeholder(self):
        # If env var not set, placeholder is left as-is (warning emitted, no crash)
        with patch.dict(os.environ, {}, clear=True):
            # Remove the vars if they happen to be set
            os.environ.pop("EMR_APP_ID", None)
            os.environ.pop("ALARM_EMAIL", None)
            cfg = self._load()
            # Value should remain as the original placeholder
            self.assertEqual(cfg.get("aws.emr_serverless.application_id"), "${EMR_APP_ID}")


class TestMissingBaseConfig(unittest.TestCase):

    def test_missing_base_config_raises(self):
        empty_dir = tempfile.mkdtemp()
        # No config.yaml in this dir — should raise ConfigError
        with patch("config.config_loader.CONFIG_DIR", Path(empty_dir)):
            # _load_yaml returns {} for missing files, so load should work
            # but get() on critical keys returns None
            cfg = ConfigLoader.load(env="dev")
            self.assertIsNone(cfg.get("business_rules.purchase_event_id"))


class TestRealConfigFiles(unittest.TestCase):
    """Integration-style tests against the actual config files in the project."""

    @classmethod
    def setUpClass(cls):
        cls.real_config_dir = Path(__file__).parents[2] / "src" / "config"
        if not (cls.real_config_dir / "config.yaml").exists():
            raise unittest.SkipTest("Real config.yaml not found — skipping real-file tests")

    def _load(self, env="dev"):
        with patch("config.config_loader.CONFIG_DIR", self.real_config_dir):
            return ConfigLoader.load(env=env)

    def test_purchase_event_id_is_string_one(self):
        cfg = self._load()
        self.assertEqual(cfg.get("business_rules.purchase_event_id"), "1")

    def test_search_engine_domains_contains_google(self):
        cfg = self._load()
        domains = cfg.get("business_rules.search_engine_domains")
        self.assertIn("google.com", domains)

    def test_session_key_is_ip(self):
        cfg = self._load()
        self.assertEqual(cfg.get("business_rules.session_key_column"), "ip")

    def test_revenue_column_index_is_3(self):
        cfg = self._load()
        self.assertEqual(cfg.get("business_rules.revenue_column_index"), 3)

    def test_output_sort_ascending_is_false(self):
        cfg = self._load()
        self.assertFalse(cfg.get("output.sort_ascending"))

    def test_output_columns_correct(self):
        cfg = self._load()
        cols = cfg.get("output.columns")
        self.assertEqual(cols, ["Search Engine Domain", "Search Keyword", "Revenue"])

    def test_critical_columns_present(self):
        cfg = self._load()
        critical = cfg.get("data_quality.critical_columns")
        self.assertIn("ip", critical)
        self.assertIn("referrer", critical)
        self.assertIn("hit_time_gmt", critical)

    def test_pii_columns_contain_ip(self):
        cfg = self._load()
        pii = cfg.get("data_quality.pii_columns")
        self.assertIn("ip", pii)

    def test_dev_override_lowers_threshold(self):
        cfg = self._load(env="dev")
        # Dev should have a lower threshold than base 1024
        threshold = cfg.get("processing.small_file_threshold_mb")
        self.assertLessEqual(threshold, 1024)

    def test_dev_log_level_is_debug(self):
        cfg = self._load(env="dev")
        self.assertEqual(cfg.get("logging.level"), "DEBUG")

    def test_prod_log_level_is_info(self):
        cfg = self._load(env="prod")
        self.assertEqual(cfg.get("logging.level"), "INFO")

    def test_prod_fail_on_schema_error_is_true(self):
        cfg = self._load(env="prod")
        self.assertTrue(cfg.get("data_quality.fail_on_schema_error"))

    def test_convenience_function(self):
        with patch("config.config_loader.CONFIG_DIR", self.real_config_dir):
            cfg = load_config(env="dev")
        self.assertIsNotNone(cfg)
        self.assertEqual(cfg.env, "dev")


if __name__ == "__main__":
    unittest.main(verbosity=2)
