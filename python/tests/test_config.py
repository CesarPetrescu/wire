"""Tests for the Wire YAML configuration system."""

import os
import tempfile

import pytest

from wire.config import (
    WireConfig,
    load_config,
    validate_config,
    _expand_env,
    _parse_duration,
)


# ── Duration parsing ─────────────────────────────────────────────────────────

class TestParseDuration:
    def test_seconds(self):
        assert _parse_duration("30s") == 30.0

    def test_minutes(self):
        assert _parse_duration("1m") == 60.0

    def test_hours(self):
        assert _parse_duration("2h") == 7200.0

    def test_numeric(self):
        assert _parse_duration(42) == 42.0

    def test_float_string(self):
        assert _parse_duration("1.5s") == 1.5

    def test_invalid(self):
        with pytest.raises(ValueError):
            _parse_duration("invalid")


# ── Env var expansion ────────────────────────────────────────────────────────

class TestEnvExpansion:
    def test_expand_single(self):
        os.environ["WIRE_TEST_SECRET"] = "my-secret"
        assert _expand_env("${WIRE_TEST_SECRET}") == "my-secret"
        del os.environ["WIRE_TEST_SECRET"]

    def test_expand_in_context(self):
        os.environ["WIRE_TEST_VAR"] = "hello"
        assert _expand_env("prefix_${WIRE_TEST_VAR}_suffix") == "prefix_hello_suffix"
        del os.environ["WIRE_TEST_VAR"]

    def test_no_expansion(self):
        assert _expand_env("plain text") == "plain text"

    def test_missing_var(self):
        with pytest.raises(ValueError, match="not set"):
            _expand_env("${WIRE_NONEXISTENT_VAR_12345}")


# ── Config loading ───────────────────────────────────────────────────────────

class TestLoadConfig:
    def test_controller_config(self):
        yaml_content = """
role: controller
listen:
  host: "0.0.0.0"
  port: 9000
auth:
  secret: "test-secret"
proxy:
  enabled: true
  port: 8080
  read_timeout: 30s
  static_routes:
    - prefix: /legacy
      upstream: http://10.0.0.5:9000
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            cfg = load_config(f.name)

        assert cfg.role == "controller"
        assert cfg.listen.host == "0.0.0.0"
        assert cfg.listen.port == 9000
        assert cfg.auth.resolved_secret() == "test-secret"
        assert cfg.proxy.enabled is True
        assert cfg.proxy.port == 8080
        assert cfg.proxy.read_timeout == 30.0
        assert len(cfg.proxy.static_routes) == 1
        assert cfg.proxy.static_routes[0].prefix == "/legacy"
        os.unlink(f.name)

    def test_sub_config(self):
        yaml_content = """
role: sub
auth:
  secret: "test-secret"
controller:
  url: "wss://192.168.10.1:8765"
  reconnect:
    enabled: true
    initial_delay: 1s
    max_delay: 30s
    max_attempts: 0
services:
  - prefix: /api
    upstream: http://localhost:3000
    health_check: /health
  - prefix: /dashboard
    upstream: http://localhost:4000
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            cfg = load_config(f.name)

        assert cfg.role == "sub"
        assert cfg.controller.url == "wss://192.168.10.1:8765"
        assert cfg.controller.reconnect.enabled is True
        assert cfg.controller.reconnect.initial_delay == 1.0
        assert cfg.controller.reconnect.max_delay == 30.0
        assert len(cfg.services) == 2
        assert cfg.services[0].prefix == "/api"
        assert cfg.services[0].health_check == "/health"
        os.unlink(f.name)

    def test_env_expansion_in_config(self):
        os.environ["WIRE_TEST_PORT"] = "9999"
        os.environ["WIRE_TEST_SECRET_2"] = "env-secret"
        yaml_content = """
role: controller
listen:
  port: ${WIRE_TEST_PORT}
auth:
  secret: "${WIRE_TEST_SECRET_2}"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            cfg = load_config(f.name)

        # Port comes as string from env expansion, but yaml parsing should handle it
        assert cfg.auth.resolved_secret() == "env-secret"
        del os.environ["WIRE_TEST_PORT"]
        del os.environ["WIRE_TEST_SECRET_2"]
        os.unlink(f.name)

    def test_defaults(self):
        yaml_content = "role: controller\nauth:\n  secret: test\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            cfg = load_config(f.name)

        assert cfg.listen.host == "0.0.0.0"
        assert cfg.listen.port == 8765
        assert cfg.node.cert_dir == "./certs"
        assert cfg.log.level == "info"
        os.unlink(f.name)


# ── Validation ───────────────────────────────────────────────────────────────

class TestValidation:
    def test_valid_controller(self):
        cfg = WireConfig()
        cfg.role = "controller"
        cfg.auth.secret = "secret"
        assert validate_config(cfg) == []

    def test_controller_missing_secret(self):
        cfg = WireConfig()
        cfg.role = "controller"
        errors = validate_config(cfg)
        assert any("secret" in e.lower() for e in errors)

    def test_valid_sub(self):
        cfg = WireConfig()
        cfg.role = "sub"
        cfg.auth.secret = "secret"
        cfg.controller.url = "wss://1.2.3.4:8765"
        assert validate_config(cfg) == []

    def test_sub_missing_url(self):
        cfg = WireConfig()
        cfg.role = "sub"
        cfg.auth.secret = "secret"
        errors = validate_config(cfg)
        assert any("url" in e.lower() for e in errors)

    def test_invalid_role(self):
        cfg = WireConfig()
        cfg.role = "invalid"
        errors = validate_config(cfg)
        assert any("role" in e.lower() for e in errors)
