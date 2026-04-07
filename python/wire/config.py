"""
Wire configuration — YAML-based config with environment variable expansion.

Supports both controller and sub roles via a single ``wire.yaml`` file.
"""

import logging
import os
import re
import signal
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

logger = logging.getLogger("wire.config")

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_DURATION_PATTERN = re.compile(r"^(\d+(?:\.\d+)?)\s*(s|m|h)$")


def _expand_env(value: str) -> str:
    """Expand ``${VAR}`` references to environment variable values."""

    def _replace(match: re.Match) -> str:
        var = match.group(1)
        val = os.environ.get(var)
        if val is None:
            raise ValueError(f"Environment variable ${{{var}}} is not set")
        return val

    return _ENV_PATTERN.sub(_replace, value)


def _parse_duration(value: str | int | float) -> float:
    """Parse a duration string like ``'30s'``, ``'1m'``, ``'2h'`` into seconds."""
    if isinstance(value, (int, float)):
        return float(value)
    m = _DURATION_PATTERN.match(value.strip())
    if not m:
        raise ValueError(f"Invalid duration: {value!r}")
    num = float(m.group(1))
    unit = m.group(2)
    if unit == "m":
        num *= 60
    elif unit == "h":
        num *= 3600
    return num


def _deep_expand(obj: Any) -> Any:
    """Recursively expand env vars in all string values."""
    if isinstance(obj, str):
        return _expand_env(obj)
    if isinstance(obj, dict):
        return {k: _deep_expand(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_deep_expand(v) for v in obj]
    return obj


# ── Data classes ──────────────────────────────────────────────────────────────


@dataclass
class NodeConfig:
    name: str = ""
    cert_dir: str = "./certs"


@dataclass
class ListenConfig:
    host: str = "0.0.0.0"
    port: int = 8765


@dataclass
class AuthConfig:
    secret: str = ""
    secret_file: str = ""

    def resolved_secret(self) -> str:
        if self.secret:
            return self.secret
        if self.secret_file:
            return Path(self.secret_file).read_text().strip()
        return ""


@dataclass
class HealthCheckConfig:
    interval: float = 10.0
    timeout: float = 5.0
    unhealthy_threshold: int = 3
    healthy_threshold: int = 1


@dataclass
class StaticRoute:
    prefix: str = ""
    upstream: str = ""


@dataclass
class ProxyConfig:
    enabled: bool = False
    host: str = "0.0.0.0"
    port: int = 8080
    read_timeout: float = 30.0
    static_routes: list[StaticRoute] = field(default_factory=list)
    health_check: HealthCheckConfig = field(default_factory=HealthCheckConfig)


@dataclass
class ServiceConfig:
    prefix: str = ""
    upstream: str = ""
    health_check: str = ""


@dataclass
class ReconnectConfig:
    enabled: bool = True
    initial_delay: float = 1.0
    max_delay: float = 30.0
    max_attempts: int = 0  # 0 = infinite


@dataclass
class ControllerConnectConfig:
    url: str = ""
    reconnect: ReconnectConfig = field(default_factory=ReconnectConfig)


@dataclass
class LogConfig:
    level: str = "info"
    format: str = "text"
    file: str = ""


@dataclass
class WireConfig:
    role: str = "controller"
    node: NodeConfig = field(default_factory=NodeConfig)
    listen: ListenConfig = field(default_factory=ListenConfig)
    auth: AuthConfig = field(default_factory=AuthConfig)
    proxy: ProxyConfig = field(default_factory=ProxyConfig)
    services: list[ServiceConfig] = field(default_factory=list)
    controller: ControllerConnectConfig = field(default_factory=ControllerConnectConfig)
    log: LogConfig = field(default_factory=LogConfig)


# ── Parsing ──────────────────────────────────────────────────────────────────


def _parse_health_check(raw: dict) -> HealthCheckConfig:
    cfg = HealthCheckConfig()
    if "interval" in raw:
        cfg.interval = _parse_duration(raw["interval"])
    if "timeout" in raw:
        cfg.timeout = _parse_duration(raw["timeout"])
    if "unhealthy_threshold" in raw:
        cfg.unhealthy_threshold = int(raw["unhealthy_threshold"])
    if "healthy_threshold" in raw:
        cfg.healthy_threshold = int(raw["healthy_threshold"])
    return cfg


def load_config(path: str) -> WireConfig:
    """Load and validate a Wire YAML configuration file."""
    with open(path) as f:
        raw = yaml.safe_load(f) or {}

    # Expand environment variables in all string values
    raw = _deep_expand(raw)

    cfg = WireConfig()
    cfg.role = raw.get("role", "controller")

    # Node
    if "node" in raw:
        n = raw["node"]
        cfg.node = NodeConfig(
            name=n.get("name", ""),
            cert_dir=n.get("cert_dir", "./certs"),
        )

    # Listen
    if "listen" in raw:
        li = raw["listen"]
        cfg.listen = ListenConfig(
            host=li.get("host", "0.0.0.0"),
            port=int(li.get("port", 8765)),
        )

    # Auth
    if "auth" in raw:
        a = raw["auth"]
        cfg.auth = AuthConfig(
            secret=a.get("secret", ""),
            secret_file=a.get("secret_file", ""),
        )

    # Proxy
    if "proxy" in raw:
        p = raw["proxy"]
        proxy = ProxyConfig(
            enabled=p.get("enabled", False),
            host=p.get("host", "0.0.0.0"),
            port=int(p.get("port", 8080)),
        )
        if "read_timeout" in p:
            proxy.read_timeout = _parse_duration(p["read_timeout"])
        if "static_routes" in p:
            proxy.static_routes = [
                StaticRoute(prefix=r["prefix"], upstream=r["upstream"])
                for r in p["static_routes"]
            ]
        if "health_check" in p:
            proxy.health_check = _parse_health_check(p["health_check"])
        cfg.proxy = proxy

    # Services (sub only)
    if "services" in raw:
        cfg.services = [
            ServiceConfig(
                prefix=s["prefix"],
                upstream=s["upstream"],
                health_check=s.get("health_check", ""),
            )
            for s in raw["services"]
        ]

    # Controller connection (sub only)
    if "controller" in raw:
        c = raw["controller"]
        cc = ControllerConnectConfig(url=c.get("url", ""))
        if "reconnect" in c:
            r = c["reconnect"]
            cc.reconnect = ReconnectConfig(
                enabled=r.get("enabled", True),
                max_attempts=int(r.get("max_attempts", 0)),
            )
            if "initial_delay" in r:
                cc.reconnect.initial_delay = _parse_duration(r["initial_delay"])
            if "max_delay" in r:
                cc.reconnect.max_delay = _parse_duration(r["max_delay"])
        cfg.controller = cc

    # Log
    if "log" in raw:
        lo = raw["log"]
        cfg.log = LogConfig(
            level=lo.get("level", "info"),
            format=lo.get("format", "text"),
            file=lo.get("file", ""),
        )

    return cfg


def validate_config(cfg: WireConfig) -> list[str]:
    """Validate a loaded config. Returns a list of error messages (empty = valid)."""
    errors: list[str] = []

    if cfg.role not in ("controller", "sub"):
        errors.append(f"Invalid role: {cfg.role!r} (must be 'controller' or 'sub')")

    if cfg.role == "controller":
        if not cfg.auth.resolved_secret():
            errors.append("Controller requires auth.secret or auth.secret_file")

    if cfg.role == "sub":
        if not cfg.controller.url:
            errors.append("Sub requires controller.url")
        if not cfg.auth.resolved_secret():
            errors.append("Sub requires auth.secret or auth.secret_file")

    return errors


# ── Hot-reload (SIGHUP) ─────────────────────────────────────────────────────

# Fields safe to reload without restart
_RELOADABLE_FIELDS = {"proxy.static_routes", "log.level", "proxy.health_check"}


def setup_sighup_reload(config_path: str, apply_fn):
    """Install a SIGHUP handler that reloads the config and calls *apply_fn*.

    ``apply_fn`` receives the new ``WireConfig`` and should apply
    reloadable fields (static routes, log level, health check intervals).
    """

    def _handler(signum, frame):
        logger.info("SIGHUP received, reloading config from %s", config_path)
        try:
            new_cfg = load_config(config_path)
            apply_fn(new_cfg)
            logger.info("Config reloaded successfully")
        except Exception as exc:
            logger.error("Config reload failed: %s", exc)

    signal.signal(signal.SIGHUP, _handler)
