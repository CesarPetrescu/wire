/**
 * Wire configuration — YAML-based config with environment variable expansion.
 *
 * Mirrors Python and Rust config modules: load_config, validate_config,
 * parse_duration, env-var expansion.
 */

import * as fs from "fs";
import * as yaml from "js-yaml";

// ── Interfaces ──────────────────────────────────────────────────────────────

export interface NodeConfig {
  name: string;
  cert_dir: string;
}

export interface ListenConfig {
  host: string;
  port: number;
}

export interface AuthConfig {
  secret: string;
  secret_file: string;
}

export interface HealthCheckConfig {
  interval: number;
  timeout: number;
  unhealthy_threshold: number;
  healthy_threshold: number;
}

export interface StaticRoute {
  prefix: string;
  upstream: string;
}

export interface ProxyConfig {
  enabled: boolean;
  host: string;
  port: number;
  read_timeout: number;
  static_routes: StaticRoute[];
  health_check: HealthCheckConfig;
}

export interface ServiceConfig {
  prefix: string;
  upstream: string;
  health_check: string;
}

export interface ReconnectConfig {
  enabled: boolean;
  initial_delay: number;
  max_delay: number;
  max_attempts: number;
}

export interface ControllerConnectConfig {
  url: string;
  reconnect: ReconnectConfig;
}

export interface LogConfig {
  level: string;
  format: string;
  file: string;
}

export interface WireConfig {
  role: string;
  node: NodeConfig;
  listen: ListenConfig;
  auth: AuthConfig;
  proxy: ProxyConfig;
  services: ServiceConfig[];
  controller: ControllerConnectConfig;
  log: LogConfig;
}

// ── Defaults ────────────────────────────────────────────────────────────────

function defaultHealthCheck(): HealthCheckConfig {
  return { interval: 10, timeout: 5, unhealthy_threshold: 3, healthy_threshold: 1 };
}

function defaultProxy(): ProxyConfig {
  return {
    enabled: false, host: "0.0.0.0", port: 8080,
    read_timeout: 30, static_routes: [], health_check: defaultHealthCheck(),
  };
}

function defaultReconnect(): ReconnectConfig {
  return { enabled: true, initial_delay: 1, max_delay: 30, max_attempts: 0 };
}

function defaultConfig(): WireConfig {
  return {
    role: "controller",
    node: { name: "", cert_dir: "./certs" },
    listen: { host: "0.0.0.0", port: 8765 },
    auth: { secret: "", secret_file: "" },
    proxy: defaultProxy(),
    services: [],
    controller: { url: "", reconnect: defaultReconnect() },
    log: { level: "info", format: "text", file: "" },
  };
}

// ── Duration parsing ────────────────────────────────────────────────────────

const DURATION_RE = /^(\d+(?:\.\d+)?)\s*(s|m|h)$/;

export function parseDuration(value: string | number): number {
  if (typeof value === "number") return value;
  const s = value.trim();
  const m = DURATION_RE.exec(s);
  if (!m) {
    // Try plain number
    const n = Number(s);
    if (isNaN(n)) throw new Error(`Invalid duration: '${value}'`);
    return n;
  }
  let num = parseFloat(m[1]);
  const unit = m[2];
  if (unit === "m") num *= 60;
  else if (unit === "h") num *= 3600;
  return num;
}

// ── Environment variable expansion ──────────────────────────────────────────

const ENV_RE = /\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g;

export function expandEnv(value: string): string {
  return value.replace(ENV_RE, (_, varName) => {
    const val = process.env[varName];
    if (val === undefined) {
      throw new Error(`Environment variable \${${varName}} is not set`);
    }
    return val;
  });
}

function deepExpand(obj: any): any {
  if (typeof obj === "string") return expandEnv(obj);
  if (Array.isArray(obj)) return obj.map(deepExpand);
  if (obj !== null && typeof obj === "object") {
    const out: any = {};
    for (const [k, v] of Object.entries(obj)) {
      out[k] = deepExpand(v);
    }
    return out;
  }
  return obj;
}

// ── Resolved secret ─────────────────────────────────────────────────────────

export function resolvedSecret(auth: AuthConfig): string {
  if (auth.secret) return auth.secret;
  if (auth.secret_file) {
    try {
      return fs.readFileSync(auth.secret_file, "utf-8").trim();
    } catch {
      return "";
    }
  }
  return "";
}

// ── Load & validate ─────────────────────────────────────────────────────────

export function loadConfig(path: string): WireConfig {
  const contents = fs.readFileSync(path, "utf-8");
  const raw: any = deepExpand(yaml.load(contents, { schema: yaml.DEFAULT_SCHEMA }) || {});

  const cfg = defaultConfig();
  cfg.role = raw.role ?? "controller";

  if (raw.node) {
    cfg.node.name = raw.node.name ?? "";
    cfg.node.cert_dir = raw.node.cert_dir ?? "./certs";
  }
  if (raw.listen) {
    cfg.listen.host = raw.listen.host ?? "0.0.0.0";
    cfg.listen.port = Number(raw.listen.port ?? 8765);
  }
  if (raw.auth) {
    cfg.auth.secret = raw.auth.secret ?? "";
    cfg.auth.secret_file = raw.auth.secret_file ?? "";
  }
  if (raw.proxy) {
    const p = raw.proxy;
    cfg.proxy.enabled = p.enabled ?? false;
    cfg.proxy.host = p.host ?? "0.0.0.0";
    cfg.proxy.port = Number(p.port ?? 8080);
    if (p.read_timeout) cfg.proxy.read_timeout = parseDuration(p.read_timeout);
    if (p.static_routes) {
      cfg.proxy.static_routes = p.static_routes.map((r: any) => ({
        prefix: r.prefix, upstream: r.upstream,
      }));
    }
    if (p.health_check) {
      const hc = p.health_check;
      if (hc.interval) cfg.proxy.health_check.interval = parseDuration(hc.interval);
      if (hc.timeout) cfg.proxy.health_check.timeout = parseDuration(hc.timeout);
      if (hc.unhealthy_threshold !== undefined) cfg.proxy.health_check.unhealthy_threshold = hc.unhealthy_threshold;
      if (hc.healthy_threshold !== undefined) cfg.proxy.health_check.healthy_threshold = hc.healthy_threshold;
    }
  }
  if (raw.services) {
    cfg.services = raw.services.map((s: any) => ({
      prefix: s.prefix, upstream: s.upstream, health_check: s.health_check ?? "",
    }));
  }
  if (raw.controller) {
    const c = raw.controller;
    cfg.controller.url = c.url ?? "";
    if (c.reconnect) {
      const r = c.reconnect;
      cfg.controller.reconnect.enabled = r.enabled ?? true;
      cfg.controller.reconnect.max_attempts = Number(r.max_attempts ?? 0);
      if (r.initial_delay) cfg.controller.reconnect.initial_delay = parseDuration(r.initial_delay);
      if (r.max_delay) cfg.controller.reconnect.max_delay = parseDuration(r.max_delay);
    }
  }
  if (raw.log) {
    cfg.log.level = raw.log.level ?? "info";
    cfg.log.format = raw.log.format ?? "text";
    cfg.log.file = raw.log.file ?? "";
  }

  return cfg;
}

export function validateConfig(cfg: WireConfig): string[] {
  const errors: string[] = [];

  if (cfg.role !== "controller" && cfg.role !== "sub") {
    errors.push(`Invalid role: '${cfg.role}' (must be 'controller' or 'sub')`);
  }

  if (cfg.role === "controller") {
    if (!resolvedSecret(cfg.auth)) {
      errors.push("Controller requires auth.secret or auth.secret_file");
    }
  }

  if (cfg.role === "sub") {
    if (!cfg.controller.url) {
      errors.push("Sub requires controller.url");
    }
    if (!resolvedSecret(cfg.auth)) {
      errors.push("Sub requires auth.secret or auth.secret_file");
    }
  }

  return errors;
}
