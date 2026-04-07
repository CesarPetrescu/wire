import { describe, it, beforeEach, afterEach } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
  parseDuration,
  expandEnv,
  loadConfig,
  validateConfig,
  resolvedSecret,
} from "../src/config";

// Helper to write temp YAML files
function writeTempYaml(content: string): string {
  const p = path.join(os.tmpdir(), `wire-test-${Date.now()}-${Math.random().toString(36).slice(2)}.yaml`);
  fs.writeFileSync(p, content, "utf-8");
  return p;
}

describe("parseDuration", () => {
  it("seconds", () => {
    assert.equal(parseDuration("30s"), 30);
  });
  it("minutes", () => {
    assert.equal(parseDuration("5m"), 300);
  });
  it("hours", () => {
    assert.equal(parseDuration("2h"), 7200);
  });
  it("numeric int", () => {
    assert.equal(parseDuration(42), 42);
  });
  it("float string", () => {
    assert.equal(parseDuration("1.5"), 1.5);
  });
  it("invalid throws", () => {
    assert.throws(() => parseDuration("abc"), /Invalid duration/);
  });
});

describe("expandEnv", () => {
  beforeEach(() => {
    process.env.WIRE_TEST_VAR = "hello";
  });
  afterEach(() => {
    delete process.env.WIRE_TEST_VAR;
  });

  it("single expansion", () => {
    assert.equal(expandEnv("${WIRE_TEST_VAR}"), "hello");
  });
  it("in context", () => {
    assert.equal(expandEnv("prefix_${WIRE_TEST_VAR}_suffix"), "prefix_hello_suffix");
  });
  it("no expansion", () => {
    assert.equal(expandEnv("no_vars_here"), "no_vars_here");
  });
  it("missing var throws", () => {
    assert.throws(() => expandEnv("${WIRE_NONEXISTENT_ZZZZZ}"), /not set/);
  });
});

describe("loadConfig", () => {
  it("controller config", () => {
    const p = writeTempYaml(`
role: controller
node:
  name: main-ctrl
  cert_dir: /etc/wire/certs
listen:
  host: 127.0.0.1
  port: 9000
auth:
  secret: s3cret
proxy:
  enabled: true
  host: 0.0.0.0
  port: 8080
  read_timeout: 1m
  static_routes:
    - prefix: /api
      upstream: http://backend:3000
  health_check:
    interval: 15s
    timeout: 3s
    unhealthy_threshold: 5
    healthy_threshold: 2
log:
  level: debug
  format: json
`);
    try {
      const cfg = loadConfig(p);
      assert.equal(cfg.role, "controller");
      assert.equal(cfg.node.name, "main-ctrl");
      assert.equal(cfg.node.cert_dir, "/etc/wire/certs");
      assert.equal(cfg.listen.host, "127.0.0.1");
      assert.equal(cfg.listen.port, 9000);
      assert.equal(cfg.auth.secret, "s3cret");
      assert.equal(cfg.proxy.enabled, true);
      assert.equal(cfg.proxy.port, 8080);
      assert.equal(cfg.proxy.read_timeout, 60);
      assert.equal(cfg.proxy.static_routes.length, 1);
      assert.equal(cfg.proxy.static_routes[0].prefix, "/api");
      assert.equal(cfg.proxy.health_check.interval, 15);
      assert.equal(cfg.proxy.health_check.timeout, 3);
      assert.equal(cfg.proxy.health_check.unhealthy_threshold, 5);
      assert.equal(cfg.proxy.health_check.healthy_threshold, 2);
      assert.equal(cfg.log.level, "debug");
      assert.equal(cfg.log.format, "json");
    } finally {
      fs.unlinkSync(p);
    }
  });

  it("sub config with services", () => {
    const p = writeTempYaml(`
role: sub
auth:
  secret: my-secret
controller:
  url: wss://ctrl.example.com:8765
  reconnect:
    enabled: true
    initial_delay: 2s
    max_delay: 1m
    max_attempts: 10
services:
  - prefix: /api
    upstream: http://localhost:3000
    health_check: /health
  - prefix: /admin
    upstream: http://localhost:4000
`);
    try {
      const cfg = loadConfig(p);
      assert.equal(cfg.role, "sub");
      assert.equal(cfg.controller.url, "wss://ctrl.example.com:8765");
      assert.equal(cfg.controller.reconnect.enabled, true);
      assert.equal(cfg.controller.reconnect.initial_delay, 2);
      assert.equal(cfg.controller.reconnect.max_delay, 60);
      assert.equal(cfg.controller.reconnect.max_attempts, 10);
      assert.equal(cfg.services.length, 2);
      assert.equal(cfg.services[0].prefix, "/api");
      assert.equal(cfg.services[0].health_check, "/health");
      assert.equal(cfg.services[1].prefix, "/admin");
    } finally {
      fs.unlinkSync(p);
    }
  });

  it("env expansion in config", () => {
    process.env.WIRE_SECRET_TEST = "expanded-secret";
    const p = writeTempYaml(`
role: controller
auth:
  secret: "\${WIRE_SECRET_TEST}"
`);
    try {
      const cfg = loadConfig(p);
      assert.equal(cfg.auth.secret, "expanded-secret");
    } finally {
      delete process.env.WIRE_SECRET_TEST;
      fs.unlinkSync(p);
    }
  });

  it("defaults", () => {
    const p = writeTempYaml("{}");
    try {
      const cfg = loadConfig(p);
      assert.equal(cfg.role, "controller");
      assert.equal(cfg.listen.host, "0.0.0.0");
      assert.equal(cfg.listen.port, 8765);
      assert.equal(cfg.node.cert_dir, "./certs");
      assert.equal(cfg.proxy.enabled, false);
      assert.equal(cfg.proxy.port, 8080);
      assert.equal(cfg.log.level, "info");
      assert.equal(cfg.log.format, "text");
    } finally {
      fs.unlinkSync(p);
    }
  });
});

describe("validateConfig", () => {
  it("valid controller", () => {
    const p = writeTempYaml(`
role: controller
auth:
  secret: s3cret
`);
    try {
      const cfg = loadConfig(p);
      const errors = validateConfig(cfg);
      assert.equal(errors.length, 0);
    } finally {
      fs.unlinkSync(p);
    }
  });

  it("controller missing secret", () => {
    const p = writeTempYaml(`
role: controller
`);
    try {
      const cfg = loadConfig(p);
      const errors = validateConfig(cfg);
      assert.ok(errors.length > 0);
      assert.ok(errors[0].includes("secret"));
    } finally {
      fs.unlinkSync(p);
    }
  });

  it("valid sub", () => {
    const p = writeTempYaml(`
role: sub
auth:
  secret: s3cret
controller:
  url: wss://ctrl:8765
`);
    try {
      const cfg = loadConfig(p);
      const errors = validateConfig(cfg);
      assert.equal(errors.length, 0);
    } finally {
      fs.unlinkSync(p);
    }
  });

  it("sub missing url", () => {
    const p = writeTempYaml(`
role: sub
auth:
  secret: s3cret
`);
    try {
      const cfg = loadConfig(p);
      const errors = validateConfig(cfg);
      assert.ok(errors.length > 0);
      assert.ok(errors[0].includes("url"));
    } finally {
      fs.unlinkSync(p);
    }
  });

  it("invalid role", () => {
    const p = writeTempYaml(`
role: bogus
auth:
  secret: s3cret
`);
    try {
      const cfg = loadConfig(p);
      const errors = validateConfig(cfg);
      assert.ok(errors.length > 0);
      assert.ok(errors[0].includes("Invalid role"));
    } finally {
      fs.unlinkSync(p);
    }
  });
});

describe("resolvedSecret", () => {
  it("inline secret", () => {
    assert.equal(resolvedSecret({ secret: "inline", secret_file: "" }), "inline");
  });

  it("secret from file", () => {
    const p = path.join(os.tmpdir(), `wire-secret-${Date.now()}`);
    fs.writeFileSync(p, "  file-secret  \n", "utf-8");
    try {
      assert.equal(resolvedSecret({ secret: "", secret_file: p }), "file-secret");
    } finally {
      fs.unlinkSync(p);
    }
  });

  it("empty when neither set", () => {
    assert.equal(resolvedSecret({ secret: "", secret_file: "" }), "");
  });
});
