use std::io::Write;
use tempfile::NamedTempFile;
use wire_rs::config::{load_config, parse_duration_secs, validate_config, WireConfig};

// ---------------------------------------------------------------------------
// Duration parsing
// ---------------------------------------------------------------------------

#[test]
fn test_parse_seconds() {
    assert_eq!(parse_duration_secs("30s").unwrap(), 30.0);
}

#[test]
fn test_parse_minutes() {
    assert_eq!(parse_duration_secs("5m").unwrap(), 300.0);
}

#[test]
fn test_parse_hours() {
    assert_eq!(parse_duration_secs("2h").unwrap(), 7200.0);
}

#[test]
fn test_parse_numeric() {
    assert_eq!(parse_duration_secs("42").unwrap(), 42.0);
}

#[test]
fn test_parse_float_string() {
    assert_eq!(parse_duration_secs("1.5").unwrap(), 1.5);
}

#[test]
fn test_parse_invalid() {
    assert!(parse_duration_secs("abc").is_err());
}

// ---------------------------------------------------------------------------
// Env expansion
// ---------------------------------------------------------------------------

#[test]
fn test_env_expansion_in_config() {
    // SAFETY: test is single-threaded with respect to this env var name.
    unsafe {
        std::env::set_var("WIRE_TEST_SECRET", "s3cret_from_env");
    }

    let yaml = r#"
role: controller
auth:
  secret: "${WIRE_TEST_SECRET}"
"#;

    let mut f = NamedTempFile::new().expect("create temp file");
    f.write_all(yaml.as_bytes()).expect("write yaml");
    f.flush().expect("flush");

    let cfg = load_config(f.path().to_str().unwrap()).expect("load config");
    assert_eq!(cfg.auth.resolved_secret(), "s3cret_from_env");

    unsafe {
        std::env::remove_var("WIRE_TEST_SECRET");
    }
}

// ---------------------------------------------------------------------------
// Config loading
// ---------------------------------------------------------------------------

#[test]
fn test_load_controller_config() {
    let yaml = r#"
role: controller
node:
  name: ctrl-1
  cert_dir: /etc/wire/certs
listen:
  host: 127.0.0.1
  port: 9000
auth:
  secret: my_secret
proxy:
  enabled: true
  host: 0.0.0.0
  port: 9090
  read_timeout: "60s"
  static_routes:
    - prefix: /api
      upstream: http://localhost:3000
  health_check:
    interval: "15s"
    timeout: "3s"
    unhealthy_threshold: 5
    healthy_threshold: 2
log:
  level: debug
  format: json
  file: /var/log/wire.log
"#;

    let mut f = NamedTempFile::new().expect("create temp file");
    f.write_all(yaml.as_bytes()).expect("write yaml");
    f.flush().expect("flush");

    let cfg = load_config(f.path().to_str().unwrap()).expect("load config");

    assert_eq!(cfg.role, "controller");
    assert_eq!(cfg.node.name, "ctrl-1");
    assert_eq!(cfg.node.cert_dir, "/etc/wire/certs");
    assert_eq!(cfg.listen.host, "127.0.0.1");
    assert_eq!(cfg.listen.port, 9000);
    assert_eq!(cfg.auth.secret, "my_secret");
    assert_eq!(cfg.auth.resolved_secret(), "my_secret");

    assert!(cfg.proxy.enabled);
    assert_eq!(cfg.proxy.host, "0.0.0.0");
    assert_eq!(cfg.proxy.port, 9090);
    assert_eq!(cfg.proxy.read_timeout, "60s");
    assert_eq!(cfg.proxy.static_routes.len(), 1);
    assert_eq!(cfg.proxy.static_routes[0].prefix, "/api");
    assert_eq!(cfg.proxy.static_routes[0].upstream, "http://localhost:3000");
    assert_eq!(cfg.proxy.health_check.interval, "15s");
    assert_eq!(cfg.proxy.health_check.timeout, "3s");
    assert_eq!(cfg.proxy.health_check.unhealthy_threshold, 5);
    assert_eq!(cfg.proxy.health_check.healthy_threshold, 2);

    assert_eq!(cfg.log.level, "debug");
    assert_eq!(cfg.log.format, "json");
    assert_eq!(cfg.log.file, "/var/log/wire.log");
}

#[test]
fn test_load_sub_config() {
    let yaml = r#"
role: sub
node:
  name: worker-1
auth:
  secret: sub_secret
controller:
  url: wss://controller.example.com:8765
  reconnect:
    enabled: true
    initial_delay: "2s"
    max_delay: "60s"
    max_attempts: 10
services:
  - prefix: /svc-a
    upstream: http://localhost:4000
    health_check: /svc-a/health
  - prefix: /svc-b
    upstream: http://localhost:4001
"#;

    let mut f = NamedTempFile::new().expect("create temp file");
    f.write_all(yaml.as_bytes()).expect("write yaml");
    f.flush().expect("flush");

    let cfg = load_config(f.path().to_str().unwrap()).expect("load config");

    assert_eq!(cfg.role, "sub");
    assert_eq!(cfg.node.name, "worker-1");
    assert_eq!(cfg.auth.resolved_secret(), "sub_secret");
    assert_eq!(cfg.controller.url, "wss://controller.example.com:8765");
    assert!(cfg.controller.reconnect.enabled);
    assert_eq!(cfg.controller.reconnect.initial_delay, "2s");
    assert_eq!(cfg.controller.reconnect.max_delay, "60s");
    assert_eq!(cfg.controller.reconnect.max_attempts, 10);

    assert_eq!(cfg.services.len(), 2);
    assert_eq!(cfg.services[0].prefix, "/svc-a");
    assert_eq!(cfg.services[0].upstream, "http://localhost:4000");
    assert_eq!(cfg.services[0].health_check, "/svc-a/health");
    assert_eq!(cfg.services[1].prefix, "/svc-b");
    assert_eq!(cfg.services[1].upstream, "http://localhost:4001");
}

#[test]
fn test_defaults() {
    // Use a YAML that specifies empty sub-mappings so serde field-level
    // defaults kick in (e.g. default_cert_dir, default_host, etc.).
    let yaml = r#"
node: {}
listen: {}
proxy: {}
log: {}
"#;

    let mut f = NamedTempFile::new().expect("create temp file");
    f.write_all(yaml.as_bytes()).expect("write yaml");
    f.flush().expect("flush");

    let cfg = load_config(f.path().to_str().unwrap()).expect("load config");

    assert_eq!(cfg.role, "controller");
    assert_eq!(cfg.listen.host, "0.0.0.0");
    assert_eq!(cfg.listen.port, 8765);
    assert_eq!(cfg.node.cert_dir, "./certs");
    assert_eq!(cfg.proxy.port, 8080);
    assert!(!cfg.proxy.enabled);
    assert!(cfg.services.is_empty());
    assert_eq!(cfg.log.level, "info");
    assert_eq!(cfg.log.format, "text");
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

#[test]
fn test_valid_controller() {
    let mut cfg = WireConfig::default();
    cfg.role = "controller".to_string();
    cfg.auth.secret = "a_secret".to_string();

    let errors = validate_config(&cfg);
    assert!(errors.is_empty(), "expected no errors, got: {:?}", errors);
}

#[test]
fn test_controller_missing_secret() {
    let mut cfg = WireConfig::default();
    cfg.role = "controller".to_string();
    // auth.secret left empty

    let errors = validate_config(&cfg);
    assert!(!errors.is_empty(), "expected validation error for missing secret");
    assert!(
        errors.iter().any(|e| e.contains("secret")),
        "error should mention secret: {:?}",
        errors
    );
}

#[test]
fn test_valid_sub() {
    let mut cfg = WireConfig::default();
    cfg.role = "sub".to_string();
    cfg.controller.url = "wss://ctrl:8765".to_string();
    cfg.auth.secret = "sub_secret".to_string();

    let errors = validate_config(&cfg);
    assert!(errors.is_empty(), "expected no errors, got: {:?}", errors);
}

#[test]
fn test_sub_missing_url() {
    let mut cfg = WireConfig::default();
    cfg.role = "sub".to_string();
    cfg.auth.secret = "sub_secret".to_string();
    // controller.url left empty

    let errors = validate_config(&cfg);
    assert!(!errors.is_empty(), "expected validation error for missing url");
    assert!(
        errors.iter().any(|e| e.contains("url") || e.contains("controller")),
        "error should mention url/controller: {:?}",
        errors
    );
}

#[test]
fn test_invalid_role() {
    let mut cfg = WireConfig::default();
    cfg.role = "bogus".to_string();
    cfg.auth.secret = "some_secret".to_string();

    let errors = validate_config(&cfg);
    assert!(!errors.is_empty(), "expected validation error for invalid role");
    assert!(
        errors.iter().any(|e| e.contains("bogus")),
        "error should mention the invalid role: {:?}",
        errors
    );
}
