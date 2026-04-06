//! Wire configuration — YAML-based config with environment variable expansion.

use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize)]
pub struct WireConfig {
    #[serde(default = "default_role")]
    pub role: String,
    #[serde(default)]
    pub node: NodeConfig,
    #[serde(default)]
    pub listen: ListenConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub proxy: ProxyConfig,
    #[serde(default)]
    pub services: Vec<ServiceConfig>,
    #[serde(default)]
    pub controller: ControllerConnectConfig,
    #[serde(default)]
    pub log: LogConfig,
}

impl Default for WireConfig {
    fn default() -> Self {
        Self {
            role: default_role(),
            node: NodeConfig::default(),
            listen: ListenConfig::default(),
            auth: AuthConfig::default(),
            proxy: ProxyConfig::default(),
            services: Vec::new(),
            controller: ControllerConnectConfig::default(),
            log: LogConfig::default(),
        }
    }
}

fn default_role() -> String {
    "controller".to_string()
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct NodeConfig {
    #[serde(default)]
    pub name: String,
    #[serde(default = "default_cert_dir")]
    pub cert_dir: String,
}

fn default_cert_dir() -> String {
    "./certs".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListenConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

impl Default for ListenConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
        }
    }
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}
fn default_port() -> u16 {
    8765
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AuthConfig {
    #[serde(default)]
    pub secret: String,
    #[serde(default)]
    pub secret_file: String,
}

impl AuthConfig {
    pub fn resolved_secret(&self) -> String {
        if !self.secret.is_empty() {
            return self.secret.clone();
        }
        if !self.secret_file.is_empty() {
            if let Ok(contents) = fs::read_to_string(&self.secret_file) {
                return contents.trim().to_string();
            }
        }
        String::new()
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct HealthCheckConfig {
    #[serde(default = "default_interval")]
    pub interval: String,
    #[serde(default = "default_hc_timeout")]
    pub timeout: String,
    #[serde(default = "default_unhealthy_threshold")]
    pub unhealthy_threshold: u32,
    #[serde(default = "default_healthy_threshold")]
    pub healthy_threshold: u32,
}

fn default_interval() -> String {
    "10s".to_string()
}
fn default_hc_timeout() -> String {
    "5s".to_string()
}
fn default_unhealthy_threshold() -> u32 {
    3
}
fn default_healthy_threshold() -> u32 {
    1
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct StaticRoute {
    pub prefix: String,
    pub upstream: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ProxyConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_proxy_port")]
    pub port: u16,
    #[serde(default = "default_read_timeout")]
    pub read_timeout: String,
    #[serde(default)]
    pub static_routes: Vec<StaticRoute>,
    #[serde(default)]
    pub health_check: HealthCheckConfig,
}

fn default_proxy_port() -> u16 {
    8080
}
fn default_read_timeout() -> String {
    "30s".to_string()
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ServiceConfig {
    pub prefix: String,
    pub upstream: String,
    #[serde(default)]
    pub health_check: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ReconnectConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_initial_delay")]
    pub initial_delay: String,
    #[serde(default = "default_max_delay")]
    pub max_delay: String,
    #[serde(default)]
    pub max_attempts: u32,
}

fn default_true() -> bool {
    true
}
fn default_initial_delay() -> String {
    "1s".to_string()
}
fn default_max_delay() -> String {
    "30s".to_string()
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ControllerConnectConfig {
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub reconnect: ReconnectConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct LogConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default = "default_log_format")]
    pub format: String,
    #[serde(default)]
    pub file: String,
}

fn default_log_level() -> String {
    "info".to_string()
}
fn default_log_format() -> String {
    "text".to_string()
}

/// Parse a duration string like "30s", "1m", "2h" into seconds.
pub fn parse_duration_secs(s: &str) -> Result<f64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("Empty duration string".into());
    }
    let (num_str, multiplier) = if s.ends_with('s') {
        (&s[..s.len() - 1], 1.0)
    } else if s.ends_with('m') {
        (&s[..s.len() - 1], 60.0)
    } else if s.ends_with('h') {
        (&s[..s.len() - 1], 3600.0)
    } else {
        // Try parsing as plain number (seconds)
        return s.parse::<f64>().map_err(|e| format!("Invalid duration '{}': {}", s, e));
    };
    let num: f64 = num_str
        .parse()
        .map_err(|e| format!("Invalid duration '{}': {}", s, e))?;
    Ok(num * multiplier)
}

/// Expand `${VAR}` in a string using environment variables.
fn expand_env(s: &str) -> String {
    let mut result = s.to_string();
    // Simple regex-free approach
    loop {
        if let Some(start) = result.find("${") {
            if let Some(end) = result[start..].find('}') {
                let var_name = &result[start + 2..start + end];
                let val = std::env::var(var_name).unwrap_or_default();
                result = format!("{}{}{}", &result[..start], val, &result[start + end + 1..]);
                continue;
            }
        }
        break;
    }
    result
}

/// Recursively expand env vars in a serde_yaml::Value.
fn expand_env_value(v: serde_yaml::Value) -> serde_yaml::Value {
    match v {
        serde_yaml::Value::String(s) => serde_yaml::Value::String(expand_env(&s)),
        serde_yaml::Value::Mapping(m) => {
            let mut new_map = serde_yaml::Mapping::new();
            for (k, val) in m {
                new_map.insert(k, expand_env_value(val));
            }
            serde_yaml::Value::Mapping(new_map)
        }
        serde_yaml::Value::Sequence(seq) => {
            serde_yaml::Value::Sequence(seq.into_iter().map(expand_env_value).collect())
        }
        other => other,
    }
}

/// Load a Wire YAML configuration file.
pub fn load_config(path: &str) -> Result<WireConfig, Box<dyn std::error::Error + Send + Sync>> {
    let contents = fs::read_to_string(path)?;
    let raw: serde_yaml::Value = serde_yaml::from_str(&contents)?;
    let expanded = expand_env_value(raw);
    let cfg: WireConfig = serde_yaml::from_value(expanded)?;
    Ok(cfg)
}

/// Validate a loaded config. Returns a list of error messages.
pub fn validate_config(cfg: &WireConfig) -> Vec<String> {
    let mut errors = Vec::new();

    if cfg.role != "controller" && cfg.role != "sub" {
        errors.push(format!(
            "Invalid role: '{}' (must be 'controller' or 'sub')",
            cfg.role
        ));
    }

    if cfg.role == "controller" && cfg.auth.resolved_secret().is_empty() {
        errors.push("Controller requires auth.secret or auth.secret_file".into());
    }

    if cfg.role == "sub" {
        if cfg.controller.url.is_empty() {
            errors.push("Sub requires controller.url".into());
        }
        if cfg.auth.resolved_secret().is_empty() {
            errors.push("Sub requires auth.secret or auth.secret_file".into());
        }
    }

    errors
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration_secs("30s").unwrap(), 30.0);
        assert_eq!(parse_duration_secs("1m").unwrap(), 60.0);
        assert_eq!(parse_duration_secs("2h").unwrap(), 7200.0);
        assert!(parse_duration_secs("invalid").is_err());
    }

    #[test]
    fn test_expand_env() {
        std::env::set_var("WIRE_TEST_VAR", "hello");
        assert_eq!(expand_env("${WIRE_TEST_VAR}"), "hello");
        assert_eq!(expand_env("prefix_${WIRE_TEST_VAR}_suffix"), "prefix_hello_suffix");
        assert_eq!(expand_env("no_vars_here"), "no_vars_here");
        std::env::remove_var("WIRE_TEST_VAR");
    }

    #[test]
    fn test_default_config() {
        let cfg = WireConfig::default();
        assert_eq!(cfg.role, "controller");
        assert_eq!(cfg.listen.port, 8765);
    }
}
