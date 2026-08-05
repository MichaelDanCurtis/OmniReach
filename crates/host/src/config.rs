use serde::Deserialize;
use std::path::PathBuf;
use thiserror::Error;

fn default_listen_addr() -> String {
    "127.0.0.1:7717".to_string()
}

fn default_scrollback() -> usize {
    2 * 1024 * 1024
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("could not parse config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("could not read config file: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccessConfig {
    pub team_domain: String,
    pub aud: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShellConfig {
    pub name: String,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub default: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    #[serde(default = "default_scrollback")]
    pub scrollback_bytes: usize,
    #[serde(default)]
    pub access: Option<AccessConfig>,
    /// Path to the Chrome/Chromium binary for the remote browser. `None` lets
    /// chromiumoxide search the platform's usual locations.
    #[serde(default)]
    pub chrome_path: Option<String>,
    /// Where the remote browser writes downloads. Defaults to
    /// `<config dir>/downloads`, which the file browser can then reach — the
    /// two features are only useful together.
    #[serde(default)]
    pub downloads_dir: Option<String>,
    #[serde(default)]
    pub shells: Vec<ShellConfig>,
}

impl Config {
    pub fn config_path() -> Option<PathBuf> {
        dirs::config_dir().map(|d| d.join("OmniReach").join("config.toml"))
    }

    pub fn load() -> Result<Config, ConfigError> {
        match Self::config_path() {
            Some(p) if p.exists() => Self::from_str(&std::fs::read_to_string(p)?),
            _ => Self::from_str(""),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Result<Config, ConfigError> {
        let mut c: Config = toml::from_str(s)?;
        if c.shells.is_empty() {
            c.shells.push(Self::platform_default_shell());
        }
        Ok(c)
    }

    fn platform_default_shell() -> ShellConfig {
        #[cfg(windows)]
        {
            ShellConfig {
                name: "pwsh".into(),
                command: "powershell.exe".into(),
                args: vec!["-NoLogo".into()],
                default: true,
            }
        }
        #[cfg(not(windows))]
        {
            ShellConfig {
                name: "sh".into(),
                command: std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".into()),
                args: vec![],
                default: true,
            }
        }
    }

    pub fn default_shell(&self) -> &ShellConfig {
        self.shells
            .iter()
            .find(|s| s.default)
            .or_else(|| self.shells.first())
            .expect("from_str guarantees at least one shell")
    }

    /// Resolved download directory for the remote browser.
    pub fn downloads_dir(&self) -> PathBuf {
        if let Some(d) = &self.downloads_dir {
            return PathBuf::from(d);
        }
        Self::config_path()
            .and_then(|p| p.parent().map(|d| d.join("downloads")))
            .unwrap_or_else(|| std::env::temp_dir().join("omnireach-downloads"))
    }

    pub fn shell_by_name(&self, name: &str) -> Option<&ShellConfig> {
        self.shells.iter().find(|s| s.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_full_config() {
        let toml = r#"
listen_addr = "127.0.0.1:7717"
scrollback_bytes = 2097152

[access]
team_domain = "myteam.cloudflareaccess.com"
aud = "abc123"

[[shells]]
name = "pwsh"
command = "pwsh.exe"
args = ["-NoLogo"]
default = true

[[shells]]
name = "wsl"
command = "wsl.exe"
args = []
"#;
        let c = Config::from_str(toml).unwrap();
        assert_eq!(c.listen_addr, "127.0.0.1:7717");
        assert_eq!(c.scrollback_bytes, 2_097_152);
        let a = c.access.as_ref().unwrap();
        assert_eq!(a.team_domain, "myteam.cloudflareaccess.com");
        assert_eq!(a.aud, "abc123");
        assert_eq!(c.shells.len(), 2);
        assert_eq!(c.default_shell().name, "pwsh");
        assert_eq!(c.shell_by_name("wsl").unwrap().command, "wsl.exe");
        assert!(c.shell_by_name("nope").is_none());
    }

    #[test]
    fn applies_defaults_for_an_empty_config() {
        let c = Config::from_str("").unwrap();
        assert_eq!(c.listen_addr, "127.0.0.1:7717");
        assert_eq!(c.scrollback_bytes, 2_097_152);
        assert!(
            c.access.is_none(),
            "access is optional so M1/M2 can run locally"
        );
        assert!(
            !c.shells.is_empty(),
            "must synthesize a platform default shell"
        );
    }

    #[test]
    fn first_shell_is_default_when_none_is_flagged() {
        let toml = r#"
[[shells]]
name = "a"
command = "a.exe"

[[shells]]
name = "b"
command = "b.exe"
"#;
        let c = Config::from_str(toml).unwrap();
        assert_eq!(c.default_shell().name, "a");
    }
}
