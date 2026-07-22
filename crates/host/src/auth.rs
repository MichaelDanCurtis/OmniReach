use crate::config::AccessConfig;
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use parking_lot::RwLock;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use thiserror::Error;

const MIN_REFRESH_INTERVAL: Duration = Duration::from_secs(300);

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("no Access token presented")]
    Missing,
    #[error("Access token is malformed")]
    Malformed,
    #[error("Access token signed by an unknown key")]
    UnknownKid,
    #[error("Access token rejected: {0}")]
    Invalid(String),
    #[error("could not fetch JWKS: {0}")]
    JwksFetch(String),
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccessClaims {
    pub sub: String,
    #[serde(default)]
    pub email: Option<String>,
    pub exp: u64,
}

pub struct Authenticator {
    cfg: AccessConfig,
    keys: RwLock<HashMap<String, DecodingKey>>,
    last_refresh: RwLock<Option<Instant>>,
    validation: Validation,
}

impl Authenticator {
    pub fn new(cfg: AccessConfig) -> Self {
        let mut validation = Validation::new(Algorithm::RS256);
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.validate_aud = true;
        validation.aud = Some(HashSet::from([cfg.aud.clone()]));
        validation.iss = Some(HashSet::from([format!("https://{}", cfg.team_domain)]));
        validation.algorithms = vec![Algorithm::RS256];

        Self {
            cfg,
            keys: RwLock::new(HashMap::new()),
            last_refresh: RwLock::new(None),
            validation,
        }
    }

    pub fn validation(&self) -> &Validation {
        &self.validation
    }

    #[cfg(test)]
    pub fn seed_keys_for_test(&self, keys: HashMap<String, DecodingKey>) {
        *self.keys.write() = keys;
        *self.last_refresh.write() = Some(Instant::now());
    }

    pub fn jwks_url(&self) -> String {
        format!("https://{}/cdn-cgi/access/certs", self.cfg.team_domain)
    }

    pub async fn refresh_keys(&self) -> Result<(), AuthError> {
        let body: JwkSet = reqwest::get(self.jwks_url())
            .await
            .map_err(|e| AuthError::JwksFetch(e.to_string()))?
            .json()
            .await
            .map_err(|e| AuthError::JwksFetch(e.to_string()))?;

        let mut map = HashMap::new();
        for jwk in &body.keys {
            if let Some(kid) = jwk.common.key_id.clone() {
                if let Ok(k) = DecodingKey::from_jwk(jwk) {
                    map.insert(kid, k);
                }
            }
        }
        *self.keys.write() = map;
        *self.last_refresh.write() = Some(Instant::now());
        Ok(())
    }

    fn should_refresh(&self) -> bool {
        match *self.last_refresh.read() {
            None => true,
            Some(t) => t.elapsed() >= MIN_REFRESH_INTERVAL,
        }
    }

    /// Validate synchronously against cached keys. Callers must have
    /// populated the cache via `ensure_and_validate` first.
    pub fn validate(&self, token: &str) -> Result<AccessClaims, AuthError> {
        if token.is_empty() {
            return Err(AuthError::Missing);
        }
        let header = decode_header(token).map_err(|_| AuthError::Malformed)?;
        let kid = header.kid.ok_or(AuthError::Malformed)?;

        let keys = self.keys.read();
        let key = keys.get(&kid).ok_or(AuthError::UnknownKid)?;

        decode::<AccessClaims>(token, key, &self.validation)
            .map(|d| d.claims)
            .map_err(|e| AuthError::Invalid(e.to_string()))
    }

    /// Refresh keys if stale, then validate. Retries once on `UnknownKid`
    /// because Cloudflare rotates signing keys without warning.
    pub async fn ensure_and_validate(&self, token: &str) -> Result<AccessClaims, AuthError> {
        if self.should_refresh() {
            self.refresh_keys().await?;
        }
        match self.validate(token) {
            Err(AuthError::UnknownKid) => {
                self.refresh_keys().await?;
                self.validate(token)
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> AccessConfig {
        AccessConfig {
            team_domain: "test.cloudflareaccess.com".into(),
            aud: "test-aud".into(),
        }
    }

    #[test]
    fn rejects_an_empty_token() {
        let a = Authenticator::new(cfg());
        assert!(matches!(a.validate(""), Err(AuthError::Missing)));
    }

    #[test]
    fn rejects_a_malformed_token() {
        let a = Authenticator::new(cfg());
        assert!(matches!(a.validate("not-a-jwt"), Err(AuthError::Malformed)));
    }

    #[test]
    fn rejects_a_token_with_an_unknown_kid() {
        // Three base64url segments with a header naming a kid we have no key for.
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};
        let header = B64.encode(br#"{"alg":"RS256","kid":"nope"}"#);
        let payload = B64.encode(br#"{"sub":"x","aud":"test-aud","exp":9999999999}"#);
        let token = format!("{header}.{payload}.c2ln");

        let a = Authenticator::new(cfg());
        a.seed_keys_for_test(std::collections::HashMap::new());
        assert!(matches!(a.validate(&token), Err(AuthError::UnknownKid)));
    }

    #[test]
    fn validation_is_configured_to_check_aud_iss_and_exp() {
        let a = Authenticator::new(cfg());
        let v = a.validation();
        assert!(v.validate_exp, "must reject expired tokens");
        assert!(v.validate_aud, "must reject tokens for another application");
        assert_eq!(
            v.iss
                .as_ref()
                .expect("issuer must be pinned")
                .iter()
                .next()
                .unwrap(),
            "https://test.cloudflareaccess.com"
        );
        assert!(v.aud.as_ref().unwrap().contains("test-aud"));
        assert_eq!(v.algorithms, vec![jsonwebtoken::Algorithm::RS256]);
    }

    #[test]
    fn crypto_provider_is_installed_so_decode_does_not_panic() {
        // With no crypto backend feature, jsonwebtoken panics here rather
        // than returning Err. A clean Err proves the backend is present.
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};
        let header = B64.encode(br#"{"alg":"RS256","kid":"k1"}"#);
        let payload = B64.encode(br#"{"sub":"x","aud":"test-aud","exp":9999999999}"#);
        let token = format!("{header}.{payload}.aW52YWxpZA");

        let a = Authenticator::new(cfg());
        let key = DecodingKey::from_rsa_components(
            "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
            "AQAB",
        )
        .unwrap();
        a.seed_keys_for_test(HashMap::from([("k1".to_string(), key)]));

        // Must return Err (bad signature), not panic.
        let result = a.validate(&token);
        assert!(
            matches!(result, Err(AuthError::Invalid(_))),
            "got {result:?}"
        );
    }
}
