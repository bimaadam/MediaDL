//! URL validation with SSRF checks. Literal addresses are classified locally.
//! Names are resolved once; a request is refused when any answer is non-public,
//! and the first public answer is pinned onto the HTTP client so a later lookup
//! cannot switch the connection to an internal address.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use async_trait::async_trait;
use reqwest::redirect::Policy;
use url::Url;

use crate::application::{AppError, UrlGuard};
use crate::domain::{
    DomainError, host_is_blocked_name, ip_is_public, is_opaque_facebook_share, is_shortener,
};

const MAX_URL_LEN: usize = 2048;
const MAX_HOPS: u8 = 5;
const BROWSER_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

pub struct SafeHttp {
    client: reqwest::Client,
}

impl SafeHttp {
    pub fn new() -> Result<Self, AppError> {
        let client = reqwest::Client::builder()
            .redirect(Policy::none())
            .timeout(Duration::from_secs(20))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|error| AppError::Dependency(format!("http client: {error}")))?;
        Ok(Self { client })
    }

    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    pub async fn validate(&self, raw: &str) -> Result<Url, AppError> {
        if raw.len() > MAX_URL_LEN {
            return Err(AppError::Validation("url is too long".into()));
        }
        let mut url = parse_public_url(raw)?;
        self.ensure_public(&url).await?;
        if expands(&url) {
            url = self.follow_expandable(url).await?;
            self.ensure_public(&url).await?;
        }
        Ok(url)
    }

    /// Pin every connection for `url` to the addresses checked by [`Self::ensure_public`].
    ///
    /// reqwest 0.12 only accepts DNS overrides on the client builder, so each
    /// pinned request gets a client whose resolver cannot return a different address.
    pub async fn pinned_get(&self, url: &Url) -> Result<reqwest::RequestBuilder, AppError> {
        let addresses = self.ensure_public(url).await?;
        let host = host_of(url)?.to_owned();
        let port = url.port_or_known_default().ok_or(DomainError::InvalidUrl)?;
        let sockets: Vec<SocketAddr> =
            addresses.into_iter().map(|address| SocketAddr::new(address, port)).collect();
        let client = reqwest::Client::builder()
            .redirect(Policy::none())
            .timeout(Duration::from_secs(20))
            .connect_timeout(Duration::from_secs(5))
            .resolve_to_addrs(&host, &sockets)
            .build()
            .map_err(|error| AppError::Dependency(format!("http client: {error}")))?;
        // Facebook answers a bare client with /unsupportedbrowser. A browser
        // User-Agent only asks for the public redirect, without cookies or login.
        Ok(client
            .get(url.clone())
            .header(reqwest::header::USER_AGENT, BROWSER_USER_AGENT)
            .header(
                reqwest::header::ACCEPT,
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            )
            .header(reqwest::header::ACCEPT_LANGUAGE, "en-US,en;q=0.9")
            .header("sec-fetch-dest", "document")
            .header("sec-fetch-mode", "navigate")
            .header("sec-fetch-site", "none")
            .header("upgrade-insecure-requests", "1"))
    }

    async fn follow_expandable(&self, mut current: Url) -> Result<Url, AppError> {
        for _ in 0..MAX_HOPS {
            if !expands(&current) {
                return Ok(current);
            }
            let response = self.pinned_get(&current).await?.send().await.map_err(|error| {
                AppError::Dependency(format!("shortener request failed: {error}"))
            })?;
            let status = response.status();
            if !status.is_redirection() {
                return Ok(current);
            }
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| {
                    AppError::Validation("shortener redirect is missing a location".into())
                })?;
            current = current.join(location).map_err(|_| DomainError::InvalidUrl)?;
            parse_scheme(&current)?;
            self.ensure_public(&current).await?;
        }
        Err(AppError::Validation("too many shortener redirects".into()))
    }

    async fn ensure_public(&self, url: &Url) -> Result<Vec<IpAddr>, AppError> {
        let host = host_of(url)?;
        if host_is_blocked_name(host) {
            return Err(DomainError::BlockedDestination.into());
        }
        if let Ok(ip) = host.parse::<IpAddr>() {
            if !ip_is_public(ip) {
                return Err(DomainError::BlockedDestination.into());
            }
            return Ok(vec![ip]);
        }
        let port = url.port_or_known_default().ok_or(DomainError::InvalidUrl)?;
        let mut addresses = Vec::new();
        let looked_up = tokio::net::lookup_host((host, port))
            .await
            .map_err(|_| DomainError::BlockedDestination)?;
        for socket in looked_up {
            let ip = socket.ip();
            if !ip_is_public(ip) {
                return Err(DomainError::BlockedDestination.into());
            }
            if !addresses.contains(&ip) {
                addresses.push(ip);
            }
        }
        if addresses.is_empty() {
            return Err(DomainError::BlockedDestination.into());
        }
        Ok(addresses)
    }
}

fn parse_public_url(raw: &str) -> Result<Url, AppError> {
    let url = Url::parse(raw).map_err(|_| DomainError::InvalidUrl)?;
    parse_scheme(&url)?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err(AppError::Validation("urls must not contain credentials".into()));
    }
    Ok(url)
}

fn parse_scheme(url: &Url) -> Result<(), AppError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(DomainError::InvalidUrl.into());
    }
    Ok(())
}

fn host_of(url: &Url) -> Result<&str, AppError> {
    url.host_str().map(|host| host.trim_end_matches('.')).ok_or(DomainError::InvalidUrl.into())
}

fn expands(url: &Url) -> bool {
    host_of(url).is_ok_and(|host| is_shortener(host)) || is_opaque_facebook_share(url)
}

#[async_trait]
impl UrlGuard for SafeHttp {
    async fn validate(&self, raw: &str) -> Result<Url, AppError> {
        SafeHttp::validate(self, raw).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "calls Facebook; a public share link must resolve to the post id"]
    async fn facebook_share_redirects_to_a_post() {
        let guard = SafeHttp::new().expect("client");
        let url =
            guard.validate("https://www.facebook.com/share/19ji6Wnxkf/").await.expect("share link");
        let identified = crate::domain::identify_url(&url).expect("identify").expect("platform");
        assert_eq!(identified.content_id.as_str(), "1186536767364694", "{url}");
    }

    #[tokio::test]
    async fn blocks_metadata_and_loopback_literals() {
        let guard = SafeHttp::new().expect("client");
        for raw in [
            "http://169.254.169.254/latest/meta-data",
            "http://127.0.0.1/file.mp4",
            "http://10.1.2.3/a.mp4",
            "http://localhost/a.mp4",
            "http://metadata.google.internal/a.mp4",
            "file:///etc/passwd",
            "http://user:pass@example.com/a.mp4",
        ] {
            assert!(guard.validate(raw).await.is_err(), "{raw} should be rejected");
        }
    }
}
