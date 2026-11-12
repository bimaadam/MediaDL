use url::Url;

use super::error::DomainError;
use super::identity::{ContentId, sha256_hex};
use super::platform::PlatformKind;

const TRACKING_PARAMS: &[&str] = &[
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "utm_id",
    "fbclid",
    "gclid",
    "igshid",
    "igsh",
    "si",
    "feature",
    "pp",
    "ref",
    "ref_src",
    "spm",
    "share_id",
    "is_from_webapp",
    "sender_device",
    "_r",
    "t",
    "start",
    "time_continue",
    "ab_channel",
    "context",
    "s",
];

const DIRECT_EXTENSIONS: &[&str] = &[
    "mp4", "mkv", "webm", "mov", "m4v", "mp3", "m4a", "aac", "wav", "ogg", "opus", "jpg", "jpeg",
    "png", "gif", "webp",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentifiedContent {
    pub platform: PlatformKind,
    pub content_id: ContentId,
    pub canonical_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalUrl {
    pub url: Url,
}

/// Identify a platform from an already validated URL. Shorteners and
/// `facebook.com/share/...` links return `None` so the guard can expand them.
pub fn identify_url(url: &Url) -> Result<Option<IdentifiedContent>, DomainError> {
    let host = normalized_host(url).ok_or(DomainError::InvalidUrl)?;
    if let Some(found) = identify_host(url, &host) {
        return Ok(Some(found?));
    }
    if is_shortener(&host) || is_opaque_facebook_share(url) {
        return Ok(None);
    }
    if is_direct_media(url) {
        let canonical = strip_tracking(url);
        let id = ContentId::new(sha256_hex(canonical.as_bytes()))?;
        return Ok(Some(IdentifiedContent {
            platform: PlatformKind::Direct,
            content_id: id,
            canonical_url: canonical,
        }));
    }
    Err(DomainError::UnsupportedPlatform)
}

pub fn is_shortener(host: &str) -> bool {
    matches!(host, "vm.tiktok.com" | "vt.tiktok.com" | "fb.watch" | "t.co" | "redd.it")
}

/// Share links have no content id until Facebook redirects them.
/// `/share/r/`, `/share/v/`, and `/share/p/` are tokens, not reel or post ids.
pub fn is_opaque_facebook_share(url: &Url) -> bool {
    let Some(host) = normalized_host(url) else {
        return false;
    };
    if !(host == "facebook.com" || host.ends_with(".facebook.com")) {
        return false;
    }
    match segments(url).as_slice() {
        ["share", id] if is_storage_id(id) => true,
        ["share", kind, id] if matches!(*kind, "r" | "v" | "p") && is_storage_id(id) => true,
        _ => false,
    }
}

fn identify_host(url: &Url, host: &str) -> Option<Result<IdentifiedContent, DomainError>> {
    let identified = if is_youtube_host(host) {
        youtube_id(url).map(|id| (PlatformKind::YouTube, id))
    } else if host == "tiktok.com" || host.ends_with(".tiktok.com") {
        tiktok_id(url).map(|id| (PlatformKind::TikTok, id))
    } else if host == "facebook.com" || host.ends_with(".facebook.com") {
        facebook_id(url).map(|id| (PlatformKind::Facebook, id))
    } else if host == "instagram.com" || host.ends_with(".instagram.com") {
        instagram_id(url).map(|id| (PlatformKind::Instagram, id))
    } else if matches!(host, "x.com" | "twitter.com" | "mobile.twitter.com")
        || host.ends_with(".x.com")
        || host.ends_with(".twitter.com")
    {
        x_id(url).map(|id| (PlatformKind::X, id))
    } else if host == "reddit.com"
        || host.ends_with(".reddit.com")
        || host == "v.redd.it"
        || host == "redd.it"
    {
        reddit_id(url).map(|id| (PlatformKind::Reddit, id))
    } else {
        None
    };
    identified.map(|(platform, id)| {
        let content_id = ContentId::new(id)?;
        Ok(IdentifiedContent { platform, content_id, canonical_url: strip_tracking(url) })
    })
}

fn is_youtube_host(host: &str) -> bool {
    matches!(
        host,
        "youtube.com" | "m.youtube.com" | "music.youtube.com" | "youtu.be" | "youtube-nocookie.com"
    ) || host.ends_with(".youtube.com")
}

fn youtube_id(url: &Url) -> Option<String> {
    let host = normalized_host(url)?;
    if host == "youtu.be" {
        return first_segment(url).filter(|id| is_youtube_id(id)).map(str::to_owned);
    }
    if let Some(id) = query_value(url, "v")
        && is_youtube_id(&id)
    {
        return Some(id);
    }
    let segs = segments(url);
    match segs.as_slice() {
        ["shorts", id] | ["embed", id] | ["live", id] | ["v", id] if is_youtube_id(id) => {
            Some((*id).to_owned())
        }
        _ => None,
    }
}

fn is_youtube_id(value: &str) -> bool {
    value.len() == 11 && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn tiktok_id(url: &Url) -> Option<String> {
    let segs = segments(url);
    segs.windows(2).find_map(|window| {
        if window[0] == "video"
            && window[1].chars().all(|c| c.is_ascii_digit())
            && window[1].len() >= 6
        {
            Some(window[1].to_owned())
        } else {
            None
        }
    })
}

fn facebook_id(url: &Url) -> Option<String> {
    for name in ["v", "story_fbid", "fbid"] {
        if let Some(id) = query_value(url, name)
            && is_storage_id(&id)
        {
            return Some(id);
        }
    }
    let segs = segments(url);
    match segs.as_slice() {
        ["reel", id] | ["videos", id] | ["watch", id] if is_storage_id(id) => {
            Some((*id).to_owned())
        }
        [_, "videos", id] | [_, "posts", id] | [_, "permalink", id] if is_storage_id(id) => {
            Some((*id).to_owned())
        }
        ["groups", _, "posts", id] | ["groups", _, "permalink", id] if is_storage_id(id) => {
            Some((*id).to_owned())
        }
        [_, "photos", _, id] if is_storage_id(id) => Some((*id).to_owned()),
        _ => None,
    }
}

fn is_storage_id(value: &str) -> bool {
    (1..=200).contains(&value.len())
        && value.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '~' | '-'))
}

fn instagram_id(url: &Url) -> Option<String> {
    let segs = segments(url);
    match segs.as_slice() {
        ["reel", id] | ["p", id] | ["tv", id] if is_loose_id(id) => Some((*id).to_owned()),
        _ => None,
    }
}

fn x_id(url: &Url) -> Option<String> {
    let segs = segments(url);
    segs.windows(2).find_map(|window| {
        if window[0] == "status"
            && window[1].chars().all(|c| c.is_ascii_digit())
            && !window[1].is_empty()
        {
            Some(window[1].to_owned())
        } else {
            None
        }
    })
}

fn reddit_id(url: &Url) -> Option<String> {
    let host = normalized_host(url)?;
    if host == "v.redd.it" || host == "redd.it" {
        return first_segment(url).filter(|id| is_loose_id(id)).map(str::to_owned);
    }
    let segs = segments(url);
    segs.windows(2).find_map(|window| {
        if window[0] == "comments" && is_loose_id(window[1]) {
            Some(window[1].to_owned())
        } else {
            None
        }
    })
}

fn is_loose_id(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn is_direct_media(url: &Url) -> bool {
    let path = url.path().to_ascii_lowercase();
    DIRECT_EXTENSIONS.iter().any(|ext| path.ends_with(&format!(".{ext}")))
}

/// `img_index` is the carousel slide. That post is a photo, not a video.
pub fn asks_for_instagram_slide(url: &Url) -> bool {
    url.query_pairs().any(|(key, value)| key == "img_index" && !value.is_empty())
}

pub fn strip_tracking(url: &Url) -> String {
    let mut cleaned = url.clone();
    cleaned.set_fragment(None);
    let kept: Vec<(String, String)> = cleaned
        .query_pairs()
        .filter(|(key, _)| !TRACKING_PARAMS.contains(&key.as_ref()))
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    if kept.is_empty() {
        cleaned.set_query(None);
    } else {
        let query = kept.into_iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
        cleaned.set_query(Some(&query));
    }
    if let Some(host) = cleaned.host_str() {
        let _ = cleaned.set_host(Some(&host.to_ascii_lowercase()));
    }
    cleaned.to_string()
}

fn normalized_host(url: &Url) -> Option<String> {
    let host = url.host_str()?.trim_end_matches('.').to_ascii_lowercase();
    Some(host.strip_prefix("www.").unwrap_or(&host).to_owned())
}

fn segments(url: &Url) -> Vec<&str> {
    url.path_segments()
        .map(|parts| parts.filter(|part| !part.is_empty()).collect())
        .unwrap_or_default()
}

fn first_segment(url: &Url) -> Option<&str> {
    url.path_segments()?.find(|part| !part.is_empty())
}

fn query_value(url: &Url, name: &str) -> Option<String> {
    url.query_pairs().find(|(key, _)| key == name).map(|(_, value)| value.into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(raw: &str) -> Url {
        Url::parse(raw).unwrap()
    }

    #[test]
    fn youtube_urls_share_one_content_id() {
        let samples = [
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ&utm_source=share&t=42",
            "https://youtu.be/dQw4w9WgXcQ?si=abc",
            "https://m.youtube.com/shorts/dQw4w9WgXcQ",
            "https://music.youtube.com/watch?v=dQw4w9WgXcQ&feature=share",
            "https://www.youtube.com/embed/dQw4w9WgXcQ",
        ];
        let ids: Vec<_> = samples
            .into_iter()
            .map(|raw| identify_url(&url(raw)).unwrap().unwrap().content_id)
            .collect();
        assert!(ids.iter().all(|id| id.as_str() == "dQw4w9WgXcQ"));
        assert!(
            identify_url(&url(samples[0]))
                .unwrap()
                .unwrap()
                .canonical_url
                .contains("watch?v=dQw4w9WgXcQ")
        );
        assert!(!identify_url(&url(samples[0])).unwrap().unwrap().canonical_url.contains("utm_"));
    }

    #[test]
    fn an_instagram_slide_is_a_photo_request() {
        let slide = url("https://www.instagram.com/p/DfohSk7IDuv/?img_index=2&vrfl=abc");
        assert!(asks_for_instagram_slide(&slide));
        let identified = identify_url(&slide).unwrap().unwrap();
        assert!(identified.canonical_url.contains("img_index=2"));
        assert!(!asks_for_instagram_slide(&url("https://www.instagram.com/reel/DeOmjz1zFPS/")));
    }

    #[test]
    fn other_platforms_extract_canonical_ids() {
        let cases = [
            (
                "https://www.tiktok.com/@name/video/1234567890123456789?is_from_webapp=1",
                PlatformKind::TikTok,
                "1234567890123456789",
            ),
            (
                "https://www.facebook.com/watch/?v=99887766&fbclid=abc",
                PlatformKind::Facebook,
                "99887766",
            ),
            (
                "https://www.facebook.com/zuck/posts/10114604383032681?locale=id_ID",
                PlatformKind::Facebook,
                "10114604383032681",
            ),
            (
                "https://www.facebook.com/100080251467735/posts/1186536767364694/?share_url=https%3A%2F%2Fwww.facebook.com%2Fshare%2F19ji6Wnxkf%2F",
                PlatformKind::Facebook,
                "1186536767364694",
            ),
            (
                "https://www.facebook.com/permalink.php?story_fbid=pfbid0abcDEF123&id=4",
                PlatformKind::Facebook,
                "pfbid0abcDEF123",
            ),
            (
                "https://www.facebook.com/groups/example/posts/55667788",
                PlatformKind::Facebook,
                "55667788",
            ),
            (
                "https://www.instagram.com/reel/AbCDef123/?igsh=xyz",
                PlatformKind::Instagram,
                "AbCDef123",
            ),
            (
                "https://x.com/someone/status/1780000000000000001?s=20",
                PlatformKind::X,
                "1780000000000000001",
            ),
            (
                "https://twitter.com/someone/status/1780000000000000001",
                PlatformKind::X,
                "1780000000000000001",
            ),
            (
                "https://www.reddit.com/r/videos/comments/abc123/title/?utm_source=share",
                PlatformKind::Reddit,
                "abc123",
            ),
            ("https://v.redd.it/xyz987", PlatformKind::Reddit, "xyz987"),
        ];
        for (raw, platform, id) in cases {
            let found = identify_url(&url(raw)).unwrap().unwrap();
            assert_eq!(found.platform, platform, "{raw}");
            assert_eq!(found.content_id.as_str(), id, "{raw}");
        }
    }

    #[test]
    fn direct_media_ignores_tracking_and_rejects_pages() {
        let a = identify_url(&url("https://cdn.example.com/a/file.mp4?utm_source=x")).unwrap();
        let b = identify_url(&url("https://cdn.example.com/a/file.mp4")).unwrap();
        assert_eq!(a.unwrap().content_id, b.unwrap().content_id);
        assert!(identify_url(&url("https://example.com/watch")).is_err());
        assert!(identify_url(&url("https://vm.tiktok.com/ZMabc/")).unwrap().is_none());
        assert!(
            identify_url(&url("https://www.facebook.com/share/19ji6Wnxkf/")).unwrap().is_none()
        );
        for raw in [
            "https://www.facebook.com/share/p/AbC123xyz/",
            "https://www.facebook.com/share/v/AbC123xyz/",
            "https://www.facebook.com/share/r/1G49T9jReV/",
        ] {
            assert!(is_opaque_facebook_share(&url(raw)), "{raw}");
            assert!(identify_url(&url(raw)).unwrap().is_none(), "{raw}");
        }
        assert!(!is_opaque_facebook_share(&url(
            "https://www.facebook.com/reel/1699138351147517/"
        )));
    }
}
