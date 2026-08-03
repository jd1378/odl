//! Which hosts are delegated to `yt-dlp`.
//!
//! yt-dlp exposes no machine-readable list of the URLs its extractors claim —
//! `--list-extractors` yields class names, not patterns — so this list is
//! curated by hand and deliberately limited to major, well-known sites. It is
//! not meant to mirror yt-dlp's full extractor set; users who need more can
//! extend it through `ytdlp.extra_hosts`.

use reqwest::Url;

/// Registrable domains delegated to `yt-dlp`.
///
/// Keep sorted, lowercase, and without a leading dot — [`is_delegated_url`]
/// relies on all three, and a unit test enforces them.
static DELEGATED_HOSTS: &[&str] = &[
    "bandcamp.com",
    "bilibili.com",
    "bilibili.tv",
    "dailymotion.com",
    "facebook.com",
    "instagram.com",
    "nicovideo.jp",
    "odysee.com",
    "reddit.com",
    "rumble.com",
    "soundcloud.com",
    "tiktok.com",
    "twitch.tv",
    "twitter.com",
    "vimeo.com",
    "x.com",
    "youtu.be",
    "youtube.com",
];

/// Whether `host` is `domain` itself or a subdomain of it.
///
/// Matching is exact or dot-anchored, never a substring test: a substring
/// match would delegate `youtube.com.example.net`, handing an attacker-chosen
/// page to an extractor purely because of a name it does not own.
fn matches_domain(host: &str, domain: &str) -> bool {
    if host == domain {
        return true;
    }
    host.len() > domain.len()
        && host.ends_with(domain)
        && host.as_bytes()[host.len() - domain.len() - 1] == b'.'
}

/// Whether this URL should be handed to `yt-dlp`.
///
/// `extra` adds domains to the built-in list; `excluded` removes them and
/// wins over both, so a user can always veto a site we ship.
///
/// Only `http`/`https` URLs are ever delegated.
pub fn is_delegated_url(url: &Url, extra: &[String], excluded: &[String]) -> bool {
    if !matches!(url.scheme(), "http" | "https") {
        return false;
    }
    let Some(host) = url.host_str() else {
        return false;
    };
    // `Url` lowercases and punycodes hosts on parse, but a caller could have
    // built one by hand; normalizing here keeps matching predictable.
    let host = host.trim_matches('.').to_ascii_lowercase();

    if excluded.iter().any(|d| matches_domain(&host, d)) {
        return false;
    }
    DELEGATED_HOSTS.iter().any(|d| matches_domain(&host, d))
        || extra.iter().any(|d| matches_domain(&host, d))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    #[test]
    fn builtin_list_is_sorted_lowercase_and_bare() {
        let mut sorted = DELEGATED_HOSTS.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted, DELEGATED_HOSTS,
            "DELEGATED_HOSTS must be sorted and free of duplicates"
        );
        for host in DELEGATED_HOSTS {
            assert_eq!(*host, host.to_ascii_lowercase(), "{host} must be lowercase");
            assert!(host.contains('.'), "{host} must be a registrable domain");
            assert!(!host.starts_with('.'), "{host} must not have a leading dot");
            assert!(!host.contains('/'), "{host} must not contain a path");
            assert!(host.is_ascii(), "{host} must be punycode, not unicode");
        }
    }

    #[test]
    fn matches_domain_and_subdomains() {
        assert!(is_delegated_url(
            &url("https://youtube.com/watch?v=x"),
            &[],
            &[]
        ));
        assert!(is_delegated_url(
            &url("https://www.youtube.com/watch?v=x"),
            &[],
            &[]
        ));
        assert!(is_delegated_url(&url("https://m.youtube.com/"), &[], &[]));
        assert!(is_delegated_url(&url("https://youtu.be/abc"), &[], &[]));
    }

    #[test]
    fn does_not_match_lookalike_hosts() {
        // The case that a substring match would get wrong.
        assert!(!is_delegated_url(
            &url("https://youtube.com.evil.example/"),
            &[],
            &[]
        ));
        assert!(!is_delegated_url(&url("https://notyoutube.com/"), &[], &[]));
        assert!(!is_delegated_url(
            &url("https://youtube.company/"),
            &[],
            &[]
        ));
        assert!(!is_delegated_url(&url("https://example.com/"), &[], &[]));
    }

    #[test]
    fn host_matching_is_case_insensitive() {
        assert!(is_delegated_url(&url("https://WWW.YouTube.COM/"), &[], &[]));
    }

    #[test]
    fn only_http_schemes_are_delegated() {
        assert!(!is_delegated_url(&url("ftp://youtube.com/file"), &[], &[]));
        assert!(!is_delegated_url(&url("file:///tmp/youtube.com"), &[], &[]));
    }

    #[test]
    fn extra_hosts_extend_and_exclusions_win() {
        let extra = vec!["video.example".to_string()];
        assert!(is_delegated_url(
            &url("https://cdn.video.example/x"),
            &extra,
            &[]
        ));

        let excluded = vec!["youtube.com".to_string()];
        assert!(!is_delegated_url(
            &url("https://www.youtube.com/"),
            &[],
            &excluded
        ));
        // Exclusion beats an explicit extra entry too.
        assert!(!is_delegated_url(
            &url("https://video.example/"),
            &extra,
            &["video.example".to_string()]
        ));
    }
}
