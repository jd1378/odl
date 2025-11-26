use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use percent_encoding::percent_decode_str;
use regex::Regex;
use reqwest::{
    Response, Url,
    header::{
        ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG,
        HeaderMap, LAST_MODIFIED, WWW_AUTHENTICATE,
    },
};

use crate::hash::HashDigest;

#[derive(Debug, Clone)]
pub struct ResponseInfo {
    status_code: u16,
    request_url: Url,
    response_headers: HeaderMap,
}

impl ResponseInfo {
    #[cfg(test)]
    fn new(status_code: u16, request_url: Url, response_headers: HeaderMap) -> Self {
        Self {
            status_code,
            request_url,
            response_headers,
        }
    }

    pub fn is_successful(&self) -> bool {
        (self.status_code >= 200) && (self.status_code <= 299)
    }

    pub fn status_code(&self) -> u16 {
        self.status_code
    }

    pub fn url(&self) -> &Url {
        &self.request_url
    }

    pub fn response_headers(&self) -> &HeaderMap {
        &self.response_headers
    }

    /// Returns the MIME type (media type) from the response headers, if present.
    ///
    /// Looks for the "content-type" header and returns its value as a String,
    /// without any parameters (e.g., charset).
    pub fn mime_type(&self) -> Option<String> {
        self.response_headers
            .get(CONTENT_TYPE)
            .and_then(|val| val.to_str().ok())
            .map(|ct| ct.split(';').next().unwrap_or("").trim().to_string())
            .filter(|s| !s.is_empty())
    }

    /// Retrieve the content_length of the response.
    ///
    /// Returns None if the "content-length" header is missing or if its value
    /// is not a u64.
    fn content_length(&self) -> Option<u64> {
        self.response_headers
            .get(CONTENT_LENGTH)
            .and_then(|val| val.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
    }

    /// Returns the total length of the resource, even if this is a partial response.
    pub fn total_length(&self) -> Option<u64> {
        if let Some(content_range) = self.content_range()
            && content_range.total.is_some()
        {
            return content_range.total;
        }
        self.content_length()
    }

    /// Retrieve etag of the response.
    pub fn etag(&self) -> Option<String> {
        self.response_headers
            .get(ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    }

    /// Extracts filename from response headers or request url. if both fail, returns "download"
    pub fn extract_filename(&self) -> String {
        // Try to extract filename from Content-Disposition header
        if let Some(header) = self.response_headers.get(CONTENT_DISPOSITION)
            && let Ok(header_str) = header.to_str()
        {
            // Look for filename*= (RFC 5987), then fallback to filename=
            for part in header_str.split(';').map(str::trim) {
                if let Some(val) = part.strip_prefix("filename*=") {
                    // e.g. filename*=UTF-8''%E2%82%AC%20rates
                    let value = val.trim_matches('"').trim_start_matches("UTF-8''");
                    if let Ok(decoded) = percent_decode_str(value).decode_utf8() {
                        return decoded.into_owned();
                    }
                } else if let Some(val) = part.strip_prefix("filename=") {
                    return val.trim_matches('"').to_string();
                }
            }
        }
        // Fallback: extract from URL path
        if let Some(name) = self
            .request_url
            .path_segments()
            .and_then(|mut segments| segments.next_back())
            .filter(|s| !s.is_empty())
        {
            return name.to_string();
        }
        // Default fallback
        "download".to_string()
    }

    /// Extracts content range value from response headers
    pub fn content_range(&self) -> Option<ContentRange> {
        self.response_headers
            .get(CONTENT_RANGE)
            .and_then(|val| val.to_str().ok())
            .and_then(|header| {
                CONTENT_RANGE_RE.captures(header).and_then(|caps| {
                    let start = caps.get(1)?.as_str().parse().ok()?;
                    let end = caps.get(2)?.as_str().parse().ok()?;
                    let total = match caps.get(3)?.as_str() {
                        "*" => None,
                        n => n.parse().ok(),
                    };
                    Some(ContentRange { start, end, total })
                })
            })
    }

    /// Checks whether server indicates that it accepts ranges
    pub fn accepts_ranges(&self) -> bool {
        self.response_headers
            .get(ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok())
            .map(|s| !s.eq_ignore_ascii_case("none"))
            .unwrap_or(false)
    }

    /// Parses the "Last-Modified" header from the response into a unix timestamp.
    ///
    /// Returns None if the header is missing or cannot be parsed.
    pub fn parse_last_modified(&self) -> Option<i64> {
        self.response_headers
            .get(LAST_MODIFIED)
            .and_then(|header_value| {
                header_value.to_str().ok().and_then(|date_str| {
                    // RFC 2822 format is expected for Last-Modified
                    DateTime::parse_from_rfc2822(date_str)
                        .map(|dt| dt.with_timezone(&Utc))
                        .map(|x| x.timestamp())
                        .ok()
                })
            })
    }

    /// Returns whether the server has returned a 401 response
    pub fn requires_auth(&self) -> bool {
        self.status_code == 401
    }

    /// Returns whether the server requires a basic auth
    pub fn requires_basic_auth(&self) -> bool {
        self.requires_auth()
            && self
                .response_headers
                .get(WWW_AUTHENTICATE)
                .and_then(|val| val.to_str().ok())
                .map(|s| s.to_ascii_lowercase().contains("basic"))
                .unwrap_or(false)
    }

    pub fn is_partial(&self) -> bool {
        self.status_code == 206
    }

    pub fn is_resumable(&self) -> bool {
        self.accepts_ranges()
            || (self.is_partial() && self.content_range().is_some_and(|x| x.total.is_some()))
    }

    /// Extracts common hash values from response headers.
    /// Returns a vector of detected hashes (MD5, SHA1, SHA256, SHA384, SHA512).
    /// Returns early as soon as any of stronger hashes are found: SHA512, SHA384, SHA256
    pub fn extract_hashes(&self) -> Vec<HashDigest> {
        let mut hashes = Vec::new();

        // Digest (RFC 3230, e.g. "SHA-256=abc..., md5=xyz...")
        if let Some(val) = self.response_headers.get("Digest")
            && let Ok(s) = val.to_str()
        {
            for part in s.split(',') {
                let part = part.trim();
                if let Some((alg, value)) = part.split_once('=') {
                    let value = value.trim();
                    match alg.trim().to_ascii_lowercase().as_str() {
                        "sha-512" => {
                            hashes.push(HashDigest::SHA512(
                                value.to_string(),
                                crate::hash::HashEncoding::Base64,
                            ));
                            return hashes;
                        }
                        "sha-384" => {
                            hashes.push(HashDigest::SHA384(
                                value.to_string(),
                                crate::hash::HashEncoding::Base64,
                            ));
                            return hashes;
                        }
                        "sha-256" => {
                            hashes.push(HashDigest::SHA256(
                                value.to_string(),
                                crate::hash::HashEncoding::Base64,
                            ));
                            return hashes;
                        }
                        "sha" | "sha1" | "sha-1" => hashes.push(HashDigest::SHA1(
                            value.to_string(),
                            crate::hash::HashEncoding::Base64,
                        )),
                        "md5" => hashes.push(HashDigest::MD5(
                            value.to_string(),
                            crate::hash::HashEncoding::Base64,
                        )),

                        _ => {}
                    }
                }
            }
        }

        // X-Checksum-*
        for (name, value) in self.response_headers.iter() {
            let name_str = name.as_str().to_ascii_lowercase();
            if let Some(suffix) = name_str.strip_prefix("x-checksum-")
                && let Ok(val_str) = value.to_str()
            {
                let val_trimmed = val_str.trim();
                // It's usually hex,
                // If it's not hex, It's base64
                fn is_hex(s: &str) -> bool {
                    s.len().is_multiple_of(2) && s.chars().all(|c| c.is_ascii_hexdigit())
                }
                let encoding = if is_hex(val_trimmed) {
                    crate::hash::HashEncoding::Hex
                } else {
                    crate::hash::HashEncoding::Base64
                };
                match suffix {
                    "sha512" => {
                        hashes.push(HashDigest::SHA512(val_trimmed.to_string(), encoding));
                        return hashes;
                    }
                    "sha384" => {
                        hashes.push(HashDigest::SHA384(val_trimmed.to_string(), encoding));
                        return hashes;
                    }
                    "sha256" => {
                        hashes.push(HashDigest::SHA256(val_trimmed.to_string(), encoding));
                        return hashes;
                    }
                    "sha" | "sha1" | "sha-1" => {
                        hashes.push(HashDigest::SHA1(val_trimmed.to_string(), encoding))
                    }
                    "md5" => hashes.push(HashDigest::MD5(val_trimmed.to_string(), encoding)),
                    _ => {}
                }
            }
        }

        // Content-SHA256
        if let Some(val) = self.response_headers.get("Content-SHA256")
            && let Ok(s) = val.to_str()
        {
            hashes.push(HashDigest::SHA256(
                s.trim().to_string(),
                crate::hash::HashEncoding::Hex,
            ));
            return hashes;
        }

        // Content-MD5 rfc1864
        if let Some(val) = self.response_headers.get("Content-MD5")
            && let Ok(s) = val.to_str()
        {
            hashes.push(HashDigest::MD5(
                s.trim().to_string(),
                crate::hash::HashEncoding::Base64,
            ));
        }

        // Prefer Content-Digest (RFC 9530) over Repr-Digest if present
        if let Some(val) = self.response_headers.get("Content-Digest") {
            if let Ok(s) = val.to_str() {
                for part in s.split(',') {
                    let part = part.trim();
                    if let Some((alg, value)) = part.split_once('=') {
                        let value = value.trim();
                        // Content-Digest values are surrounded by colons, e.g. ":base64value:"
                        let value = value.trim_matches(':');
                        match alg.trim().to_ascii_lowercase().as_str() {
                            "sha-512" => {
                                hashes.push(HashDigest::SHA512(
                                    value.to_string(),
                                    crate::hash::HashEncoding::Base64,
                                ));
                                return hashes;
                            }
                            "sha-384" => {
                                hashes.push(HashDigest::SHA384(
                                    value.to_string(),
                                    crate::hash::HashEncoding::Base64,
                                ));
                                return hashes;
                            }
                            "sha-256" => {
                                hashes.push(HashDigest::SHA256(
                                    value.to_string(),
                                    crate::hash::HashEncoding::Base64,
                                ));
                                return hashes;
                            }
                            "sha" | "sha1" | "sha-1" => hashes.push(HashDigest::SHA1(
                                value.to_string(),
                                crate::hash::HashEncoding::Base64,
                            )),
                            "md5" => hashes.push(HashDigest::MD5(
                                value.to_string(),
                                crate::hash::HashEncoding::Base64,
                            )),
                            _ => {}
                        }
                    }
                }
            }
        } else if let Some(val) = self.response_headers.get("Repr-Digest")
            && let Ok(s) = val.to_str()
        {
            for part in s.split(',') {
                let part = part.trim();
                if let Some((alg, value)) = part.split_once('=') {
                    let value = value.trim();
                    // Repr-Digest values are surrounded by colons, e.g. ":base64value:"
                    let value = value.trim_matches(':');
                    match alg.trim().to_ascii_lowercase().as_str() {
                        "sha-512" => {
                            hashes.push(HashDigest::SHA512(
                                value.to_string(),
                                crate::hash::HashEncoding::Base64,
                            ));
                            return hashes;
                        }
                        "sha-384" => {
                            hashes.push(HashDigest::SHA384(
                                value.to_string(),
                                crate::hash::HashEncoding::Base64,
                            ));
                            return hashes;
                        }
                        "sha-256" => {
                            hashes.push(HashDigest::SHA256(
                                value.to_string(),
                                crate::hash::HashEncoding::Base64,
                            ));
                            return hashes;
                        }
                        "sha" | "sha1" | "sha-1" => hashes.push(HashDigest::SHA1(
                            value.to_string(),
                            crate::hash::HashEncoding::Base64,
                        )),
                        "md5" => hashes.push(HashDigest::MD5(
                            value.to_string(),
                            crate::hash::HashEncoding::Base64,
                        )),
                        _ => {}
                    }
                }
            }
        }

        hashes
    }
}

impl From<Response> for ResponseInfo {
    fn from(value: Response) -> Self {
        Self {
            status_code: value.status().as_u16(),
            request_url: value.url().to_owned(),
            response_headers: value.headers().to_owned(),
        }
    }
}

static CONTENT_RANGE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^bytes (\d+)-(\d+)/(\d+|\*)$").unwrap());

pub struct ContentRange {
    pub start: u64,
    pub end: u64,
    pub total: Option<u64>,
}

#[cfg(test)]
mod tests {
    use chrono::Datelike;
    use reqwest::header::HeaderValue;

    use super::*;

    fn make_response_info_with_headers(url: &str, headers: HeaderMap) -> ResponseInfo {
        ResponseInfo::new(200, Url::parse(url).unwrap(), headers)
    }

    #[test]
    fn test_accepts_ranges() {
        let mut headers = HeaderMap::new();
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        assert!(!resp.accepts_ranges());
        headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        assert!(resp.accepts_ranges());
        headers.insert(ACCEPT_RANGES, HeaderValue::from_static("none"));
        let resp = make_response_info_with_headers("http://example.com", headers);
        assert!(!resp.accepts_ranges());
    }

    #[test]
    fn test_content_length() {
        let mut headers = HeaderMap::new();
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        assert_eq!(resp.content_length(), None);
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("12345"));
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        assert_eq!(resp.content_length(), Some(12345));
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("notanumber"));
        let resp = make_response_info_with_headers("http://example.com", headers);
        assert_eq!(resp.content_length(), None);
    }

    #[test]
    fn test_extract_filename_content_disposition() {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_DISPOSITION,
            HeaderValue::from_static("attachment; filename=\"example.txt\""),
        );
        let resp =
            make_response_info_with_headers("http://example.com/path/to/file", headers.clone());
        assert_eq!(resp.extract_filename(), "example.txt");

        headers.insert(
            CONTENT_DISPOSITION,
            HeaderValue::from_static("attachment; filename*=UTF-8''%E2%82%AC%20rates"),
        );
        let resp = make_response_info_with_headers("http://example.com/path/to/file", headers);
        assert_eq!(resp.extract_filename(), "€ rates");
    }

    #[test]
    fn test_extract_filename_from_url() {
        let headers = HeaderMap::new();
        let resp =
            make_response_info_with_headers("http://example.com/path/to/file.txt", headers.clone());
        assert_eq!(resp.extract_filename(), "file.txt");

        let resp = make_response_info_with_headers("http://example.com/", headers);
        assert_eq!(resp.extract_filename(), "download");
    }

    #[test]
    fn test_parse_last_modified() {
        let mut headers = HeaderMap::new();
        headers.insert(
            LAST_MODIFIED,
            HeaderValue::from_static("Wed, 21 Oct 2015 07:28:00 GMT"),
        );
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        let dt = resp.parse_last_modified();
        assert!(dt.is_some());
        let dt = dt.unwrap();
        // dt is i64 (timestamp), convert to DateTime<Utc> for checking

        let dt = DateTime::from_timestamp(dt, 0).unwrap();
        assert_eq!(dt.year(), 2015);
        assert_eq!(dt.month(), 10);
        assert_eq!(dt.day(), 21);

        headers.insert(LAST_MODIFIED, HeaderValue::from_static("invalid-date"));
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        assert!(resp.parse_last_modified().is_none());

        let headers = HeaderMap::new();
        let resp = make_response_info_with_headers("http://example.com", headers);
        assert!(resp.parse_last_modified().is_none());
    }

    #[test]
    fn test_content_range() {
        let mut headers = HeaderMap::new();
        // Valid content-range header
        headers.insert(CONTENT_RANGE, HeaderValue::from_static("bytes 0-499/1234"));
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        let cr = resp.content_range();
        assert!(cr.is_some());
        let cr = cr.unwrap();
        assert_eq!(cr.start, 0);
        assert_eq!(cr.end, 499);
        assert_eq!(cr.total, Some(1234));

        // Content-range with unknown total
        headers.insert(CONTENT_RANGE, HeaderValue::from_static("bytes 0-499/*"));
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        let cr = resp.content_range();
        assert!(cr.is_some());
        let cr = cr.unwrap();
        assert_eq!(cr.start, 0);
        assert_eq!(cr.end, 499);
        assert_eq!(cr.total, None);

        // Invalid content-range header
        headers.insert(CONTENT_RANGE, HeaderValue::from_static("invalid"));
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        assert!(resp.content_range().is_none());

        // No content-range header
        let headers = HeaderMap::new();
        let resp = make_response_info_with_headers("http://example.com", headers);
        assert!(resp.content_range().is_none());
    }

    #[test]
    fn test_etag() {
        let mut headers = HeaderMap::new();
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        assert_eq!(resp.etag(), None);

        headers.insert(ETAG, HeaderValue::from_static("\"abc123\""));
        let resp = make_response_info_with_headers("http://example.com", headers);
        assert_eq!(resp.etag(), Some("\"abc123\"".to_string()));
    }

    #[test]
    fn test_is_successful() {
        let headers = HeaderMap::new();
        let resp = ResponseInfo::new(
            200,
            Url::parse("http://example.com").unwrap(),
            headers.clone(),
        );
        assert!(resp.is_successful());

        let resp = ResponseInfo::new(
            299,
            Url::parse("http://example.com").unwrap(),
            headers.clone(),
        );
        assert!(resp.is_successful());

        let resp = ResponseInfo::new(
            199,
            Url::parse("http://example.com").unwrap(),
            headers.clone(),
        );
        assert!(!resp.is_successful());

        let resp = ResponseInfo::new(300, Url::parse("http://example.com").unwrap(), headers);
        assert!(!resp.is_successful());
    }

    #[test]
    fn test_total_length() {
        let mut headers = HeaderMap::new();

        // No content-length or content-range
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        assert_eq!(resp.total_length(), None);

        // Only content-length
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("1234"));
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        assert_eq!(resp.total_length(), Some(1234));

        // Content-range with total
        headers.insert(CONTENT_RANGE, HeaderValue::from_static("bytes 0-499/2000"));
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        assert_eq!(resp.total_length(), Some(2000));

        // Content-range with unknown total, fallback to content-length
        headers.insert(CONTENT_RANGE, HeaderValue::from_static("bytes 0-499/*"));
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        assert_eq!(resp.total_length(), Some(1234));

        // Invalid content-range, fallback to content-length
        headers.insert(CONTENT_RANGE, HeaderValue::from_static("invalid"));
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        assert_eq!(resp.total_length(), Some(1234));

        // Content-length not a number, should fallback to None
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("notanumber"));
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        assert_eq!(resp.total_length(), None);

        // Content-range with total, but content-length is invalid
        headers.insert(CONTENT_RANGE, HeaderValue::from_static("bytes 0-499/3000"));
        let resp = make_response_info_with_headers("http://example.com", headers);
        assert_eq!(resp.total_length(), Some(3000));
    }

    #[test]
    fn test_extract_hashes_digest_header() {
        let mut headers = HeaderMap::new();
        // Only MD5
        headers.insert("Digest", HeaderValue::from_static("md5=abc123=="));
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        let hashes = resp.extract_hashes();
        assert_eq!(hashes.len(), 1);
        match &hashes[0] {
            HashDigest::MD5(val, enc) => {
                assert_eq!(val, "abc123==");
                assert_eq!(*enc, crate::hash::HashEncoding::Base64);
            }
            _ => panic!("Expected MD5"),
        }

        // SHA-256 and MD5, should return only SHA-256 (stronger hash)
        headers.insert(
            "Digest",
            HeaderValue::from_static("sha-256=sha256val, md5=md5val"),
        );
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        let hashes = resp.extract_hashes();
        assert_eq!(hashes.len(), 1);
        match &hashes[0] {
            HashDigest::SHA256(val, enc) => {
                assert_eq!(val, "sha256val");
                assert_eq!(*enc, crate::hash::HashEncoding::Base64);
            }
            _ => panic!("Expected SHA256"),
        }

        // SHA-512, should return only SHA-512
        headers.insert(
            "Digest",
            HeaderValue::from_static("sha-512=sha512val, sha-256=sha256val"),
        );
        let resp = make_response_info_with_headers("http://example.com", headers);
        let hashes = resp.extract_hashes();
        assert_eq!(hashes.len(), 1);
        match &hashes[0] {
            HashDigest::SHA512(val, enc) => {
                assert_eq!(val, "sha512val");
                assert_eq!(*enc, crate::hash::HashEncoding::Base64);
            }
            _ => panic!("Expected SHA512"),
        }
    }

    #[test]
    fn test_extract_hashes_x_checksum_headers() {
        let mut headers = HeaderMap::new();
        // Hex SHA256
        headers.insert(
            "x-checksum-sha256",
            HeaderValue::from_static("abcdef1234567890"),
        );
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        let hashes = resp.extract_hashes();
        assert_eq!(hashes.len(), 1);
        match &hashes[0] {
            HashDigest::SHA256(val, enc) => {
                assert_eq!(val, "abcdef1234567890");
                assert_eq!(*enc, crate::hash::HashEncoding::Hex);
            }
            _ => panic!("Expected SHA256"),
        }

        headers.clear();

        // Base64 SHA1 (not hex)
        headers.insert(
            "x-checksum-sha1",
            HeaderValue::from_static("nothexbase64=="),
        );
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        let hashes = resp.extract_hashes();
        assert!(
            hashes
                .iter()
                .any(|h| matches!(h, HashDigest::SHA1(_, crate::hash::HashEncoding::Base64)))
        );

        // MD5
        headers.insert(
            "x-checksum-md5",
            HeaderValue::from_static("abcdefabcdefabcdefabcdefabcdefab"),
        );
        let resp = make_response_info_with_headers("http://example.com", headers);
        let hashes = resp.extract_hashes();
        assert!(hashes.iter().any(|h| matches!(h, HashDigest::MD5(_, _))));
    }

    #[test]
    fn test_extract_hashes_content_sha256_and_md5() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "Content-SHA256",
            HeaderValue::from_static("abcdefabcdefabcdefabcdefabcdefab"),
        );
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        let hashes = resp.extract_hashes();
        assert_eq!(hashes.len(), 1);
        match &hashes[0] {
            HashDigest::SHA256(val, enc) => {
                assert_eq!(val, "abcdefabcdefabcdefabcdefabcdefab");
                assert_eq!(*enc, crate::hash::HashEncoding::Hex);
            }
            _ => panic!("Expected SHA256"),
        }

        headers.clear();

        // Content-MD5 (base64)
        headers.insert(
            "Content-MD5",
            HeaderValue::from_static("YWJjZGVmMTIzNDU2Nzg5MA=="),
        );
        let resp = make_response_info_with_headers("http://example.com", headers);
        let hashes = resp.extract_hashes();
        assert!(hashes.iter().any(|h| matches!(h, HashDigest::MD5(val, crate::hash::HashEncoding::Base64) if val == "YWJjZGVmMTIzNDU2Nzg5MA==")));
    }

    #[test]
    fn test_extract_hashes_repr_digest() {
        let mut headers = HeaderMap::new();
        // Only SHA-256
        headers.insert(
            "Repr-Digest",
            HeaderValue::from_static("sha-256=:base64sha256:"),
        );
        let resp = make_response_info_with_headers("http://example.com", headers.clone());
        let hashes = resp.extract_hashes();
        assert_eq!(hashes.len(), 1);
        match &hashes[0] {
            HashDigest::SHA256(val, enc) => {
                assert_eq!(val, "base64sha256");
                assert_eq!(*enc, crate::hash::HashEncoding::Base64);
            }
            _ => panic!("Expected SHA256"),
        }

        // SHA-512 and MD5, should return only SHA-512
        headers.insert(
            "Repr-Digest",
            HeaderValue::from_static("sha-512=:base64sha512:, md5=:md5val:"),
        );
        let resp = make_response_info_with_headers("http://example.com", headers);
        let hashes = resp.extract_hashes();
        assert_eq!(hashes.len(), 1);
        match &hashes[0] {
            HashDigest::SHA512(val, enc) => {
                assert_eq!(val, "base64sha512");
                assert_eq!(*enc, crate::hash::HashEncoding::Base64);
            }
            _ => panic!("Expected SHA512"),
        }
    }

    #[test]
    fn test_extract_hashes_none() {
        let headers = HeaderMap::new();
        let resp = make_response_info_with_headers("http://example.com", headers);
        let hashes = resp.extract_hashes();
        assert!(hashes.is_empty());
    }
}
