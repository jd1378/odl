use base64::{Engine as _, engine::general_purpose};
use digest::Digest;
use md5::Md5;
use sha1::Sha1;
use sha2::{Sha256, Sha384, Sha512};
use std::fmt::Write as _;
use tokio::io::{self as async_io, AsyncRead, AsyncReadExt};

use crate::download_metadata::{ChecksumAlgorithm, ChecksumEncoding, FileChecksum};

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{:02x}", b);
    }
    s
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HashDigest {
    SHA512(String, HashEncoding),
    SHA384(String, HashEncoding),
    SHA256(String, HashEncoding),
    SHA1(String, HashEncoding),
    MD5(String, HashEncoding),
}

impl HashDigest {
    /// Hashes the contents of an async reader using the specified Digest type (async).
    async fn hash_reader<D: Digest + Default + Unpin>(
        mut reader: impl AsyncRead + Unpin,
    ) -> async_io::Result<D> {
        let mut hasher = D::default();
        let mut buf = [0u8; 8192];
        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        Ok(hasher)
    }

    /// Compute a hash from an async reader using the specified algorithm and encoding (async).
    pub async fn from_reader_with_algorithm<R: AsyncRead + Unpin>(
        reader: R,
        algo: HashAlgorithm,
        encoding: HashEncoding,
    ) -> async_io::Result<HashDigest> {
        match algo {
            HashAlgorithm::MD5 => {
                let hasher = Self::hash_reader::<Md5>(reader).await?;
                let bytes = hasher.finalize();
                let s = match encoding {
                    HashEncoding::Hex => to_hex(&bytes),
                    HashEncoding::Base64 => general_purpose::STANDARD.encode(bytes),
                };
                Ok(HashDigest::MD5(s, encoding))
            }
            HashAlgorithm::SHA1 => {
                let hasher = Self::hash_reader::<Sha1>(reader).await?;
                let bytes = hasher.finalize();
                let s = match encoding {
                    HashEncoding::Hex => to_hex(&bytes),
                    HashEncoding::Base64 => general_purpose::STANDARD.encode(bytes),
                };
                Ok(HashDigest::SHA1(s, encoding))
            }
            HashAlgorithm::SHA256 => {
                let hasher = Self::hash_reader::<Sha256>(reader).await?;
                let bytes = hasher.finalize();
                let s = match encoding {
                    HashEncoding::Hex => to_hex(&bytes),
                    HashEncoding::Base64 => general_purpose::STANDARD.encode(bytes),
                };
                Ok(HashDigest::SHA256(s, encoding))
            }
            HashAlgorithm::SHA384 => {
                let hasher = Self::hash_reader::<Sha384>(reader).await?;
                let bytes = hasher.finalize();
                let s = match encoding {
                    HashEncoding::Hex => to_hex(&bytes),
                    HashEncoding::Base64 => general_purpose::STANDARD.encode(bytes),
                };
                Ok(HashDigest::SHA384(s, encoding))
            }
            HashAlgorithm::SHA512 => {
                let hasher = Self::hash_reader::<Sha512>(reader).await?;
                let bytes = hasher.finalize();
                let s = match encoding {
                    HashEncoding::Hex => to_hex(&bytes),
                    HashEncoding::Base64 => general_purpose::STANDARD.encode(bytes),
                };
                Ok(HashDigest::SHA512(s, encoding))
            }
        }
    }

    /// Parse a user-supplied checksum string into a `HashDigest`.
    ///
    /// Accepts `ALGO:DIGEST` (digest hex-encoded) or
    /// `ALGO:ENCODING:DIGEST`. `ALGO` is one of `md5`, `sha1`, `sha256`,
    /// `sha384`, `sha512`; `ENCODING` is `hex` (default) or `base64`.
    /// Hex digests are validated for length and lowercased so they match
    /// the lowercase hex produced during verification.
    pub fn parse_cli(s: &str) -> Result<HashDigest, String> {
        let parts: Vec<&str> = s.splitn(3, ':').collect();
        let (algo_str, encoding, digest) = match parts.as_slice() {
            [algo, digest] => (*algo, HashEncoding::Hex, (*digest).to_string()),
            [algo, enc, digest] => {
                let encoding = match enc.to_ascii_lowercase().as_str() {
                    "hex" => HashEncoding::Hex,
                    "base64" | "b64" => HashEncoding::Base64,
                    other => return Err(format!("unknown checksum encoding '{other}'")),
                };
                (*algo, encoding, (*digest).to_string())
            }
            _ => {
                return Err(format!(
                    "invalid checksum '{s}': expected ALGO:DIGEST or ALGO:ENCODING:DIGEST"
                ));
            }
        };

        let algo = match algo_str.to_ascii_lowercase().as_str() {
            "md5" => HashAlgorithm::MD5,
            "sha1" => HashAlgorithm::SHA1,
            "sha256" => HashAlgorithm::SHA256,
            "sha384" => HashAlgorithm::SHA384,
            "sha512" => HashAlgorithm::SHA512,
            other => return Err(format!("unknown checksum algorithm '{other}'")),
        };

        if digest.is_empty() {
            return Err(format!("invalid checksum '{s}': empty digest"));
        }

        let digest = match encoding {
            HashEncoding::Hex => {
                let expected_len = match algo {
                    HashAlgorithm::MD5 => 32,
                    HashAlgorithm::SHA1 => 40,
                    HashAlgorithm::SHA256 => 64,
                    HashAlgorithm::SHA384 => 96,
                    HashAlgorithm::SHA512 => 128,
                };
                if digest.len() != expected_len || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
                    return Err(format!(
                        "invalid {algo:?} hex digest: expected {expected_len} hex characters"
                    ));
                }
                digest.to_ascii_lowercase()
            }
            HashEncoding::Base64 => {
                let decoded = general_purpose::STANDARD
                    .decode(digest.as_bytes())
                    .map_err(|e| format!("invalid base64 digest: {e}"))?;
                let expected_bytes = match algo {
                    HashAlgorithm::MD5 => 16,
                    HashAlgorithm::SHA1 => 20,
                    HashAlgorithm::SHA256 => 32,
                    HashAlgorithm::SHA384 => 48,
                    HashAlgorithm::SHA512 => 64,
                };
                if decoded.len() != expected_bytes {
                    return Err(format!(
                        "invalid {algo:?} base64 digest: expected {expected_bytes} bytes, got {}",
                        decoded.len()
                    ));
                }
                digest
            }
        };

        Ok(match algo {
            HashAlgorithm::MD5 => HashDigest::MD5(digest, encoding),
            HashAlgorithm::SHA1 => HashDigest::SHA1(digest, encoding),
            HashAlgorithm::SHA256 => HashDigest::SHA256(digest, encoding),
            HashAlgorithm::SHA384 => HashDigest::SHA384(digest, encoding),
            HashAlgorithm::SHA512 => HashDigest::SHA512(digest, encoding),
        })
    }

    /// Compute a hash from an async reader using the algorithm implied by the HashDigest variant and encoding (async).
    pub async fn from_reader<R: AsyncRead + Unpin>(
        reader: R,
        hash_type: &HashDigest,
    ) -> async_io::Result<HashDigest> {
        let algo = HashAlgorithm::from(hash_type);
        let encoding = HashEncoding::from(hash_type);
        Self::from_reader_with_algorithm(reader, algo, encoding).await
    }
}

/// Supported hash algorithms for file/content checksums.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum HashAlgorithm {
    // Ordered from strongest to weakest.
    // We only check the strongest one from the ones available.
    SHA512,
    SHA384,
    SHA256,
    SHA1,
    MD5,
}

impl From<&HashDigest> for HashAlgorithm {
    fn from(hash_type: &HashDigest) -> Self {
        match hash_type {
            HashDigest::MD5(_, _) => HashAlgorithm::MD5,
            HashDigest::SHA1(_, _) => HashAlgorithm::SHA1,
            HashDigest::SHA256(_, _) => HashAlgorithm::SHA256,
            HashDigest::SHA384(_, _) => HashAlgorithm::SHA384,
            HashDigest::SHA512(_, _) => HashAlgorithm::SHA512,
        }
    }
}

impl From<&HashDigest> for HashEncoding {
    fn from(hash_type: &HashDigest) -> Self {
        match hash_type {
            HashDigest::MD5(_, encoding)
            | HashDigest::SHA1(_, encoding)
            | HashDigest::SHA256(_, encoding)
            | HashDigest::SHA384(_, encoding)
            | HashDigest::SHA512(_, encoding) => *encoding,
        }
    }
}
impl TryFrom<&FileChecksum> for HashDigest {
    type Error = &'static str;

    fn try_from(checksum: &FileChecksum) -> Result<Self, Self::Error> {
        let algo = match ChecksumAlgorithm::try_from(checksum.algorithm) {
            Ok(ChecksumAlgorithm::Sha512) => HashAlgorithm::SHA512,
            Ok(ChecksumAlgorithm::Sha384) => HashAlgorithm::SHA384,
            Ok(ChecksumAlgorithm::Sha256) => HashAlgorithm::SHA256,
            Ok(ChecksumAlgorithm::Sha1) => HashAlgorithm::SHA1,
            Ok(ChecksumAlgorithm::Md5) => HashAlgorithm::MD5,
            _ => return Err("Unknown checksum algorithm"),
        };

        let encoding = match ChecksumEncoding::try_from(checksum.encoding) {
            Ok(ChecksumEncoding::Hex) => HashEncoding::Hex,
            Ok(ChecksumEncoding::Base64) => HashEncoding::Base64,
            _ => return Err("Unknown checksum encoding"),
        };

        let digest = checksum.digest.clone();

        let hash_digest = match algo {
            HashAlgorithm::SHA512 => HashDigest::SHA512(digest, encoding),
            HashAlgorithm::SHA384 => HashDigest::SHA384(digest, encoding),
            HashAlgorithm::SHA256 => HashDigest::SHA256(digest, encoding),
            HashAlgorithm::SHA1 => HashDigest::SHA1(digest, encoding),
            HashAlgorithm::MD5 => HashDigest::MD5(digest, encoding),
        };

        Ok(hash_digest)
    }
}

impl TryFrom<FileChecksum> for HashDigest {
    type Error = &'static str;
    fn try_from(checksum: FileChecksum) -> Result<Self, Self::Error> {
        HashDigest::try_from(&checksum)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum HashEncoding {
    Hex,
    Base64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader as AsyncBufReader;

    async fn hash_hex_async(algo: HashAlgorithm, data: &[u8]) -> String {
        let digest = HashDigest::from_reader_with_algorithm(
            AsyncBufReader::new(data),
            algo,
            HashEncoding::Hex,
        )
        .await
        .unwrap();
        match digest {
            HashDigest::MD5(s, _)
            | HashDigest::SHA1(s, _)
            | HashDigest::SHA256(s, _)
            | HashDigest::SHA384(s, _)
            | HashDigest::SHA512(s, _) => s,
        }
    }

    #[tokio::test]
    async fn test_md5_hex_async() {
        let data = b"hello world";
        let hash = hash_hex_async(HashAlgorithm::MD5, data).await;
        assert_eq!(hash, "5eb63bbbe01eeed093cb22bb8f5acdc3");
    }

    #[tokio::test]
    async fn test_sha1_hex_async() {
        let data = b"hello world";
        let hash = hash_hex_async(HashAlgorithm::SHA1, data).await;
        assert_eq!(hash, "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed");
    }

    #[tokio::test]
    async fn test_sha256_hex_async() {
        let data = b"hello world";
        let hash = hash_hex_async(HashAlgorithm::SHA256, data).await;
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[tokio::test]
    async fn test_sha384_hex_async() {
        let data = b"hello world";
        let hash = hash_hex_async(HashAlgorithm::SHA384, data).await;
        assert_eq!(
            hash,
            "fdbd8e75a67f29f701a4e040385e2e23986303ea10239211af907fcbb83578b3e417cb71ce646efd0819dd8c088de1bd"
        );
    }

    #[tokio::test]
    async fn test_sha512_hex_async() {
        let data = b"hello world";
        let hash = hash_hex_async(HashAlgorithm::SHA512, data).await;
        assert_eq!(
            hash,
            "309ecc489c12d6eb4cc40f50c902f2b4d0ed77ee511a7c7a9bcd3ca86d4cd86f989dd35bc5ff499670da34255b45b0cfd830e81f605dcf7dc5542e93ae9cd76f"
        );
    }

    #[tokio::test]
    async fn test_md5_empty_async() {
        let data = b"";
        let hash = hash_hex_async(HashAlgorithm::MD5, data).await;
        assert_eq!(hash, "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[test]
    fn parse_cli_hex_sha256_lowercases() {
        let d = HashDigest::parse_cli(
            "sha256:B94D27B9934D3E08A52E52D7DA7DABFAC484EFE37A5380EE9088F7ACE2EFCDE9",
        )
        .unwrap();
        assert_eq!(
            d,
            HashDigest::SHA256(
                "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9".to_string(),
                HashEncoding::Hex,
            )
        );
    }

    #[test]
    fn parse_cli_explicit_encoding_and_algos() {
        assert_eq!(
            HashDigest::parse_cli("md5:hex:5eb63bbbe01eeed093cb22bb8f5acdc3").unwrap(),
            HashDigest::MD5(
                "5eb63bbbe01eeed093cb22bb8f5acdc3".to_string(),
                HashEncoding::Hex
            )
        );
        assert_eq!(
            HashDigest::parse_cli("sha256:base64:uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=")
                .unwrap(),
            HashDigest::SHA256(
                "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=".to_string(),
                HashEncoding::Base64,
            )
        );
    }

    #[test]
    fn parse_cli_rejects_bad_input() {
        assert!(HashDigest::parse_cli("sha256:nothex").is_err());
        assert!(HashDigest::parse_cli("sha256:abcd").is_err()); // wrong length
        assert!(HashDigest::parse_cli("bogus:deadbeef").is_err());
        assert!(HashDigest::parse_cli("sha256").is_err());
        assert!(HashDigest::parse_cli("sha256:").is_err());
        assert!(HashDigest::parse_cli("sha256:rot13:deadbeef").is_err());
    }

    #[tokio::test]
    async fn test_sha256_base64_async() {
        let data = b"hello world";
        let digest = HashDigest::from_reader_with_algorithm(
            AsyncBufReader::new(&data[..]),
            HashAlgorithm::SHA256,
            HashEncoding::Base64,
        )
        .await
        .unwrap();
        match digest {
            HashDigest::SHA256(s, HashEncoding::Base64) => {
                assert_eq!(s, "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=");
            }
            _ => panic!("Unexpected digest variant"),
        }
    }
}
