use base64::{Engine as _, engine::general_purpose};
use digest::Digest;
use md5::Md5;
use sha1::Sha1;
use sha2::{Sha256, Sha384, Sha512};
use std::io::{self, Read};
use tokio::io::{self as async_io, AsyncRead, AsyncReadExt};

use crate::download_metadata::{ChecksumAlgorithm, ChecksumEncoding, FileChecksum};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HashDigest {
    SHA512(String, HashEncoding),
    SHA384(String, HashEncoding),
    SHA256(String, HashEncoding),
    SHA1(String, HashEncoding),
    MD5(String, HashEncoding),
}

impl HashDigest {
    /// Hashes the contents of a reader using the specified Digest type (sync).
    fn hash_reader<D: Digest + Default>(mut reader: impl Read) -> io::Result<D> {
        let mut hasher = D::default();
        let mut buf = [0u8; 8192];
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        Ok(hasher)
    }

    /// Hashes the contents of an async reader using the specified Digest type (async).
    async fn hash_async_reader<D: Digest + Default + Unpin>(
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

    /// Compute a hash from a reader using the specified algorithm and encoding (sync).
    pub fn from_reader_with_algorithm<R: Read>(
        reader: R,
        algo: HashAlgorithm,
        encoding: HashEncoding,
    ) -> io::Result<HashDigest> {
        match algo {
            HashAlgorithm::MD5 => {
                let hasher = Self::hash_reader::<Md5>(reader)?;
                let bytes = hasher.finalize();
                let s = match encoding {
                    HashEncoding::Hex => format!("{:x}", bytes),
                    HashEncoding::Base64 => general_purpose::STANDARD.encode(bytes),
                };
                Ok(HashDigest::MD5(s, encoding))
            }
            HashAlgorithm::SHA1 => {
                let hasher = Self::hash_reader::<Sha1>(reader)?;
                let bytes = hasher.finalize();
                let s = match encoding {
                    HashEncoding::Hex => format!("{:x}", bytes),
                    HashEncoding::Base64 => general_purpose::STANDARD.encode(bytes),
                };
                Ok(HashDigest::SHA1(s, encoding))
            }
            HashAlgorithm::SHA256 => {
                let hasher = Self::hash_reader::<Sha256>(reader)?;
                let bytes = hasher.finalize();
                let s = match encoding {
                    HashEncoding::Hex => format!("{:x}", bytes),
                    HashEncoding::Base64 => general_purpose::STANDARD.encode(bytes),
                };
                Ok(HashDigest::SHA256(s, encoding))
            }
            HashAlgorithm::SHA384 => {
                let hasher = Self::hash_reader::<Sha384>(reader)?;
                let bytes = hasher.finalize();
                let s = match encoding {
                    HashEncoding::Hex => format!("{:x}", bytes),
                    HashEncoding::Base64 => general_purpose::STANDARD.encode(bytes),
                };
                Ok(HashDigest::SHA384(s, encoding))
            }
            HashAlgorithm::SHA512 => {
                let hasher = Self::hash_reader::<Sha512>(reader)?;
                let bytes = hasher.finalize();
                let s = match encoding {
                    HashEncoding::Hex => format!("{:x}", bytes),
                    HashEncoding::Base64 => general_purpose::STANDARD.encode(bytes),
                };
                Ok(HashDigest::SHA512(s, encoding))
            }
        }
    }

    /// Compute a hash from an async reader using the specified algorithm and encoding (async).
    pub async fn from_async_reader_with_algorithm<R: AsyncRead + Unpin>(
        reader: R,
        algo: HashAlgorithm,
        encoding: HashEncoding,
    ) -> async_io::Result<HashDigest> {
        match algo {
            HashAlgorithm::MD5 => {
                let hasher = Self::hash_async_reader::<Md5>(reader).await?;
                let bytes = hasher.finalize();
                let s = match encoding {
                    HashEncoding::Hex => format!("{:x}", bytes),
                    HashEncoding::Base64 => general_purpose::STANDARD.encode(bytes),
                };
                Ok(HashDigest::MD5(s, encoding))
            }
            HashAlgorithm::SHA1 => {
                let hasher = Self::hash_async_reader::<Sha1>(reader).await?;
                let bytes = hasher.finalize();
                let s = match encoding {
                    HashEncoding::Hex => format!("{:x}", bytes),
                    HashEncoding::Base64 => general_purpose::STANDARD.encode(bytes),
                };
                Ok(HashDigest::SHA1(s, encoding))
            }
            HashAlgorithm::SHA256 => {
                let hasher = Self::hash_async_reader::<Sha256>(reader).await?;
                let bytes = hasher.finalize();
                let s = match encoding {
                    HashEncoding::Hex => format!("{:x}", bytes),
                    HashEncoding::Base64 => general_purpose::STANDARD.encode(bytes),
                };
                Ok(HashDigest::SHA256(s, encoding))
            }
            HashAlgorithm::SHA384 => {
                let hasher = Self::hash_async_reader::<Sha384>(reader).await?;
                let bytes = hasher.finalize();
                let s = match encoding {
                    HashEncoding::Hex => format!("{:x}", bytes),
                    HashEncoding::Base64 => general_purpose::STANDARD.encode(bytes),
                };
                Ok(HashDigest::SHA384(s, encoding))
            }
            HashAlgorithm::SHA512 => {
                let hasher = Self::hash_async_reader::<Sha512>(reader).await?;
                let bytes = hasher.finalize();
                let s = match encoding {
                    HashEncoding::Hex => format!("{:x}", bytes),
                    HashEncoding::Base64 => general_purpose::STANDARD.encode(bytes),
                };
                Ok(HashDigest::SHA512(s, encoding))
            }
        }
    }

    /// Compute a hash from a reader using the algorithm implied by the HashDigest variant and encoding (sync).
    pub fn from_reader<R: Read>(reader: R, hash_type: &HashDigest) -> io::Result<HashDigest> {
        let algo = HashAlgorithm::from(hash_type);
        let encoding = HashEncoding::from(hash_type);
        Self::from_reader_with_algorithm(reader, algo, encoding)
    }

    /// Compute a hash from an async reader using the algorithm implied by the HashDigest variant and encoding (async).
    pub async fn from_async_reader<R: AsyncRead + Unpin>(
        reader: R,
        hash_type: &HashDigest,
    ) -> async_io::Result<HashDigest> {
        let algo = HashAlgorithm::from(hash_type);
        let encoding = HashEncoding::from(hash_type);
        Self::from_async_reader_with_algorithm(reader, algo, encoding).await
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
    use std::io::Cursor;
    use tokio::io::BufReader as AsyncBufReader;

    fn hash_hex(algo: HashAlgorithm, data: &[u8]) -> String {
        let digest =
            HashDigest::from_reader_with_algorithm(Cursor::new(data), algo, HashEncoding::Hex)
                .unwrap();
        match digest {
            HashDigest::MD5(s, _)
            | HashDigest::SHA1(s, _)
            | HashDigest::SHA256(s, _)
            | HashDigest::SHA384(s, _)
            | HashDigest::SHA512(s, _) => s,
        }
    }

    #[test]
    fn test_md5_hex() {
        let data = b"hello world";
        let hash = hash_hex(HashAlgorithm::MD5, data);
        assert_eq!(hash, "5eb63bbbe01eeed093cb22bb8f5acdc3");
    }

    #[test]
    fn test_sha1_hex() {
        let data = b"hello world";
        let hash = hash_hex(HashAlgorithm::SHA1, data);
        assert_eq!(hash, "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed");
    }

    #[test]
    fn test_sha256_hex() {
        let data = b"hello world";
        let hash = hash_hex(HashAlgorithm::SHA256, data);
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_sha384_hex() {
        let data = b"hello world";
        let hash = hash_hex(HashAlgorithm::SHA384, data);
        assert_eq!(
            hash,
            "fdbd8e75a67f29f701a4e040385e2e23986303ea10239211af907fcbb83578b3e417cb71ce646efd0819dd8c088de1bd"
        );
    }

    #[test]
    fn test_sha512_hex() {
        let data = b"hello world";
        let hash = hash_hex(HashAlgorithm::SHA512, data);
        assert_eq!(
            hash,
            "309ecc489c12d6eb4cc40f50c902f2b4d0ed77ee511a7c7a9bcd3ca86d4cd86f989dd35bc5ff499670da34255b45b0cfd830e81f605dcf7dc5542e93ae9cd76f"
        );
    }

    #[test]
    fn test_md5_empty() {
        let data = b"";
        let hash = hash_hex(HashAlgorithm::MD5, data);
        assert_eq!(hash, "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[test]
    fn test_sha256_base64() {
        let data = b"hello world";
        let digest = HashDigest::from_reader_with_algorithm(
            Cursor::new(data),
            HashAlgorithm::SHA256,
            HashEncoding::Base64,
        )
        .unwrap();
        match digest {
            HashDigest::SHA256(s, HashEncoding::Base64) => {
                assert_eq!(s, "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=");
            }
            _ => panic!("Unexpected digest variant"),
        }
    }

    async fn hash_hex_async(algo: HashAlgorithm, data: &[u8]) -> String {
        let digest = HashDigest::from_async_reader_with_algorithm(
            AsyncBufReader::new(&data[..]),
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

    #[tokio::test]
    async fn test_sha256_base64_async() {
        let data = b"hello world";
        let digest = HashDigest::from_async_reader_with_algorithm(
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
