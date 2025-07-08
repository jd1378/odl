use crate::{
    download_metadata::{ChecksumAlgorithm, ChecksumEncoding, FileChecksum},
    hash::{HashDigest, HashEncoding},
};

impl From<HashDigest> for FileChecksum {
    fn from(hash_digest: HashDigest) -> Self {
        let (algorithm, encoding, digest) = match hash_digest {
            HashDigest::SHA512(s, enc) => (ChecksumAlgorithm::Sha512, enc, s),
            HashDigest::SHA384(s, enc) => (ChecksumAlgorithm::Sha384, enc, s),
            HashDigest::SHA256(s, enc) => (ChecksumAlgorithm::Sha256, enc, s),
            HashDigest::SHA1(s, enc) => (ChecksumAlgorithm::Sha1, enc, s),
            HashDigest::MD5(s, enc) => (ChecksumAlgorithm::Md5, enc, s),
        };

        let encoding = match encoding {
            HashEncoding::Hex => ChecksumEncoding::Hex,
            HashEncoding::Base64 => ChecksumEncoding::Base64,
        };

        FileChecksum {
            algorithm: algorithm.into(),
            encoding: encoding.into(),
            digest,
        }
    }
}
