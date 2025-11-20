use crate::{
    Download,
    download_metadata::DownloadMetadata,
    error::{ConflictError, MetadataError, OdlError},
    hash::HashDigest,
};

use tokio::io::BufReader;

/// can return OdlError::StdIoError of file not found kind
pub async fn check_final_file_checksum(
    metadata: &DownloadMetadata,
    instruction: &Download,
    remove_if_empty_and_size_unknown: bool,
) -> Result<(), OdlError> {
    let final_path = instruction.final_file_path();
    // do a simple size check first anyway, if we know that
    let actual_size = match tokio::fs::metadata(&final_path).await {
        Ok(meta) => meta.len(),
        Err(e) => {
            return Err(OdlError::StdIoError {
                e,
                extra_info: Some(format!(
                    "Failed to get file size for final file at {}",
                    final_path.display(),
                )),
            });
        }
    };
    if let Some(size) = metadata.size {
        if actual_size != size {
            return Err(OdlError::Conflict(ConflictError::ChecksumMismatch {
                expected: format!("size={}", size),
                actual: format!("size={}", actual_size),
            }));
        }
    } else if remove_if_empty_and_size_unknown && actual_size == 0 {
        let _ = tokio::fs::remove_file(&final_path).await;
        return Err(OdlError::Conflict(ConflictError::ChecksumMismatch {
            expected: "size=unknown".to_string(),
            actual: "size=0".to_string(),
        }));
    }
    if !metadata.checksums.is_empty() {
        for checksum in &metadata.checksums {
            let expected = HashDigest::try_from(checksum).map_err(|e| {
                OdlError::MetadataError(MetadataError::Other {
                    message: format!("Invalid checksum in metadata: {}", e),
                })
            })?;
            let file = tokio::fs::File::open(&final_path).await?;
            let reader = BufReader::new(file);
            let actual = HashDigest::from_reader(reader, &expected)
                .await
                .map_err(|e| OdlError::StdIoError {
                    e,
                    extra_info: Some(format!(
                        "Failed to open file for calculating checksum at {}",
                        final_path.display(),
                    )),
                })?;

            if actual != expected {
                return Err(OdlError::Conflict(ConflictError::ChecksumMismatch {
                    expected: format!("{:?}", expected),
                    actual: format!("{:?}", actual),
                }));
            }
        }
    }
    Ok(())
}
