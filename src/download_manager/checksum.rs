use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{
    Download,
    download_manager::io::spawn_stage_sampler,
    download_metadata::DownloadMetadata,
    error::{ConflictError, MetadataError, OdlError},
    hash::HashDigest,
    progress::{DownloadContext, ProgressEvent, VERIFY_ULID},
};

/// can return OdlError::StdIoError of file not found kind
///
/// `ctx` is optional because two callers verify outside any download: the
/// crash-recovery check and the conflict resolver, neither of which has a
/// progress stream to report on.
pub async fn check_final_file_checksum(
    metadata: &DownloadMetadata,
    instruction: &Download,
    remove_if_empty_and_size_unknown: bool,
    verify_contents: bool,
    ctx: Option<&DownloadContext>,
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
    // The size check above always runs: it is one `stat`, and a truncated
    // file is worth catching whoever owns verification. Hashing the contents
    // is what a caller can opt out of.
    if verify_contents && !metadata.checksums.is_empty() {
        // Hashing gigabytes takes long enough to look like a hang, and it is
        // the one post-transfer stage that reported nothing at all: a consumer
        // could only park its bar at 100% and hope. Reported on its own row,
        // the same way assembly is. The row spans one pass over the file, so
        // with more than one checksum it restarts per algorithm — which is
        // what is actually happening.
        let reporting = ctx.filter(|_| actual_size > 0);
        if let Some(ctx) = reporting {
            ctx.emit(ProgressEvent::PartAdded {
                ulid: VERIFY_ULID.to_string(),
                offset: 0,
                size: actual_size,
            });
        }

        for checksum in &metadata.checksums {
            let expected = HashDigest::try_from(checksum).map_err(|e| {
                OdlError::MetadataError(MetadataError::Other {
                    message: format!("Invalid checksum in metadata: {}", e),
                })
            })?;

            let hashed = Arc::new(AtomicU64::new(0));
            let sampler = reporting.map(|ctx| {
                spawn_stage_sampler(ctx.clone(), VERIFY_ULID, Arc::clone(&hashed), actual_size)
            });
            let counter = Arc::clone(&hashed);

            let file = tokio::fs::File::open(&final_path).await?;
            let actual = HashDigest::from_reader_with_progress(
                file,
                expected.algorithm(),
                expected.encoding(),
                move |n| {
                    counter.fetch_add(n, Ordering::Relaxed);
                },
            )
            .await
            .map_err(|e| OdlError::StdIoError {
                e,
                extra_info: Some(format!(
                    "Failed to open file for calculating checksum at {}",
                    final_path.display(),
                )),
            });
            if let Some(sampler) = sampler {
                sampler.abort();
            }
            let actual = actual?;

            // Compared by value rather than by text: the two are computed in
            // the same encoding here, but saying so explicitly means a future
            // change of source cannot turn a good file into a mismatch.
            if !actual.matches(&expected) {
                // The row is left where it stopped rather than finished: this
                // file did not pass, and a bar landing at 100% would say the
                // opposite of the error that follows.
                return Err(OdlError::Conflict(ConflictError::ChecksumMismatch {
                    expected: format!("{:?}", expected),
                    actual: format!("{:?}", actual),
                }));
            }
        }

        if let Some(ctx) = reporting {
            ctx.emit(ProgressEvent::PartProgress {
                ulid: VERIFY_ULID.to_string(),
                downloaded: actual_size,
                total: actual_size,
            });
            ctx.emit(ProgressEvent::PartFinished {
                ulid: VERIFY_ULID.to_string(),
            });
        }
    }
    Ok(())
}
