use std::path::PathBuf;

use crate::{
    Download,
    conflict::{
        FileChangedResolution, NotResumableResolution, ServerConflict, ServerConflictResolver,
    },
    download_manager::{
        checksum::check_final_file_checksum,
        io::{persist_metadata, remove_all_parts},
    },
    download_metadata::{DownloadMetadata, FileChecksum},
    error::{ConflictError, OdlError},
    fs_utils::read_delimited_message_from_path,
};
use futures::future::join_all;

async fn apply_restart_state_to_metadata(
    metadata: &mut DownloadMetadata,
    new_download: &Download,
    new_checksums: Vec<FileChecksum>,
) {
    metadata.finished = false;
    metadata.last_etag = new_download.etag().map(str::to_owned);
    metadata.last_modified = new_download.last_modified();
    metadata.size = new_download.size();
    remove_all_parts(new_download.download_dir()).await;
    metadata.parts = Download::determine_parts(metadata.size, metadata.max_connections);
    metadata.checksums = new_checksums;
}

/// Checks for common conflicts between new instruction and metadata on disk
/// and attemps to resolve them before the download starts.
/// writes the updated metadata to disk and returns it
pub async fn resolve_server_conflicts<CR>(
    instruction: &Download,
    conflict_resolver: &CR,
    verify_contents: bool,
) -> Result<DownloadMetadata, OdlError>
where
    CR: ServerConflictResolver,
{
    let mut metadata: DownloadMetadata = match read_delimited_message_from_path::<
        DownloadMetadata,
        PathBuf,
    >(&instruction.metadata_path())
    .await
    {
        Ok(mut disk_metadata) => {
            // update disk_metadata from instruction
            //
            // `is_resumable` is the exception: on disk it can hold what the
            // server actually *did* — stopped honouring `Range` part-way
            // through — which outranks what its headers advertise, since the
            // instruction only ever repeats the advertisement. Once observed,
            // it stays false, and the restart below re-splits the download
            // into the single part such a server can serve.
            disk_metadata.is_resumable &= instruction.is_resumable();
            disk_metadata.filename = instruction.filename().to_string();
            disk_metadata.max_connections = if disk_metadata.is_resumable {
                instruction.max_connections()
            } else {
                1
            };
            disk_metadata.requires_auth = instruction.requires_auth();
            disk_metadata.requires_basic_auth = instruction.requires_basic_auth();
            disk_metadata.use_server_time = instruction.use_server_time();
            disk_metadata.save_dir = instruction.save_dir().to_string_lossy().into_owned();
            // Refresh the stored probe. Skipped when this instruction never
            // probed (`quick_evaluate`), so an older observation survives
            // rather than being replaced by nothing.
            if instruction.response_headers().is_some() {
                disk_metadata.response_headers = instruction.stored_response_headers();
                disk_metadata.response_headers_probed_at = instruction.response_headers_probed_at();
            }
            disk_metadata
        }
        Err(e) => {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(OdlError::StdIoError {
                    e,
                    extra_info: Some(format!(
                        "Failed to read metadata for download at {}",
                        instruction.metadata_path().display(),
                    )),
                });
            }
            instruction.as_metadata()
        }
    };

    let mut should_reset_state = false;
    if metadata.finished {
        let checksum_result: Result<(), OdlError> =
            check_final_file_checksum(&metadata, instruction, true, verify_contents, None).await;

        match checksum_result {
            Ok(_) => {
                // no need to do anything
            }
            Err(e) => {
                if let OdlError::StdIoError { e: io_err, .. } = e {
                    if io_err.kind() == std::io::ErrorKind::NotFound {
                        should_reset_state = true;
                    } else {
                        return Err(OdlError::StdIoError {
                            e: io_err,
                            extra_info: Some(format!(
                                "Failed to check final file checksum for {}",
                                instruction.final_file_path().display(),
                            )),
                        });
                    }
                } else if let OdlError::Conflict(ConflictError::ChecksumMismatch { .. }) = e {
                    should_reset_state = true;
                } else {
                    return Err(e);
                }
            }
        }
    }

    let new_checksums = instruction.as_metadata().checksums;
    if should_reset_state {
        apply_restart_state_to_metadata(&mut metadata, instruction, new_checksums).await
    } else if !metadata.finished {
        // Check if all finished parts actually exist on disk, otherwise mark them unfinished
        let finished_parts: Vec<_> = metadata
            .parts
            .iter()
            .filter(|(_, part)| part.finished)
            .map(|(_, part)| {
                let ulid = part.ulid.clone();
                let path = instruction.part_path(&ulid);
                async move {
                    let exists = tokio::fs::try_exists(&path).await.unwrap_or(false);
                    (ulid, exists)
                }
            })
            .collect();

        let results = join_all(finished_parts).await;
        for (ulid, exists) in results {
            if !exists && let Some(part) = metadata.parts.get_mut(&ulid) {
                part.finished = false;
            }
        }

        // Do possible corruption checks between new download instructions and the metadata on disk
        let mut conflict: Option<ServerConflict> = None;

        // Since resolution of either of issues is restarting the download, we just need to check one.
        if !metadata.is_resumable {
            conflict = Some(ServerConflict::NotResumable)
        } else if metadata.last_etag.as_deref() != instruction.etag()
            || metadata.last_modified != instruction.last_modified()
            || metadata.size != instruction.size()
            || metadata.checksums != new_checksums
        {
            conflict = Some(ServerConflict::FileChanged);
        }

        if let Some(conflict) = conflict {
            match conflict {
                ServerConflict::FileChanged => {
                    match conflict_resolver.resolve_file_changed(instruction).await {
                        FileChangedResolution::Abort => {
                            return Err(OdlError::Conflict(ConflictError::Server { conflict }));
                        }
                        FileChangedResolution::Restart => {
                            apply_restart_state_to_metadata(
                                &mut metadata,
                                instruction,
                                new_checksums,
                            )
                            .await
                        }
                    }
                }
                ServerConflict::NotResumable => {
                    match conflict_resolver.resolve_not_resumable(instruction).await {
                        NotResumableResolution::Abort => {
                            return Err(OdlError::Conflict(ConflictError::Server { conflict }));
                        }
                        NotResumableResolution::Restart => {
                            apply_restart_state_to_metadata(
                                &mut metadata,
                                instruction,
                                new_checksums,
                            )
                            .await
                        }
                    }
                }
                ServerConflict::UrlBroken | ServerConflict::CredentialsInvalid => {
                    return Err(OdlError::Conflict(ConflictError::Server { conflict }));
                }
            }
        }
    }

    // write metadata changes back to disk, if any
    persist_metadata(&metadata, instruction).await?;

    Ok(metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        download::DownloadBuilder, download_metadata::ResponseHeader,
        fs_utils::read_delimited_message_from_path,
    };
    use async_trait::async_trait;
    use reqwest::{
        Url,
        header::{HeaderMap, HeaderValue},
    };

    struct RestartResolver;
    #[async_trait]
    impl ServerConflictResolver for RestartResolver {
        async fn resolve_file_changed(&self, _: &Download) -> FileChangedResolution {
            FileChangedResolution::Restart
        }
        async fn resolve_not_resumable(&self, _: &Download) -> NotResumableResolution {
            NotResumableResolution::Restart
        }
    }

    /// Once a server has been seen ignoring `Range` mid-download, the record
    /// of that outranks the `accept-ranges` its headers keep advertising:
    /// a resume must not split the file up and ask for slices again.
    #[tokio::test]
    async fn an_observed_non_resumable_server_stays_non_resumable()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let tmp = tempfile::tempdir()?;
        let save_dir = tempfile::tempdir()?;

        let size = Some(Download::MIN_PART_SIZE * 8);
        let instruction = DownloadBuilder::default()
            .download_dir(tmp.path().to_path_buf())
            .save_dir(save_dir.path().to_path_buf())
            .url(Url::parse("http://example.invalid/file")?)
            .filename("file".to_string())
            .size(size)
            // What the probe advertises, which is what was disproved.
            .is_resumable(true)
            .max_connections(6)
            .parts(Download::determine_parts(size, 6))
            .build()?;

        let mut on_disk = instruction.as_metadata();
        on_disk.is_resumable = false;
        on_disk.max_connections = 1;
        on_disk.parts = Download::determine_parts(size, 1);
        persist_metadata(&on_disk, &instruction).await?;

        let resolved = resolve_server_conflicts(&instruction, &RestartResolver, true).await?;
        assert!(!resolved.is_resumable);
        assert_eq!(resolved.max_connections, 1);
        assert_eq!(resolved.parts.len(), 1);
        Ok(())
    }

    /// A resumed download must show the headers of the probe that just ran,
    /// not the ones stored when the download first started.
    #[tokio::test]
    async fn resume_refreshes_stored_response_headers()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let tmp = tempfile::tempdir()?;
        let save_dir = tempfile::tempdir()?;

        let mut fresh_headers = HeaderMap::new();
        fresh_headers.insert("x-cache", HeaderValue::from_static("HIT"));

        let instruction = DownloadBuilder::default()
            .download_dir(tmp.path().to_path_buf())
            .save_dir(save_dir.path().to_path_buf())
            .url(Url::parse("http://example.invalid/file")?)
            .filename("file".to_string())
            .size(Some(1024))
            .is_resumable(true)
            .max_connections(1)
            .parts(Download::determine_parts(Some(1024), 1))
            .response_headers(Some(fresh_headers))
            .response_headers_probed_at(Some(1_700_000_100))
            .build()?;

        // Seed disk with a metadata carrying an older probe.
        let mut on_disk = instruction.as_metadata();
        on_disk.response_headers = vec![ResponseHeader {
            name: "x-cache".to_string(),
            value: "MISS".to_string(),
        }];
        on_disk.response_headers_probed_at = Some(1_600_000_000);
        persist_metadata(&on_disk, &instruction).await?;

        resolve_server_conflicts(&instruction, &RestartResolver, true).await?;

        let written: DownloadMetadata =
            read_delimited_message_from_path(&instruction.metadata_path()).await?;
        assert_eq!(written.response_headers.len(), 1);
        assert_eq!(written.response_headers[0].value, "HIT");
        assert_eq!(written.response_headers_probed_at, Some(1_700_000_100));
        Ok(())
    }
}
