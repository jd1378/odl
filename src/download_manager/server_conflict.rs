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
    metadata.last_etag = new_download.etag().to_owned();
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
            disk_metadata.is_resumable = instruction.is_resumable();
            disk_metadata.filename = instruction.filename().to_string();
            disk_metadata.max_connections = instruction.max_connections();
            disk_metadata.requires_auth = instruction.requires_auth();
            disk_metadata.requires_basic_auth = instruction.requires_basic_auth();
            disk_metadata.use_server_time = instruction.use_server_time();
            disk_metadata.save_dir = instruction.save_dir().to_string_lossy().into_owned();
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
            check_final_file_checksum(&metadata, instruction, true).await;

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
        } else if metadata.last_etag != *instruction.etag()
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
