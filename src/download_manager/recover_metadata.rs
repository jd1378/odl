use std::path::PathBuf;

use crate::{
    Download,
    download_metadata::DownloadMetadata,
    error::OdlError,
    fs_utils::{atomic_replace, read_delimited_message_from_path},
};

/// Attempt to recover from an interrupted metadata write.
pub async fn recover_metadata(instruction: &Download) -> Result<(), OdlError> {
    let metadata_temp_path = instruction.metadata_temp_path();

    match read_delimited_message_from_path::<DownloadMetadata, PathBuf>(&metadata_temp_path).await {
        Ok(_) => {
            // If temp metadata is valid, atomically replace the main metadata file
            atomic_replace(metadata_temp_path, instruction.metadata_path()).await?;
        }
        Err(_) => {
            // If temp metadata is invalid or unreadable, remove it if it exists
            if tokio::fs::try_exists(&metadata_temp_path)
                .await
                .unwrap_or(false)
            {
                // successful removal is important at this point
                tokio::fs::remove_file(&metadata_temp_path).await?;
            }
        }
    }

    Ok(())
}
