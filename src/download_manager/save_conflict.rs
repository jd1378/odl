use crate::{
    Download,
    conflict::{SameDownloadExistsResolution, SaveConflict, SaveConflictResolver},
    error::{ConflictError, OdlError},
    fs_utils::{IsUnique, is_filename_unique},
};

/// Checks for common storage conflicts before the download begins
pub async fn resolve_save_conflicts<CR>(
    mut instruction: Download,
    conflict_resolver: &CR,
) -> Result<Download, OdlError>
where
    CR: SaveConflictResolver,
{
    if tokio::fs::try_exists(instruction.download_dir())
        .await
        .unwrap_or(false)
    {
        let resolution: SameDownloadExistsResolution = SameDownloadExistsResolution::Resume;
        match resolution {
            SameDownloadExistsResolution::Abort => {
                return Err(OdlError::Conflict(ConflictError::Save {
                    conflict: SaveConflict::SameDownloadExists,
                }));
            }
            SameDownloadExistsResolution::AddNumberToNameAndContinue => {
                let result = is_filename_unique(&instruction.download_dir()).await?;
                if let IsUnique::SuggestedAlternative(filename) = result {
                    instruction.set_filename(filename);
                }
            }
            SameDownloadExistsResolution::Resume => {
                // do nothing and return
                return Ok(instruction);
            }
        }
    }

    let final_path = instruction.save_dir().join(instruction.filename());

    // This can still happen even if AddNumberToNameAndContinue solution is used previously
    // But it does not happen often in that case.
    if tokio::fs::try_exists(&final_path).await.unwrap_or(false) {
        match conflict_resolver.final_file_exists(&instruction).await {
            crate::conflict::FinalFileExistsResolution::Abort => {
                return Err(OdlError::Conflict(ConflictError::Save {
                    conflict: SaveConflict::FinalFileExists,
                }));
            }
            crate::conflict::FinalFileExistsResolution::ReplaceAndContinue => {
                // We try to safely remove files, so just in case that
                // the download_dir is not selected correctly, we don't end up
                // deleting the wrong files.
                let _ = tokio::fs::remove_file(instruction.metadata_path()).await;
                let _ = tokio::fs::remove_file(instruction.metadata_temp_path()).await;
                let mut entries = tokio::fs::read_dir(instruction.download_dir()).await?;
                while let Some(entry) = entries.next_entry().await? {
                    let path = entry.path();
                    if let Some(ext) = path.extension()
                        && ext == Download::PART_EXTENSION
                    {
                        tokio::fs::remove_file(&path).await?;
                    }
                }
            }
            crate::conflict::FinalFileExistsResolution::AddNumberToNameAndContinue => {
                if let IsUnique::SuggestedAlternative(new_name) =
                    is_filename_unique(&final_path).await?
                {
                    instruction.set_filename(new_name);
                }
            }
        }
    }

    Ok(instruction)
}
