use std::path::PathBuf;

use tokio::io::BufWriter;

use crate::{
    Download,
    download_manager::checksum::check_final_file_checksum,
    download_metadata::{DownloadMetadata, PartDetails},
    error::OdlError,
    fs_utils::set_file_mtime_async,
};

/// removes all .part files on disk
pub async fn remove_all_parts(download_dir: &PathBuf) {
    // Remove all .part files in the download directory
    // Effectively resetting the download progress
    if let Ok(mut entries) = tokio::fs::read_dir(&download_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == Download::PART_EXTENSION {
                    let _ = tokio::fs::remove_file(&path).await;
                }
            }
        }
    }
}

pub async fn assemble_final_file(
    metadata: &DownloadMetadata,
    instruction: &Download,
) -> Result<PathBuf, OdlError> {
    let final_path = instruction.final_file_path();
    let final_file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&final_path)
        .await?;
    let mut final_file = BufWriter::new(final_file);
    let mut sorted_parts: Vec<&PartDetails> = metadata.parts.values().collect();
    sorted_parts.sort_by_key(|p| p.offset);
    for p in sorted_parts.iter() {
        let part_path = instruction.part_path(&p.ulid);
        let mut part_file = tokio::fs::File::open(&part_path).await?;
        tokio::io::copy(&mut part_file, &mut final_file).await?;
    }

    if metadata.use_server_time {
        if let Some(last_modified) = metadata.last_modified {
            if let Err(e) = set_file_mtime_async(&final_path, last_modified).await {
                tracing::error!(
                    "Failed to set file mtime for {}: {}",
                    final_path.display(),
                    e
                );
            }
        }
    }

    check_final_file_checksum(metadata, instruction, false).await?;

    return Ok(final_path);
}

/// Sums the sizes of all part files on disk for a given instruction and metadata.
/// Returns None if metadata.size is None, otherwise returns the total size in bytes.
pub async fn sum_parts_on_disk(instruction: &Download, metadata: &DownloadMetadata) -> Option<u64> {
    metadata.size?;
    let part_futures = metadata.parts.values().map(|part| {
        let part_path = instruction.part_path(&part.ulid);
        async move {
            match tokio::fs::metadata(&part_path).await {
                Ok(meta) => meta.len(),
                Err(_) => 0,
            }
        }
    });
    let sizes = futures::future::join_all(part_futures).await;
    Some(sizes.into_iter().sum())
}
