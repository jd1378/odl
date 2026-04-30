use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use futures::io;
use prost::Message;
use reflink_copy::ReflinkBlockBuilder;
use tracing::info_span;
use tracing_indicatif::span_ext::IndicatifSpanExt;

use crate::{
    Download,
    download_manager::checksum::check_final_file_checksum,
    download_metadata::{DownloadMetadata, PartDetails},
    error::OdlError,
    fs_utils::{atomic_write, set_file_mtime_async},
};

const COPY_BUF_SIZE: usize = 1024 * 1024;

/// removes all .part files on disk
pub async fn remove_all_parts(download_dir: &Path) {
    // Remove all .part files in the download directory
    // Effectively resetting the download progress
    if let Ok(mut entries) = tokio::fs::read_dir(download_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if let Some(ext) = path.extension()
                && ext == Download::PART_EXTENSION
            {
                let _ = tokio::fs::remove_file(&path).await;
            }
        }
    }
}

pub async fn assemble_final_file(
    metadata: &DownloadMetadata,
    instruction: &Download,
) -> Result<PathBuf, OdlError> {
    let final_path = instruction.final_file_path();
    let mut sorted_parts: Vec<&PartDetails> = metadata.parts.values().collect();
    sorted_parts.sort_by_key(|p| p.offset);

    let total: u64 = sorted_parts.iter().map(|p| p.size).sum();
    let span = info_span!("assemble");
    span.pb_set_length(total);
    span.pb_set_position(0);
    span.pb_set_message("Assembling");
    span.pb_start();

    let parts: Vec<(PathBuf, u64, u64)> = sorted_parts
        .iter()
        .map(|p| (instruction.part_path(&p.ulid), p.offset, p.size))
        .collect();
    // File length is determined by the highest end offset, not the sum.
    // Parts are contiguous from 0 today, so sum == end, but using `end`
    // keeps `set_len` correct if a future split ever leaves gaps.
    let final_end: u64 = sorted_parts.last().map(|p| p.offset + p.size).unwrap_or(0);
    let final_path_for_blocking = final_path.clone();
    let span_for_blocking = span.clone();

    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        assemble_blocking(
            &final_path_for_blocking,
            final_end,
            parts,
            span_for_blocking,
        )
    })
    .await??;

    if metadata.use_server_time
        && let Some(last_modified) = metadata.last_modified
        && let Err(e) = set_file_mtime_async(&final_path, last_modified).await
    {
        tracing::error!(
            "Failed to set file mtime for {}: {}",
            final_path.display(),
            e
        );
    }

    check_final_file_checksum(metadata, instruction, false).await?;
    Ok(final_path)
}

fn assemble_blocking(
    final_path: &Path,
    final_end: u64,
    parts: Vec<(PathBuf, u64, u64)>,
    span: tracing::Span,
) -> std::io::Result<()> {
    use std::io::Read;

    let final_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(final_path)?;
    // Pre-size the file so reflink of an unaligned trailing part has a
    // valid destination range, and concurrent pwrites don't race extending.
    if final_end > 0 {
        final_file.set_len(final_end)?;
    }

    let cluster_size =
        NonZeroU64::new(Download::ASSEMBLY_CLUSTER_SIZE).expect("ASSEMBLY_CLUSTER_SIZE non-zero");
    let cluster_mask = Download::ASSEMBLY_CLUSTER_SIZE - 1;
    let mut reflink_disabled = false;
    let mut buf = vec![0u8; COPY_BUF_SIZE];

    let last_idx = parts.len().saturating_sub(1);
    for (idx, (part_path, offset, size)) in parts.into_iter().enumerate() {
        if size == 0 {
            continue;
        }
        let mut part_file = std::fs::File::open(&part_path)?;
        let is_last = idx == last_idx;

        // Reflink requires both endpoints on a cluster boundary, except the
        // very last part may have an unaligned tail (Linux ficlonerange allows
        // a final extent that does not exceed the source file length).
        let aligned_offset = offset & cluster_mask == 0;
        let aligned_size = size & cluster_mask == 0;
        let reflinkable = !reflink_disabled && aligned_offset && (aligned_size || is_last);

        let reflinked = if reflinkable && let Some(len_nz) = NonZeroU64::new(size) {
            let res = ReflinkBlockBuilder::new(&part_file, &final_file, len_nz)
                .from_offset(0)
                .to_offset(offset)
                .cluster_size(cluster_size)
                .reflink_block();
            match res {
                Ok(()) => true,
                Err(e) => {
                    // First failure disables reflink for the rest of the
                    // assembly: alignment was already verified, so the cause
                    // is filesystem-level (no reflink, cross-device, perms,
                    // or FS cluster size > ASSEMBLY_CLUSTER_SIZE as on ZFS).
                    tracing::debug!(error = %e, "reflink failed, falling back to copy");
                    reflink_disabled = true;
                    false
                }
            }
        } else {
            false
        };

        if reflinked {
            span.pb_inc(size);
            continue;
        }

        let mut write_offset = offset;
        let mut remaining = size;
        while remaining > 0 {
            let want = remaining.min(buf.len() as u64) as usize;
            let n = part_file.read(&mut buf[..want])?;
            if n == 0 {
                // Part file shorter than recorded size: treat as corruption
                // rather than silently leaving the zero-filled hole that
                // `set_len` created above.
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "part file {} shorter than recorded size ({} bytes missing)",
                        part_path.display(),
                        remaining
                    ),
                ));
            }
            pwrite_all(&final_file, &buf[..n], write_offset)?;
            write_offset += n as u64;
            remaining -= n as u64;
            span.pb_inc(n as u64);
        }
    }

    // sync_data() can stall multiple seconds on spinning disks / encrypted
    // FS when a copy-fallback dirtied the whole file. Update the bar message
    // so the UI doesn't look frozen at 100%.
    span.pb_set_message("Flushing data to disk");
    final_file.sync_data()?;
    Ok(())
}

#[cfg(unix)]
fn pwrite_all(file: &std::fs::File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buf, offset)
}

#[cfg(windows)]
fn pwrite_all(file: &std::fs::File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut written = 0;
    while written < buf.len() {
        let n = file.seek_write(&buf[written..], offset + written as u64)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to write whole buffer",
            ));
        }
        written += n;
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn pwrite_all(file: &std::fs::File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    // `&File` implements both `Seek` and `Write` in std, so rebinding to a
    // local `&mut &File` lets us call the trait methods without taking a
    // unique borrow of the caller's `&File`. Not atomic vs. concurrent
    // writers; the unix/windows branches above use positional I/O instead.
    let mut f = file;
    f.seek(SeekFrom::Start(offset))?;
    f.write_all(buf)
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

pub async fn persist_metadata(
    metadata: &DownloadMetadata,
    instruction: &Download,
) -> io::Result<()> {
    let encoded = metadata.encode_length_delimited_to_vec();
    persist_encoded_metadata(encoded.as_slice(), instruction).await
}

pub async fn persist_encoded_metadata(encoded: &[u8], instruction: &Download) -> io::Result<()> {
    atomic_write(
        instruction.metadata_path(),
        instruction.metadata_temp_path(),
        encoded,
    )
    .await
}
