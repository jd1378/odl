use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use futures::io;
use prost::Message;
use reflink_copy::ReflinkBlockBuilder;

use crate::{
    Download,
    download_manager::checksum::check_final_file_checksum,
    download_metadata::{DownloadMetadata, PartDetails},
    error::OdlError,
    fs_utils::{atomic_write, set_file_mtime_async},
    progress::{DownloadContext, Phase, ProgressEvent, SAMPLE_INTERVAL},
};

const COPY_BUF_SIZE: usize = 1024 * 1024;

/// Synthetic ulid used for the assembly progress bar so consumers can
/// render it as a regular child / part bar.
pub const ASSEMBLY_ULID: &str = "_assemble";

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
    ctx: &DownloadContext,
) -> Result<PathBuf, OdlError> {
    let final_path = instruction.final_file_path();
    let mut sorted_parts: Vec<&PartDetails> = metadata.parts.values().collect();
    sorted_parts.sort_by_key(|p| p.offset);

    let total: u64 = sorted_parts.iter().map(|p| p.size).sum();
    ctx.emit(ProgressEvent::PhaseChanged(Phase::Assembling));
    ctx.emit(ProgressEvent::Progress {
        downloaded: 0,
        total: Some(total),
    });
    // Surface assembly as a child bar so consumers can show progress +
    // speed + ETA for it the same way they show download parts.
    ctx.emit(ProgressEvent::PartAdded {
        ulid: ASSEMBLY_ULID.to_string(),
        offset: 0,
        size: total,
    });

    let parts: Vec<(PathBuf, u64, u64)> = sorted_parts
        .iter()
        .map(|p| (instruction.part_path(&p.ulid), p.offset, p.size))
        .collect();
    // File length is determined by the highest end offset, not the sum.
    // Parts are contiguous from 0 today, so sum == end, but using `end`
    // keeps `set_len` correct if a future split ever leaves gaps.
    let final_end: u64 = sorted_parts.last().map(|p| p.offset + p.size).unwrap_or(0);
    let final_path_for_blocking = final_path.clone();
    let ctx_for_blocking = ctx.clone();

    // Shared counter updated by the blocking assembler and read by the
    // async sampler so we can emit raw, un-smoothed Speed/PartSpeed
    // events at a fixed cadence — same model as the download sampler.
    let done_counter = Arc::new(AtomicU64::new(0));
    let done_for_blocking = Arc::clone(&done_counter);
    let sampler_handle = spawn_assembly_sampler(ctx.clone(), Arc::clone(&done_counter), total);

    let blocking_result = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        assemble_blocking(
            &final_path_for_blocking,
            final_end,
            parts,
            done_for_blocking,
            ctx_for_blocking,
        )
    })
    .await;
    sampler_handle.abort();
    // Only land the bar at 100% on success. On failure / panic the
    // outer error path emits Failed; emitting PartFinished here would
    // briefly show "complete" before the failure event.
    let blocking_ok = matches!(&blocking_result, Ok(Ok(())));
    if blocking_ok {
        ctx.emit(ProgressEvent::PartProgress {
            ulid: ASSEMBLY_ULID.to_string(),
            downloaded: total,
            total,
        });
        ctx.emit(ProgressEvent::PartFinished {
            ulid: ASSEMBLY_ULID.to_string(),
        });
    }
    blocking_result??;

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

    ctx.emit(ProgressEvent::PhaseChanged(Phase::Verifying));
    check_final_file_checksum(metadata, instruction, false).await?;
    Ok(final_path)
}

fn spawn_assembly_sampler(
    ctx: DownloadContext,
    done: Arc<AtomicU64>,
    total: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_at = Instant::now();
        let mut last_bytes: u64 = 0;
        loop {
            tokio::select! {
                _ = ctx.cancel.cancelled() => return,
                _ = tokio::time::sleep(SAMPLE_INTERVAL) => {}
            }
            let now = Instant::now();
            let cur = done.load(Ordering::Relaxed);
            let dt = now.saturating_duration_since(last_at).as_secs_f64();
            if dt > 0.0 {
                let delta = cur.saturating_sub(last_bytes);
                let rate = delta as f64 / dt;
                ctx.emit(ProgressEvent::Speed {
                    bytes_per_second: rate,
                });
                ctx.emit(ProgressEvent::PartSpeed {
                    ulid: ASSEMBLY_ULID.to_string(),
                    bytes_per_second: rate,
                });
            }
            ctx.emit(ProgressEvent::Progress {
                downloaded: cur,
                total: Some(total),
            });
            ctx.emit(ProgressEvent::PartProgress {
                ulid: ASSEMBLY_ULID.to_string(),
                downloaded: cur,
                total,
            });
            last_at = now;
            last_bytes = cur;
        }
    })
}

fn assemble_blocking(
    final_path: &Path,
    final_end: u64,
    parts: Vec<(PathBuf, u64, u64)>,
    done: Arc<AtomicU64>,
    ctx: DownloadContext,
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
        // Windows FSCTL_DUPLICATE_EXTENTS_TO_FILE has no such tail exception:
        // every range must be cluster-aligned, so the last part falls back to
        // a byte copy when its size is unaligned.
        let aligned_offset = offset & cluster_mask == 0;
        let aligned_size = size & cluster_mask == 0;
        #[cfg(windows)]
        let tail_reflinkable = false;
        #[cfg(not(windows))]
        let tail_reflinkable = is_last;
        let reflinkable = !reflink_disabled && aligned_offset && (aligned_size || tail_reflinkable);

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
            done.fetch_add(size, Ordering::Relaxed);
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
            done.fetch_add(n as u64, Ordering::Relaxed);
        }
    }

    // sync_data() can stall multiple seconds on spinning disks / encrypted
    // FS when a copy-fallback dirtied the whole file. Emit Flushing phase
    // so the UI doesn't look frozen at 100%.
    ctx.emit(ProgressEvent::PhaseChanged(Phase::Flushing));
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
