use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::io;
use prost::Message;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tracing::{Span, info_span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

use crate::{
    Download,
    download_manager::checksum::check_final_file_checksum,
    download_metadata::{DownloadMetadata, PartDetails},
    error::OdlError,
    fs_utils::{atomic_write, set_file_mtime_async},
};

/// removes all .part files on disk
pub async fn remove_all_parts(download_dir: &PathBuf) {
    // Remove all .part files in the download directory
    // Effectively resetting the download progress
    if let Ok(mut entries) = tokio::fs::read_dir(&download_dir).await {
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
    let final_file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&final_path)
        .await?;
    let mut final_file = BufWriter::new(final_file);
    let mut sorted_parts: Vec<&PartDetails> = metadata.parts.values().collect();
    sorted_parts.sort_by_key(|p| p.offset);

    let total: u64 = sorted_parts.iter().map(|p| p.size).sum();
    let span = info_span!("assemble");
    span.pb_set_length(total);
    span.pb_set_position(0);
    span.pb_set_message("Assembling");
    span.pb_start();

    let mut progress_writer = ProgressWriter {
        inner: &mut final_file,
        span: &span,
    };
    for p in sorted_parts.iter() {
        let part_path = instruction.part_path(&p.ulid);
        let mut part_file = tokio::fs::File::open(&part_path).await?;
        tokio::io::copy(&mut part_file, &mut progress_writer).await?;
    }
    final_file.flush().await?;

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

/// AsyncWrite adapter that increments a tracing-indicatif progress bar by
/// the number of bytes successfully written on each `poll_write`.
struct ProgressWriter<'a, W> {
    inner: &'a mut W,
    span: &'a Span,
}

impl<W: AsyncWrite + Unpin> AsyncWrite for ProgressWriter<'_, W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut *this.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &poll {
            this.span.pb_inc(*n as u64);
        }
        poll
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut *this.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut *this.inner).poll_shutdown(cx)
    }
}
