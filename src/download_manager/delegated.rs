//! The manager's side of engine delegation.
//!
//! Everything specific to an external downloader lives behind
//! [`crate::ytdlp`]; this module is the seam where the manager decides to use
//! it and then treats the result like any other download — same metadata,
//! same conflict resolvers, same final move into the save directory.

use std::path::{Path, PathBuf};

use crate::config::{Config, DownloadOptions};
use crate::conflict::{SaveConflictResolver, ServerConflictResolver};
use crate::download::Download;
use crate::engine::{Engine, EnginePreference};
use crate::error::OdlError;
use crate::format::FormatSelector;
use crate::progress::DownloadContext;
use reqwest::Url;

/// Everything the delegation seam needs to decide on and prepare a download.
///
/// Most fields go unread in builds without a delegating engine, where the
/// seam only has to refuse an explicit request for one.
#[cfg_attr(not(feature = "ytdlp"), allow(dead_code))]
pub(super) struct DelegateInputs<'a, CR: SaveConflictResolver> {
    pub config: &'a Config,
    pub url: &'a Url,
    pub save_dir: &'a Path,
    pub conflict_resolver: &'a CR,
    pub ctx: &'a DownloadContext,
    pub opts: &'a DownloadOptions,
    pub preference: EnginePreference,
    pub selector: Option<&'a dyn FormatSelector>,
    /// Ask the selector again even when a format is already pinned.
    ///
    /// A download in progress keeps its format so a partial file is never
    /// continued in another encoding. Set this when the caller's intent is
    /// specifically to change quality: a different choice then reads as a
    /// conflict, which the [`ServerConflictResolver`] resolves by discarding
    /// and starting over. Changing quality always means starting over.
    pub reselect_format: bool,
}

#[cfg(feature = "ytdlp")]
mod imp {
    use super::*;
    use crate::conflict::{FileChangedResolution, ServerConflict};
    use crate::download::YtdlpSpec;
    use crate::download_manager::io::persist_metadata;
    use crate::download_manager::save_conflict::resolve_save_conflicts;
    use crate::download_metadata::EngineDetails;
    use crate::download_metadata::{DownloadMetadata, YtdlpDetails};
    use crate::error::{ConflictError, YtdlpError};
    use crate::format::DefaultFormatSelector;
    use crate::fs_utils::read_delimited_message_from_path;
    use crate::progress::{Phase, ProgressEvent};
    use reqwest::{Proxy, header::HeaderMap};

    /// Whether a failure is worth a fresh process.
    ///
    /// Only a non-zero exit from the tool qualifies: by then it has already
    /// exhausted its own retries, so what is left to try is a new extraction.
    /// It resumes from its own partial file, so this continues rather than
    /// starting the transfer over.
    ///
    /// Everything else is settled — an unsupported URL, a vanished format, a
    /// missing or too-old binary — and repeating it would only spend the
    /// user's time to reach the same answer. A rate-limited refusal is
    /// deliberately excluded too: the configured backoff is measured in
    /// seconds, far too short to clear a limit that is usually minutes or
    /// hours, and retrying into it risks extending the block. That one
    /// surfaces as a retryable exit code so the *caller* can come back later.
    fn is_worth_retrying(e: &OdlError) -> bool {
        matches!(e, OdlError::Ytdlp(YtdlpError::ProcessFailed { .. }))
    }

    /// How many times a failed *download* is started afresh.
    ///
    /// The transfer's own retries belong to yt-dlp, which repeats a request
    /// against the URL it already holds — no re-extraction, no second process,
    /// no extra call on the site's metadata API. Re-running the whole thing
    /// costs a fresh extraction (measured at roughly three seconds) and is
    /// worth it only for what an internal retry cannot fix: a media URL that
    /// expired mid-download, or the tool dying outright.
    ///
    /// So: one attempt, not `max_retries` of them. Scaling this with the
    /// configured number would multiply against the retries yt-dlp is already
    /// doing, turning "three tries" into sixteen.
    const RESPAWN_ATTEMPTS: u32 = 1;

    /// Run `op`, retrying up to `budget` times on the configured backoff.
    ///
    /// The budget is per phase rather than global, because the two phases have
    /// different owners: odl retries extraction itself, while the tool retries
    /// the transfer and odl only restarts it.
    async fn with_retries<T, F, Fut>(
        opts: &DownloadOptions,
        ctx: &DownloadContext,
        budget: u32,
        mut op: F,
    ) -> Result<T, OdlError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, OdlError>>,
    {
        // A user who asked for no retries gets none at any level.
        let budget = if opts.max_retries() == 0 { 0 } else { budget };
        let policy = crate::retry_policies::FixedThenExponentialRetry {
            max_n_retries: budget,
            wait_time: opts.wait_between_retries(),
            n_fixed_retries: opts.n_fixed_retries().max(1),
        };
        let mut attempts: u32 = 0;
        loop {
            match op().await {
                Ok(value) => return Ok(value),
                Err(e) => {
                    if !is_worth_retrying(&e) {
                        return Err(e);
                    }
                    attempts = attempts.saturating_add(1);
                    // Also the cancellation check: a user who stopped the
                    // download should not wait out a backoff first.
                    if !crate::retry_policies::wait_for_retry(&policy, attempts, ctx).await {
                        return Err(e);
                    }
                    tracing::info!(attempt = attempts, error = %e, "retrying yt-dlp");
                    ctx.emit(ProgressEvent::Message(format!(
                        "Retrying ({attempts}/{budget})…"
                    )));
                }
            }
        }
    }

    /// Metadata already on disk for this download, if any is readable.
    async fn existing_metadata(dir: &Path) -> Option<DownloadMetadata> {
        read_delimited_message_from_path::<DownloadMetadata, PathBuf>(&dir.join("metadata.pb"))
            .await
            .ok()
    }

    /// Details of a stored delegated download, when the metadata describes one.
    fn stored_ytdlp(
        metadata: &DownloadMetadata,
    ) -> Option<&crate::download_metadata::YtdlpDetails> {
        use crate::download_metadata::EngineDetails;
        match metadata.engine_details.as_ref()? {
            EngineDetails::YtdlpDetails(d) => Some(d),
        }
    }

    use crate::ytdlp::{
        self,
        extract::{self, ExtractedInfo},
        run::{self, DownloadPlan},
    };

    pub use run::bytes_on_disk;

    /// Which engine an evaluation would end up using.
    ///
    /// Shares [`resolve_tools`] with the real path so the preview cannot drift
    /// from the decision. A forced-but-unusable engine reports as forced: the
    /// caller asked for it, and evaluating would raise the error rather than
    /// quietly substituting.
    pub async fn planned_engine(
        config: &Config,
        url: &Url,
        preference: EnginePreference,
    ) -> Engine {
        match resolve_tools(url, preference, config).await {
            Ok(Some(_)) => Engine::Ytdlp,
            Ok(None) => Engine::HttpMultipart,
            Err(_) => preference.forced().unwrap_or(Engine::HttpMultipart),
        }
    }

    /// Decide whether this URL is delegated, and to what.
    ///
    /// `Auto` never fails because of the external tool: a missing or unusable
    /// yt-dlp simply means the URL is downloaded over HTTP as before. An
    /// explicitly requested engine does fail, because silently doing
    /// something else is not what was asked for.
    async fn resolve_tools(
        url: &Url,
        preference: EnginePreference,
        config: &Config,
    ) -> Result<Option<ytdlp::Tools>, OdlError> {
        let opts = config.ytdlp();
        match preference.forced() {
            Some(Engine::Ytdlp) => return Ok(Some(ytdlp::tools(opts).await?)),
            Some(Engine::HttpMultipart) => return Ok(None),
            // Asking to download with the engine that means "not yet decided"
            // is a contradiction. Quietly substituting HTTP would download
            // something, which is worse than saying the request makes no sense.
            Some(other) => {
                return Err(OdlError::InvalidRequest {
                    message: format!(
                        "`{}` is not an engine a download can be performed with",
                        other.as_str()
                    ),
                });
            }
            None => {}
        }

        if !ytdlp::should_delegate(url, opts) {
            return Ok(None);
        }
        match ytdlp::tools(opts).await {
            Ok(tools) => Ok(Some(tools)),
            Err(e) => {
                tracing::info!(error = %e, "yt-dlp unavailable; downloading over HTTP instead");
                Ok(None)
            }
        }
    }

    /// Format to request, and the container it produces.
    ///
    /// A download that already exists on disk keeps its stored format: the
    /// partial data was written in that encoding, and re-deciding here is
    /// what would make a resume append incompatible bytes.
    async fn choose_format(
        info: &ExtractedInfo,
        download_root: &Path,
        can_merge: bool,
        selector: Option<&dyn FormatSelector>,
        reselect: bool,
        opts: &DownloadOptions,
    ) -> Result<String, OdlError> {
        let dir = download_root.join(crate::fs_utils::cleanup_filename(
            &info.title,
            opts.ascii_filenames(),
        ));
        if !reselect
            && let Some(metadata) = existing_metadata(&dir).await
            && let Some(stored) = stored_ytdlp(&metadata)
            && same_media(
                stored,
                &info.extractor,
                info.id.as_deref().unwrap_or_default(),
                info.source_url.as_str(),
            )
            && !stored.format_id.is_empty()
        {
            tracing::debug!(
                format_id = stored.format_id,
                "resuming with the previously chosen format"
            );
            return Ok(stored.format_id.clone());
        }

        let offer = info.offer(can_merge);
        let selector = selector.unwrap_or(&DefaultFormatSelector);
        selector
            .select(&offer)
            .await
            // A selector declining is a deliberate "no", not a failure.
            .ok_or(OdlError::Cancelled)
    }

    pub async fn try_evaluate<CR>(
        input: DelegateInputs<'_, CR>,
    ) -> Result<Option<Download>, OdlError>
    where
        CR: SaveConflictResolver,
    {
        let DelegateInputs {
            config,
            url,
            save_dir,
            conflict_resolver,
            ctx,
            opts,
            preference,
            selector,
            reselect_format,
        } = input;
        let Some(tools) = resolve_tools(url, preference, config).await? else {
            return Ok(None);
        };

        let ytdlp_opts = config.ytdlp();
        let can_merge = tools.ffmpeg.is_some();
        if !can_merge {
            tracing::info!(
                "ffmpeg not found: limited to formats that need no muxing, which caps quality on some sites"
            );
        }

        let selector_expr = ytdlp_opts
            .format()
            .unwrap_or_else(|| extract::default_selector(can_merge));

        // Extraction is odl's to retry: the tool was told not to, so each
        // failure is counted, reported and interruptible here.
        let info = with_retries(opts, ctx, opts.max_retries(), || async {
            extract::extract(url, ytdlp_opts, &tools, opts.proxy(), selector_expr)
                .await
                .map_err(OdlError::from)
        })
        .await?;
        if ctx.is_cancelled() {
            return Err(OdlError::Cancelled);
        }

        let format_id = choose_format(
            &info,
            config.download_dir(),
            can_merge,
            selector,
            reselect_format,
            opts,
        )
        .await?;
        let (size, size_is_approx) = info.size_for(&format_id);
        let quality = info.quality_for(&format_id);

        let instruction = Download::from_ytdlp(
            config.download_dir(),
            save_dir.to_path_buf(),
            YtdlpSpec {
                source_url: info.source_url.clone(),
                title: info.title.clone(),
                extractor: info.extractor.clone(),
                video_id: info.id.clone(),
                ext: info.ext_for(&format_id),
                format_id,
                size,
                size_is_approx,
                quality,
                use_server_time: opts.use_server_time(),
                ascii_filenames: opts.ascii_filenames(),
                proxy: Option::<Proxy>::from(opts),
                headers: Some(HeaderMap::from(opts)),
            },
        );

        ctx.emit(ProgressEvent::PhaseChanged(Phase::ResolvingConflicts));
        let instruction = resolve_save_conflicts(instruction, conflict_resolver).await?;

        ctx.emit(ProgressEvent::FilenameResolved(
            instruction.filename().to_string(),
        ));
        ctx.emit(ProgressEvent::Progress {
            downloaded: 0,
            total: instruction.size(),
        });

        Ok(Some(instruction))
    }

    /// Whether two sets of engine details describe the same media item.
    ///
    /// Prefers the extractor's own id, because the same video has many URL
    /// spellings — `youtu.be/X`, `watch?v=X`, `&t=30` — and comparing those as
    /// strings reports a conflict where the partial data is perfectly valid.
    /// Falls back to the URL when either side carries no id: downloads stored
    /// before the field existed have none, and neither do extractors that
    /// report none, so an empty id means "unknown", never "matches".
    fn same_media(
        stored: &YtdlpDetails,
        extractor: &str,
        video_id: &str,
        source_url: &str,
    ) -> bool {
        if stored.video_id.is_empty() || video_id.is_empty() {
            return stored.source_url == source_url;
        }
        stored.video_id == video_id && stored.extractor == extractor
    }

    /// Why a stored download cannot simply be continued.
    pub(crate) fn continuation_blocker(
        metadata: &DownloadMetadata,
        instruction: &Download,
    ) -> Option<&'static str> {
        if Engine::from(metadata.engine()) != Engine::Ytdlp {
            return Some("the stored download used a different engine");
        }
        let (Some(stored), Some(wanted)) = (stored_ytdlp(metadata), instruction.ytdlp_details())
        else {
            return Some("the stored download is missing its engine details");
        };
        if !same_media(
            stored,
            &wanted.extractor,
            &wanted.video_id,
            &wanted.source_url,
        ) {
            // Two different pages that happen to share a title land in the
            // same directory; continuing would splice one into the other.
            return Some("the stored download came from a different URL");
        }
        if stored.format_id != wanted.format_id {
            // Appending a different encoding to an existing partial produces
            // a corrupt file that no exit code would flag.
            return Some("a different format was requested");
        }
        None
    }

    pub async fn process<CR>(
        config: &Config,
        instruction: Download,
        conflict_resolver: &CR,
        ctx: &DownloadContext,
        opts: &DownloadOptions,
    ) -> Result<PathBuf, OdlError>
    where
        CR: ServerConflictResolver,
    {
        tokio::fs::create_dir_all(instruction.save_dir()).await?;

        ctx.emit(ProgressEvent::PhaseChanged(Phase::ResolvingConflicts));
        let mut metadata = match existing_metadata(instruction.download_dir()).await {
            Some(stored) => match continuation_blocker(&stored, &instruction) {
                None => {
                    // Same URL, same format: adopt the stored state and keep
                    // whatever bytes are already on disk.
                    let mut m = stored;
                    m.save_dir = instruction.save_dir().to_string_lossy().into_owned();
                    m.filename = instruction.filename().to_string();
                    m
                }
                Some(reason) => {
                    tracing::info!(reason, "existing download cannot be continued");
                    match conflict_resolver.resolve_file_changed(&instruction).await {
                        FileChangedResolution::Abort => {
                            return Err(OdlError::Conflict(ConflictError::Server {
                                conflict: ServerConflict::FileChanged,
                            }));
                        }
                        FileChangedResolution::Restart => {
                            run::discard_payload(instruction.download_dir()).await?;
                            instruction.as_metadata()
                        }
                    }
                }
            },
            None => instruction.as_metadata(),
        };

        let stem = metadata
            .parts
            .keys()
            .next()
            .cloned()
            // Metadata always carries one synthetic part, but a hand-edited
            // file might not; a fresh name is better than failing.
            .unwrap_or_else(|| ulid::Ulid::generate().to_string());

        let final_path = instruction.final_file_path();
        if metadata.finished && tokio::fs::try_exists(&final_path).await.unwrap_or(false) {
            ctx.emit(ProgressEvent::Completed {
                path: final_path.clone(),
                already_complete: true,
            });
            return Ok(final_path);
        }

        metadata.finished = false;
        persist_metadata(&metadata, &instruction).await?;

        ctx.emit(ProgressEvent::Progress {
            downloaded: run::bytes_on_disk(instruction.download_dir()).await,
            total: metadata.size,
        });

        let tools = crate::ytdlp::tools(config.ytdlp()).await?;
        let ytdlp_opts = config.ytdlp();
        let source_url =
            Url::parse(instruction.url().as_ref()).map_err(|e| OdlError::UrlDecodeError {
                message: e.to_string(),
            })?;

        let plan = DownloadPlan {
            source_url: &source_url,
            format_id: &stored_ytdlp(&metadata)
                .map(|d| d.format_id.clone())
                .unwrap_or_default(),
            download_dir: instruction.download_dir(),
            stem: &stem,
            total_size: metadata.size,
            use_server_time: instruction.use_server_time(),
            proxy: opts.proxy(),
            speed_limit: opts.speed_limit(),
            headers: instruction.headers().as_ref(),
            concurrent_fragments: opts.max_connections(),
            max_retries: opts.max_retries(),
            wait_between_retries: opts.wait_between_retries(),
        };

        let produced = match with_retries(opts, ctx, RESPAWN_ATTEMPTS, || {
            run::run_download(&plan, ytdlp_opts, &tools, ctx)
        })
        .await
        {
            Ok(path) => path,
            Err(OdlError::Ytdlp(YtdlpError::FormatUnavailable { format_id })) => {
                // The bytes on disk are of an encoding the site no longer
                // serves, so nothing can continue them. Drop them and clear
                // the pin, which makes the next run select afresh.
                //
                // Deliberately not re-selecting here: doing so would silently
                // hand back a different quality than the one that was chosen,
                // and on a non-interactive run there is nobody to ask.
                tracing::info!(
                    format_id,
                    "pinned format is gone; discarding the partial download"
                );
                run::discard_payload(instruction.download_dir()).await?;
                if let Some(details) = metadata.engine_details.as_mut() {
                    let EngineDetails::YtdlpDetails(d) = details;
                    d.format_id.clear();
                }
                metadata.size = None;
                persist_metadata(&metadata, &instruction).await?;
                return Err(YtdlpError::FormatUnavailable { format_id }.into());
            }
            Err(e) => return Err(e),
        };

        ctx.emit(ProgressEvent::PhaseChanged(Phase::Assembling));
        move_into_place(&produced, &final_path).await?;

        // The real size is only known now: an estimate was all the extractor
        // could offer for adaptive formats. Measuring the finished file also
        // retires the estimate, so nothing downstream keeps flagging an exact
        // figure as approximate.
        if let Ok(meta) = tokio::fs::metadata(&final_path).await {
            metadata.size = Some(meta.len());
            if let Some(EngineDetails::YtdlpDetails(d)) = metadata.engine_details.as_mut() {
                d.size_is_approx = false;
            }
            for part in metadata.parts.values_mut() {
                part.size = meta.len();
                part.finished = true;
            }
        }
        metadata.finished = true;
        persist_metadata(&metadata, &instruction).await?;

        ctx.emit(ProgressEvent::Completed {
            path: final_path.clone(),
            already_complete: false,
        });
        Ok(final_path)
    }

    /// Move the produced file to its final home, falling back to a copy when
    /// the download and save directories are on different filesystems.
    async fn move_into_place(from: &Path, to: &Path) -> Result<(), OdlError> {
        if tokio::fs::rename(from, to).await.is_ok() {
            return Ok(());
        }
        let (from, to) = (from.to_path_buf(), to.to_path_buf());
        let copy_from = from.clone();
        tokio::task::spawn_blocking(move || reflink_copy::reflink_or_copy(&copy_from, &to))
            .await
            .map_err(|e| OdlError::Other {
                message: format!("failed to finalize download: {e}"),
                origin: Box::new(e),
            })?
            .map_err(|e| OdlError::StdIoError {
                e,
                extra_info: Some("Failed to move the downloaded file into place".to_owned()),
            })?;
        let _ = tokio::fs::remove_file(&from).await;
        Ok(())
    }
}

#[cfg(not(feature = "ytdlp"))]
mod imp {
    use super::*;

    /// Without a delegating engine there are no foreign files to count.
    pub async fn bytes_on_disk(_dir: &Path) -> u64 {
        0
    }

    /// Only one engine exists in this build.
    pub async fn planned_engine(
        _config: &Config,
        _url: &Url,
        _preference: EnginePreference,
    ) -> Engine {
        Engine::HttpMultipart
    }

    pub async fn try_evaluate<CR>(
        input: DelegateInputs<'_, CR>,
    ) -> Result<Option<Download>, OdlError>
    where
        CR: SaveConflictResolver,
    {
        let _ = input.reselect_format;
        if input.preference.forced() == Some(Engine::Ytdlp) {
            return Err(OdlError::CliError {
                message: "this build of odl was compiled without yt-dlp support".to_owned(),
            });
        }
        Ok(None)
    }

    pub async fn process<CR>(
        _config: &Config,
        instruction: Download,
        _conflict_resolver: &CR,
        _ctx: &DownloadContext,
        _opts: &DownloadOptions,
    ) -> Result<PathBuf, OdlError>
    where
        CR: ServerConflictResolver,
    {
        // Reachable only from metadata written by a build that had the
        // feature; refusing beats pretending the HTTP engine can take over.
        let _ = instruction;
        Err(OdlError::CliError {
            message: "this download needs yt-dlp support, which this build of odl lacks".to_owned(),
        })
    }
}

pub(super) use imp::{bytes_on_disk, planned_engine, process, try_evaluate};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::download::YtdlpSpec;
    use crate::download_metadata::{DownloadMetadata, EngineDetails, YtdlpDetails};
    use crate::format::Quality;

    fn instruction(source: &str, format: &str) -> Download {
        instruction_with_id(source, format, None)
    }

    fn instruction_with_id(source: &str, format: &str, video_id: Option<&str>) -> Download {
        Download::from_ytdlp(
            Path::new("/tmp/dl"),
            PathBuf::from("/tmp/save"),
            YtdlpSpec {
                source_url: Url::parse(source).unwrap(),
                title: "Title".to_owned(),
                extractor: "youtube".to_owned(),
                video_id: video_id.map(str::to_owned),
                format_id: format.to_owned(),
                ext: "mp4".to_owned(),
                size: Some(100),
                size_is_approx: true,
                quality: Quality::Video {
                    height: 720,
                    fps: None,
                },
                use_server_time: false,
                ascii_filenames: false,
                proxy: None,
                headers: None,
            },
        )
    }

    #[test]
    fn directory_is_keyed_on_title_so_a_quality_change_still_resumes() {
        let a = instruction("https://youtu.be/x", "137+251");
        let b = Download::from_ytdlp(
            Path::new("/tmp/dl"),
            PathBuf::from("/tmp/save"),
            YtdlpSpec {
                ext: "webm".to_owned(),
                format_id: "248+251".to_owned(),
                ..YtdlpSpec {
                    source_url: Url::parse("https://youtu.be/x").unwrap(),
                    title: "Title".to_owned(),
                    extractor: "youtube".to_owned(),
                    video_id: None,
                    format_id: String::new(),
                    ext: String::new(),
                    size: None,
                    size_is_approx: false,
                    quality: Quality::Video {
                        height: 720,
                        fps: None,
                    },
                    use_server_time: false,
                    ascii_filenames: false,
                    proxy: None,
                    headers: None,
                }
            },
        );
        assert_eq!(a.download_dir(), b.download_dir());
        // The filename still tracks the container that was chosen.
        assert_ne!(a.filename(), b.filename());
    }

    #[cfg(feature = "ytdlp")]
    mod continuation {
        use super::*;

        fn metadata_for(source: &str, format: &str) -> DownloadMetadata {
            metadata_with_id(source, format, "")
        }

        /// Metadata as an older odl wrote it, or a newer one with an id.
        fn metadata_with_id(source: &str, format: &str, video_id: &str) -> DownloadMetadata {
            let mut m = instruction(source, format).as_metadata();
            m.engine_details = Some(EngineDetails::YtdlpDetails(YtdlpDetails {
                source_url: source.to_owned(),
                video_id: video_id.to_owned(),
                format_id: format.to_owned(),
                extractor: "youtube".to_owned(),
                title: "Title".to_owned(),
                size_is_approx: true,
                height: Some(720),
                fps: None,
                bitrate_kbps: None,
                audio_only: false,
            }));
            m
        }

        #[test]
        fn same_url_and_format_continues() {
            let m = metadata_for("https://youtu.be/x", "137+251");
            let i = instruction("https://youtu.be/x", "137+251");
            assert!(super::super::imp::continuation_blocker(&m, &i).is_none());
        }

        #[test]
        fn a_different_url_in_the_same_directory_blocks_continuation() {
            // Two videos with the same title share a directory; splicing one
            // into the other would produce a file that is neither.
            let m = metadata_for("https://youtu.be/x", "137+251");
            let i = instruction("https://youtu.be/y", "137+251");
            assert!(super::super::imp::continuation_blocker(&m, &i).is_some());
        }

        #[test]
        fn the_same_video_under_a_different_url_still_continues() {
            // A share link, a watch link and a timestamped link are one
            // video. Comparing the URL as a string reported a conflict and
            // made the user start over on data that was perfectly valid.
            let m = metadata_with_id("https://youtu.be/x", "137+251", "x");
            let i = instruction_with_id(
                "https://www.youtube.com/watch?v=x&t=30",
                "137+251",
                Some("x"),
            );
            assert!(super::super::imp::continuation_blocker(&m, &i).is_none());
        }

        #[test]
        fn a_different_video_id_blocks_continuation() {
            let m = metadata_with_id("https://youtu.be/x", "137+251", "x");
            let i = instruction_with_id("https://youtu.be/x", "137+251", Some("y"));
            assert!(super::super::imp::continuation_blocker(&m, &i).is_some());
        }

        #[test]
        fn the_same_id_from_another_extractor_blocks_continuation() {
            // Ids are namespaced per extractor, so `x` on one site says
            // nothing about `x` on another.
            let mut m = metadata_with_id("https://youtu.be/x", "137+251", "x");
            if let Some(EngineDetails::YtdlpDetails(d)) = m.engine_details.as_mut() {
                d.extractor = "vimeo".to_owned();
            }
            let i = instruction_with_id("https://youtu.be/x", "137+251", Some("x"));
            assert!(super::super::imp::continuation_blocker(&m, &i).is_some());
        }

        #[test]
        fn metadata_without_an_id_falls_back_to_the_url() {
            // Written by an odl that predates `video_id`. An absent id means
            // unknown, so the old URL comparison has to still apply.
            let m = metadata_for("https://youtu.be/x", "137+251");
            let same = instruction_with_id("https://youtu.be/x", "137+251", Some("x"));
            assert!(super::super::imp::continuation_blocker(&m, &same).is_none());
            let other = instruction_with_id("https://youtu.be/y", "137+251", Some("x"));
            assert!(super::super::imp::continuation_blocker(&m, &other).is_some());
        }

        #[test]
        fn a_different_format_blocks_continuation() {
            let m = metadata_for("https://youtu.be/x", "137+251");
            let i = instruction("https://youtu.be/x", "248+251");
            assert!(super::super::imp::continuation_blocker(&m, &i).is_some());
        }

        #[test]
        fn metadata_from_the_http_engine_blocks_continuation() {
            let mut m = metadata_for("https://youtu.be/x", "137+251");
            m.engine = crate::download_metadata::DownloadEngine::HttpMultipart.into();
            m.engine_details = None;
            let i = instruction("https://youtu.be/x", "137+251");
            assert!(super::super::imp::continuation_blocker(&m, &i).is_some());
        }
    }
}
