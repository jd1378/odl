//! Choosing which media format to download.
//!
//! Sites that serve media offer the same title in many encodings, and which
//! one to take is a policy decision — a CLI may want to ask, a GUI to show a
//! menu, a script to take the best available without prompting. The engine
//! therefore hands the offer to a [`FormatSelector`] and downloads whatever
//! comes back, the same way save conflicts are delegated to a resolver.
//!
//! The selector returns a *concrete* format id rather than a selector
//! expression. That matters for resuming: the chosen id is persisted, and a
//! resume re-uses it verbatim instead of re-deciding, which is what keeps a
//! partially downloaded file from being continued in a different encoding.

use async_trait::async_trait;
use url::Url;

/// What a format offers, in the terms a person judges it by.
///
/// Structured rather than a rendered string so a UI can phrase it in its own
/// language. [`Display`](std::fmt::Display) gives a reasonable English
/// rendering for callers that just need text; only `Unknown`'s wording and
/// the word "audio" are translatable — resolutions and frame rates are not.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Quality {
    /// Carries picture. `fps` is only meaningful above the usual 24–30, which
    /// is why it is optional rather than always rendered.
    Video { height: u32, fps: Option<f64> },
    /// Sound with no picture.
    Audio { bitrate_kbps: Option<u32> },
    /// A transcript rather than media: subtitles in one language.
    ///
    /// `automatic` marks machine-generated captions, which are usually far
    /// less accurate than an author-supplied track and worth distinguishing.
    Subtitles { lang: String, automatic: bool },
    /// Neither resolution nor bitrate was reported; `note` is whatever label
    /// the extractor gave, if any.
    Unknown { note: Option<String> },
}

impl std::fmt::Display for Quality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Quality::Video { height, fps } => {
                // A frame rate is only worth showing when it is unusual;
                // "1080p30" is noise next to "1080p".
                match fps.filter(|v| *v >= 50.0) {
                    Some(v) => write!(f, "{height}p{}", v.round() as u32),
                    None => write!(f, "{height}p"),
                }
            }
            Quality::Audio { bitrate_kbps } => match bitrate_kbps {
                Some(k) => write!(f, "audio {k}k"),
                None => f.write_str("audio"),
            },
            Quality::Subtitles { lang, automatic } => {
                if *automatic {
                    write!(f, "auto-subtitles ({lang})")
                } else {
                    write!(f, "subtitles ({lang})")
                }
            }
            Quality::Unknown { note } => match note {
                Some(n) => f.write_str(n),
                None => f.write_str("unknown"),
            },
        }
    }
}

/// One downloadable encoding of a media item.
#[derive(Debug, Clone, PartialEq)]
pub struct MediaFormat {
    /// Identifier the engine uses to request exactly this encoding.
    pub id: String,
    /// Container extension (`mp4`, `webm`, `m4a`, …).
    pub ext: String,
    /// Vertical resolution, when the format carries video.
    pub height: Option<u32>,
    pub fps: Option<f64>,
    /// Average bitrate in kbit/s, used to rank formats of equal height.
    pub tbr: Option<f64>,
    /// Byte size, exact when known.
    pub size: Option<u64>,
    /// Whether `size` is an estimate.
    pub size_is_approx: bool,
    /// Carries a video stream.
    pub has_video: bool,
    /// Carries an audio stream.
    pub has_audio: bool,
    /// Free-form label from the extractor (`1080p60`, `medium`, …).
    pub note: Option<String>,
}

impl MediaFormat {
    /// A format that can be downloaded on its own, with no muxing step.
    pub fn is_complete(&self) -> bool {
        self.has_video && self.has_audio
    }

    /// Audio with no video track.
    pub fn is_audio_only(&self) -> bool {
        self.has_audio && !self.has_video
    }

    /// What this format offers, in terms a person judges it by.
    pub fn quality(&self) -> Quality {
        if let Some(height) = self.height {
            Quality::Video {
                height,
                fps: self.fps,
            }
        } else if self.is_audio_only() {
            Quality::Audio {
                bitrate_kbps: self.tbr.map(|t| t.round() as u32),
            }
        } else {
            Quality::Unknown {
                note: self.note.clone().or_else(|| Some(self.id.clone())),
            }
        }
    }

    /// English rendering of [`Self::quality`], for callers that only need
    /// text. Prefer the structured value when the text is user-facing.
    pub fn quality_label(&self) -> String {
        self.quality().to_string()
    }
}

/// A transcript track an extractor reported for a media item.
#[derive(Debug, Clone, PartialEq)]
pub struct SubtitleTrack {
    /// Language tag as the site reports it (`en`, `pt-BR`, …).
    pub lang: String,
    /// File type the track is served as (`vtt`, `srt`, …).
    pub ext: String,
    /// Machine-generated rather than author-supplied.
    pub automatic: bool,
}

/// Prefixes reserved for subtitle ids, which no media format id may use.
pub const SUBTITLE_ID_PREFIXES: [&str; 2] = ["subs:", "autosubs:"];

/// Whether `lang` is shaped like a language tag.
///
/// Deliberately narrow: the tag ends up in a process argument and in a
/// filename, and everything real (`en`, `pt-BR`, `zh-Hans`) fits inside it.
fn is_language_tag(lang: &str) -> bool {
    !lang.is_empty()
        && lang.len() <= 32
        && lang
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Format id naming a subtitle track rather than a media format.
///
/// Subtitles have no format id of their own, but the chosen download has to be
/// storable and re-requestable on a resume exactly like a media format. A
/// reserved prefix keeps them in the same field without a second mechanism.
///
/// Returns `None` for a language tag that could not be read back — an id that
/// does not round-trip would resume as something else entirely.
pub fn subtitle_format_id(lang: &str, automatic: bool) -> Option<String> {
    if !is_language_tag(lang) {
        return None;
    }
    let prefix = if automatic { "autosubs" } else { "subs" };
    Some(format!("{prefix}:{lang}"))
}

/// Read back an id produced by [`subtitle_format_id`].
///
/// The language is validated on the way out as well as on the way in: an id
/// can arrive from persisted metadata or from a user's `--format-id`, neither
/// of which this crate wrote.
pub fn parse_subtitle_format_id(id: &str) -> Option<(&str, bool)> {
    let (lang, automatic) = if let Some(lang) = id.strip_prefix("autosubs:") {
        (lang, true)
    } else {
        (id.strip_prefix("subs:")?, false)
    };
    is_language_tag(lang).then_some((lang, automatic))
}

/// One entry of a quality menu.
#[derive(Debug, Clone, PartialEq)]
pub struct QualityTier {
    /// What this tier offers. Render with `to_string()` for English, or match
    /// on it to phrase it in another language.
    pub quality: Quality,
    /// Id to request for this tier, compound when two streams are muxed.
    pub format_id: String,
    /// File type this tier produces (`mp4`, `mkv`, `m4a`, `vtt`, …), so the
    /// choice is not just a resolution but a file you can predict.
    pub ext: String,
    /// Combined size, `None` when any part of it is unknown.
    pub size: Option<u64>,
    pub size_is_approx: bool,
    /// Whether choosing this tier requires a muxer.
    pub needs_merge: bool,
    /// Whether this tier can actually be downloaded right now.
    ///
    /// False only when it needs a muxer that is not installed. Such tiers are
    /// still listed: seeing that 1080p exists but needs ffmpeg is far more
    /// useful than a list that silently stops at 720p and looks like all the
    /// site offers.
    pub available: bool,
}

/// What the selector gets to choose from.
#[derive(Debug, Clone)]
pub struct FormatOffer {
    /// Page the media was extracted from.
    ///
    /// Present so a selector serving several downloads at once can tell them
    /// apart: titles collide, URLs do not.
    pub source_url: Url,
    /// Title of the media item, for display.
    pub title: String,
    /// Every format the extractor reported.
    pub formats: Vec<MediaFormat>,
    /// Format the engine would take by default, already resolved.
    pub default_id: Option<String>,
    /// Container the engine reported for [`Self::default_id`]. Trusted over
    /// deriving one, since it accounts for what muxing actually produces.
    pub default_ext: Option<String>,
    /// Transcript tracks the extractor reported, if any.
    pub subtitles: Vec<SubtitleTrack>,
    /// Whether a muxer is available. When false, combining a video-only and
    /// an audio-only format is impossible and the offer is limited to formats
    /// that are complete on their own.
    pub can_merge: bool,
}

impl FormatOffer {
    /// Formats that could be downloaded given the available tools, best
    /// first. Video is ranked by height then bitrate; audio-only formats
    /// sort last so a video request never silently yields sound alone.
    pub fn selectable(&self) -> Vec<&MediaFormat> {
        let mut out: Vec<&MediaFormat> = self
            .formats
            .iter()
            .filter(|f| {
                if self.can_merge {
                    true
                } else {
                    f.is_complete()
                }
            })
            .filter(|f| f.has_video || f.has_audio)
            .collect();
        out.sort_by(|a, b| {
            b.has_video
                .cmp(&a.has_video)
                .then(b.height.unwrap_or(0).cmp(&a.height.unwrap_or(0)))
                .then(
                    b.tbr
                        .unwrap_or(0.0)
                        .partial_cmp(&a.tbr.unwrap_or(0.0))
                        .unwrap_or(std::cmp::Ordering::Equal),
                )
        });
        out
    }

    /// Distinct quality tiers, best first. Collapses the dozens of encodings
    /// an extractor reports into the handful of choices a person actually
    /// cares about.
    ///
    /// Video tiers combine with the best audio when merging is possible, so
    /// a tier's `format_id` may be a compound one.
    /// Every format the extractor reported, best first, whether or not the
    /// tools to use it are present.
    fn ranked(&self) -> Vec<&MediaFormat> {
        let mut out: Vec<&MediaFormat> = self
            .formats
            .iter()
            .filter(|f| f.has_video || f.has_audio)
            .collect();
        out.sort_by(|a, b| {
            b.has_video
                .cmp(&a.has_video)
                .then(b.height.unwrap_or(0).cmp(&a.height.unwrap_or(0)))
                .then(
                    b.tbr
                        .unwrap_or(0.0)
                        .partial_cmp(&a.tbr.unwrap_or(0.0))
                        .unwrap_or(std::cmp::Ordering::Equal),
                )
        });
        out
    }

    pub fn quality_tiers(&self) -> Vec<QualityTier> {
        let best_audio = self.best_audio_only();
        let mut seen_heights: Vec<u32> = Vec::new();
        let mut tiers: Vec<QualityTier> = Vec::new();

        for f in self.ranked() {
            if !f.has_video {
                continue;
            }
            let Some(height) = f.height else { continue };
            if seen_heights.contains(&height) {
                continue;
            }
            seen_heights.push(height);

            let tier = if f.is_complete() {
                QualityTier {
                    quality: f.quality(),
                    ext: self.ext_for_parts(&f.id, &[f]),
                    format_id: f.id.clone(),
                    size: f.size,
                    size_is_approx: f.size_is_approx,
                    needs_merge: false,
                    available: true,
                }
            } else if let Some(audio) = best_audio {
                let id = merged_id(&f.id, &audio.id);
                QualityTier {
                    quality: f.quality(),
                    ext: self.ext_for_parts(&id, &[f, audio]),
                    format_id: id,
                    // Either part being unknown makes the sum unknown rather
                    // than an understatement.
                    size: f.size.zip(audio.size).map(|(v, a)| v.saturating_add(a)),
                    size_is_approx: f.size_is_approx || audio.size_is_approx,
                    needs_merge: true,
                    available: self.can_merge,
                }
            } else {
                continue;
            };
            tiers.push(tier);
        }

        if let Some(audio) = best_audio {
            tiers.push(QualityTier {
                quality: audio.quality(),
                ext: self.ext_for_parts(&audio.id, &[audio]),
                format_id: audio.id.clone(),
                size: audio.size,
                size_is_approx: audio.size_is_approx,
                needs_merge: false,
                available: true,
            });
        }

        // Transcripts last: they are a different kind of thing from a video
        // quality, and listing them among the resolutions would invite
        // picking one by accident. Author-supplied tracks come before
        // machine-generated ones, which are markedly less accurate.
        let mut subs: Vec<&SubtitleTrack> = self.subtitles.iter().collect();
        subs.sort_by_key(|t| (t.automatic, t.lang.clone()));
        for track in subs {
            let Some(format_id) = subtitle_format_id(&track.lang, track.automatic) else {
                // A tag that cannot round-trip through an id cannot be
                // offered: choosing it would resume as something else.
                continue;
            };
            tiers.push(QualityTier {
                quality: Quality::Subtitles {
                    lang: track.lang.clone(),
                    automatic: track.automatic,
                },
                ext: track.ext.clone(),
                format_id,
                // Extractors do not report a size for subtitle tracks, and
                // they are small enough that guessing adds nothing.
                size: None,
                size_is_approx: false,
                needs_merge: false,
                available: true,
            });
        }
        tiers
    }

    /// Container a tier produces, preferring the engine's own answer for the
    /// selection it already resolved.
    fn ext_for_parts(&self, format_id: &str, parts: &[&MediaFormat]) -> String {
        if self.default_id.as_deref() == Some(format_id)
            && let Some(ext) = &self.default_ext
        {
            return ext.clone();
        }
        let exts: Vec<&str> = parts.iter().map(|f| f.ext.as_str()).collect();
        if exts.is_empty() {
            return "bin".to_owned();
        }
        merged_ext(&exts)
    }

    /// Highest-bitrate audio-only format, used as the partner for video-only
    /// formats.
    pub fn best_audio_only(&self) -> Option<&MediaFormat> {
        self.formats
            .iter()
            .filter(|f| f.is_audio_only())
            .max_by(|a, b| {
                a.tbr
                    .unwrap_or(0.0)
                    .partial_cmp(&b.tbr.unwrap_or(0.0))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    }

    /// Look up a format by id, including one side of a compound id.
    pub fn find(&self, id: &str) -> Option<&MediaFormat> {
        self.formats.iter().find(|f| f.id == id)
    }
}

/// Compound id requesting two formats to be muxed together.
pub fn merged_id(video_id: &str, audio_id: &str) -> String {
    format!("{video_id}+{audio_id}")
}

/// The parts of a possibly-compound format id.
pub fn split_id(id: &str) -> Vec<&str> {
    id.split('+').collect()
}

/// Container the muxer will produce for a combination of source containers.
///
/// Mirrors the rule used by the muxing step: matching families keep their
/// container, anything else falls back to Matroska, which accepts any stream
/// combination.
pub fn merged_ext(exts: &[&str]) -> String {
    let mp4_family = ["mp4", "m4a", "m4v", "mov"];
    let webm_family = ["webm", "weba"];
    if exts.iter().all(|e| mp4_family.contains(e)) {
        "mp4".to_owned()
    } else if exts.iter().all(|e| webm_family.contains(e)) {
        "webm".to_owned()
    } else {
        "mkv".to_owned()
    }
}

/// Decides which format to download.
///
/// Returning `None` aborts the download; the caller reports it as a
/// cancellation rather than a failure.
#[async_trait]
pub trait FormatSelector: Send + Sync {
    async fn select(&self, offer: &FormatOffer) -> Option<String>;
}

/// Takes the engine's own default without asking.
///
/// Used for non-interactive runs — scripts, library consumers, and any output
/// mode where a prompt would corrupt the output stream.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultFormatSelector;

#[async_trait]
impl FormatSelector for DefaultFormatSelector {
    async fn select(&self, offer: &FormatOffer) -> Option<String> {
        offer
            .default_id
            .clone()
            // No resolved default means the engine could not pick one, which
            // happens when merging is unavailable and nothing is complete on
            // its own. Fall back to the best selectable format.
            .or_else(|| offer.selectable().first().map(|f| f.id.clone()))
    }
}

/// Requests one exact format, whatever the offer contains.
///
/// For callers that already know which id they want — a script repeating an
/// earlier choice, or a UI acting on a selection the user made elsewhere.
/// The id is passed through unchecked: the engine reports a format that is no
/// longer offered far more precisely than a guess here could.
#[derive(Debug, Clone)]
pub struct FixedFormatSelector(pub String);

#[async_trait]
impl FormatSelector for FixedFormatSelector {
    async fn select(&self, _offer: &FormatOffer) -> Option<String> {
        Some(self.0.clone())
    }
}

/// Picks the best tier no taller than `max_height`, for callers that want a
/// quality cap without a prompt.
#[derive(Debug, Clone, Copy)]
pub struct MaxHeightFormatSelector {
    pub max_height: u32,
}

#[async_trait]
impl FormatSelector for MaxHeightFormatSelector {
    async fn select(&self, offer: &FormatOffer) -> Option<String> {
        let best_audio = offer.best_audio_only();
        for f in offer.selectable() {
            if !f.has_video {
                continue;
            }
            if f.height.is_some_and(|h| h > self.max_height) {
                continue;
            }
            if f.is_complete() {
                return Some(f.id.clone());
            }
            if let Some(audio) = best_audio {
                return Some(merged_id(&f.id, &audio.id));
            }
        }
        DefaultFormatSelector.select(offer).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn video(id: &str, height: u32, ext: &str) -> MediaFormat {
        MediaFormat {
            id: id.to_owned(),
            ext: ext.to_owned(),
            height: Some(height),
            fps: Some(30.0),
            tbr: Some(height as f64 * 2.0),
            size: Some(1000),
            size_is_approx: false,
            has_video: true,
            has_audio: false,
            note: None,
        }
    }

    fn audio(id: &str, tbr: f64, ext: &str) -> MediaFormat {
        MediaFormat {
            id: id.to_owned(),
            ext: ext.to_owned(),
            height: None,
            fps: None,
            tbr: Some(tbr),
            size: Some(100),
            size_is_approx: false,
            has_video: false,
            has_audio: true,
            note: None,
        }
    }

    fn complete(id: &str, height: u32) -> MediaFormat {
        MediaFormat {
            has_audio: true,
            ..video(id, height, "mp4")
        }
    }

    fn offer(formats: Vec<MediaFormat>, can_merge: bool) -> FormatOffer {
        FormatOffer {
            source_url: Url::parse("https://example.test/watch").unwrap(),
            title: "T".to_owned(),
            formats,
            default_id: None,
            default_ext: None,
            subtitles: Vec::new(),
            can_merge,
        }
    }

    #[test]
    fn tiers_needing_a_muxer_are_listed_but_marked_unavailable() {
        // Hiding them would make the menu look like all the site offers, and
        // the user would never learn that installing ffmpeg unlocks 1080p.
        let o = offer(
            vec![
                video("137", 1080, "mp4"),
                audio("251", 128.0, "webm"),
                complete("18", 360),
            ],
            false,
        );
        let tiers = o.quality_tiers();
        let by_label: Vec<(String, bool)> = tiers
            .iter()
            .map(|t| (t.quality.to_string(), t.available))
            .collect();
        assert_eq!(
            by_label,
            [
                ("1080p".to_owned(), false),
                ("360p".to_owned(), true),
                ("audio 128k".to_owned(), true)
            ]
        );

        // With a muxer present the same list is fully available.
        let o = FormatOffer {
            can_merge: true,
            ..o
        };
        assert!(o.quality_tiers().iter().all(|t| t.available));
    }

    #[test]
    fn without_a_muxer_only_self_contained_formats_are_offered() {
        let o = offer(
            vec![
                video("137", 1080, "mp4"),
                audio("251", 128.0, "webm"),
                complete("18", 360),
            ],
            false,
        );
        let ids: Vec<&str> = o.selectable().iter().map(|f| f.id.as_str()).collect();
        assert_eq!(ids, ["18"], "video-only and audio-only need a muxer");
    }

    #[test]
    fn with_a_muxer_video_tiers_pair_with_the_best_audio() {
        let o = offer(
            vec![
                video("137", 1080, "mp4"),
                video("136", 720, "mp4"),
                audio("251", 128.0, "webm"),
                audio("250", 64.0, "webm"),
            ],
            true,
        );
        let tiers = o.quality_tiers();
        assert_eq!(tiers[0].quality.to_string(), "1080p");
        // Highest-bitrate audio wins as the partner.
        assert_eq!(tiers[0].format_id, "137+251");
        assert!(tiers[0].needs_merge);
        assert_eq!(tiers[1].format_id, "136+251");
        // Audio-only stays available as its own tier, listed last.
        assert_eq!(tiers.last().unwrap().format_id, "251");
        assert!(!tiers.last().unwrap().needs_merge);
    }

    #[test]
    fn quality_tiers_collapse_duplicate_heights() {
        let o = offer(
            vec![
                video("137", 1080, "mp4"),
                video("248", 1080, "webm"),
                video("136", 720, "mp4"),
                audio("251", 128.0, "webm"),
            ],
            true,
        );
        let tiers = o.quality_tiers();
        let heights: Vec<String> = tiers.iter().map(|t| t.quality.to_string()).collect();
        assert_eq!(heights, ["1080p", "720p", "audio 128k"]);
    }

    #[test]
    fn tiers_report_the_container_they_produce() {
        let o = offer(
            vec![
                video("137", 1080, "mp4"),
                audio("251", 128.0, "webm"),
                complete("18", 360),
            ],
            true,
        );
        let tiers = o.quality_tiers();
        // mp4 video muxed with webm audio fits neither, so Matroska it is.
        assert_eq!(tiers[0].ext, "mkv");
        assert_eq!(tiers[1].ext, "mp4");
        assert_eq!(tiers.last().unwrap().ext, "webm");
    }

    #[test]
    fn transcripts_are_offered_last_and_authored_ones_come_first() {
        let mut o = offer(vec![complete("18", 360)], true);
        o.subtitles = vec![
            SubtitleTrack {
                lang: "es".to_owned(),
                ext: "vtt".to_owned(),
                automatic: true,
            },
            SubtitleTrack {
                lang: "en".to_owned(),
                ext: "srt".to_owned(),
                automatic: false,
            },
        ];
        let tiers = o.quality_tiers();

        // Media first: a transcript listed among the resolutions invites
        // picking one by accident.
        assert_eq!(
            tiers[0].quality,
            Quality::Video {
                height: 360,
                fps: Some(30.0)
            }
        );
        assert_eq!(tiers[1].quality.to_string(), "subtitles (en)");
        assert_eq!(tiers[1].ext, "srt");
        assert_eq!(tiers[2].quality.to_string(), "auto-subtitles (es)");
        // Sizes are never reported for transcripts.
        assert!(tiers[1].size.is_none() && tiers[2].size.is_none());
    }

    #[test]
    fn a_subtitle_choice_round_trips_through_its_id() {
        // The id is what gets persisted, so the language and whether it is
        // machine-generated have to survive it.
        let id = subtitle_format_id("pt-BR", true).unwrap();
        assert_eq!(parse_subtitle_format_id(&id), Some(("pt-BR", true)));
        assert_eq!(
            parse_subtitle_format_id(&subtitle_format_id("en", false).unwrap()),
            Some(("en", false))
        );
        // A real media id must never be mistaken for one.
        assert_eq!(parse_subtitle_format_id("137+251"), None);
        assert_eq!(parse_subtitle_format_id("18"), None);

        // Anything that would not survive the round trip is refused at both
        // ends, so a malformed id from metadata or `--format-id` cannot be
        // mistaken for a valid transcript request.
        assert_eq!(subtitle_format_id("en:US", false), None);
        assert_eq!(subtitle_format_id("", false), None);
        assert_eq!(parse_subtitle_format_id("subs:"), None);
        assert_eq!(parse_subtitle_format_id("subs:../etc/passwd"), None);
    }

    #[test]
    fn merged_container_follows_the_source_families() {
        assert_eq!(merged_ext(&["mp4", "m4a"]), "mp4");
        assert_eq!(merged_ext(&["webm", "webm"]), "webm");
        // Mixed families cannot share a container that accepts both.
        assert_eq!(merged_ext(&["mp4", "webm"]), "mkv");
        assert_eq!(merged_ext(&["mp4"]), "mp4");
    }

    #[tokio::test]
    async fn default_selector_prefers_the_resolved_default() {
        let mut o = offer(vec![video("137", 1080, "mp4"), complete("18", 360)], true);
        o.default_id = Some("137+251".to_owned());
        assert_eq!(
            DefaultFormatSelector.select(&o).await,
            Some("137+251".to_owned())
        );
    }

    #[tokio::test]
    async fn default_selector_falls_back_when_nothing_was_resolved() {
        let o = offer(vec![complete("18", 360), video("137", 1080, "mp4")], false);
        // Only the complete format is selectable without a muxer.
        assert_eq!(
            DefaultFormatSelector.select(&o).await,
            Some("18".to_owned())
        );
    }

    #[tokio::test]
    async fn max_height_selector_caps_quality() {
        let o = offer(
            vec![
                video("137", 1080, "mp4"),
                video("136", 720, "mp4"),
                audio("251", 128.0, "webm"),
            ],
            true,
        );
        let selector = MaxHeightFormatSelector { max_height: 720 };
        assert_eq!(selector.select(&o).await, Some("136+251".to_owned()));
    }

    #[test]
    fn split_id_handles_compound_and_plain_ids() {
        assert_eq!(split_id("137+251"), ["137", "251"]);
        assert_eq!(split_id("18"), ["18"]);
    }
}
