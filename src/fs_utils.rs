use filetime::{FileTime, set_file_mtime};
use prost::Message;
use std::{io, path::Path, path::PathBuf};

use tokio::{fs::OpenOptions, io::AsyncWriteExt};

/// Longest filename component the common filesystems accept, in bytes.
const MAX_FILENAME_BYTES: usize = 255;

/// Stand-in for a name that sanitising emptied out.
const FALLBACK_FILENAME: &str = "unnamed";

static FORBIDDEN_WINDOWS_NAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// returns a filename that is safe to use on Windows, Linux and Mac OS
///
/// ### Details
///
/// On Windows, filenames cannot contain the following characters: \ / : * ? " < > | ^
///
/// On Linux the only forbidden character in filenames is '/'.
///
/// On Mac OS, same as linux, plus it cannot contain ':' (kind of)
///
/// Control characters (ASCII 0-31) are also not allowed on most platforms.
///
/// This function replaces all forbidden characters with '_', and trims leading/trailing whitespace and dots,
/// which can cause issues on Windows (e.g., filenames ending with a dot or space are not allowed).
/// Transliteration to ASCII, when `to_ascii` is set, happens before anything
/// else — it can itself introduce characters this must then sanitise.
///
/// Transliteration is opt-in and off by default, because it is lossy in a way
/// sanitising is not: `Café` and `Cafe` become the same name, and so do two
/// titles that differ only in a script this collapses. It is also the key the
/// per-download directory is named after, so turning it on renames the
/// directory of anything already in flight and strands its partial data.
///
/// What it buys is a name that is byte-identical everywhere — no locale, no
/// filesystem normalisation, no console that cannot render it — which is why
/// it exists as a choice rather than not at all.
pub fn cleanup_filename(input: &str, to_ascii: bool) -> String {
    let mut result = if to_ascii {
        deunicode::deunicode(input)
    } else {
        String::from(input)
    };
    result = result
        .chars()
        .map(|c| match c {
            // Replace forbidden characters with '_'
            '/' | '\\' | '?' | '%' | '*' | ':' | '|' | '"' | '<' | '>' | '^' => '_',
            // Remove control characters
            c if c.is_control() => '_',
            _ => c,
        })
        .collect();
    // Remove leading/trailing whitespace and dots
    result = result
        .trim_matches(|c: char| c.is_whitespace() || c == '.')
        .to_string();

    // Trimming can consume the whole name: `"..."` and `"   "` are legal
    // titles that sanitise to nothing. An empty component is not a filename —
    // joining one onto a directory yields the directory itself, so the
    // download would target its own parent.
    if result.is_empty() {
        return FALLBACK_FILENAME.to_owned();
    }

    // Avoid the reserved Windows device names by appending an underscore.
    // The comparison is against the stem, not the whole name: `NUL.mkv` is
    // just as reserved as `NUL`, and a media file always has an extension.
    let stem = result.split('.').next().unwrap_or(&result);
    if FORBIDDEN_WINDOWS_NAMES
        .iter()
        .any(|&name| name.eq_ignore_ascii_case(stem))
    {
        result.push('_');
    }

    // Limit by bytes, on a character boundary. `String::truncate` panics when
    // the index lands mid-character, which any non-Latin title long enough to
    // need truncating would do. Bytes rather than characters because that is
    // what the filesystem limit counts; a UTF-8 string of 255 bytes is also
    // at most 255 UTF-16 units, so this stays inside Windows' limit too.
    if result.len() > MAX_FILENAME_BYTES {
        let mut end = MAX_FILENAME_BYTES;
        while end > 0 && !result.is_char_boundary(end) {
            end -= 1;
        }
        result.truncate(end);
        // Truncating can expose a trailing dot or space, which Windows will
        // not accept, so trim again rather than before only.
        result = result
            .trim_end_matches(|c: char| c.is_whitespace() || c == '.')
            .to_string();
    }

    result
}

/// Creates a file at the given path and sets its last modified time to the provided UNIX timestamp (seconds).
pub async fn set_file_mtime_async<P: AsRef<Path>>(path: &P, unix_time_secs: i64) -> io::Result<()> {
    let file_time = FileTime::from_unix_time(unix_time_secs, 0);
    let path = path.as_ref().to_path_buf();
    tokio::task::spawn_blocking(move || set_file_mtime(&path, file_time)).await??;
    Ok(())
}

pub fn get_odl_dir() -> PathBuf {
    dirs::data_dir()
        .map(|mut path| {
            path.push("odl");
            path
        })
        .unwrap_or_else(|| {
            let tmp_dir = std::path::PathBuf::from("/tmp/odl");
            std::fs::create_dir_all(&tmp_dir).ok();
            tmp_dir
        })
}

/// reads a protobuf delimited encoded message of Type `M` and return if successful
pub async fn read_delimited_message_from_path<M: Message + Default, P: AsRef<Path>>(
    path: &P,
) -> io::Result<M> {
    let buf = tokio::fs::read(path).await?;
    M::decode_length_delimited(&*buf).map_err(io::Error::other)
}

/// Permissions for files that may embed user secrets: download metadata
/// (see [`atomic_write`]) and the config file, both of which can hold
/// user-supplied request headers such as `Authorization` or `Cookie`.
#[cfg(unix)]
pub(crate) const OWNER_ONLY_MODE: u32 = 0o600;

pub async fn atomic_replace(src: PathBuf, dst: PathBuf) -> io::Result<()> {
    tokio::task::spawn_blocking(move || {
        let src_p = dunce::simplified(&src);
        let dst_p = dunce::simplified(&dst);
        atomicwrites::replace_atomic(src_p, dst_p)
    })
    .await
    .map_err(io::Error::other)??;

    Ok(())
}

/// Atomically writes the given bytes to the specified path.
/// Writes to a temporary file in the same directory and then renames it over the target file.
/// Ensures that either the entire file is written or not changed at all.
/// Truncates the tmp_file if it exists
///
/// A newly created file is restricted to owner-only access (0600 on unix):
/// callers persist download metadata, which embeds user-supplied request
/// headers such as `Authorization` or `Cookie`. Permissions of a temp file
/// left behind by an earlier run are inherited as-is — that write is
/// consumed by the rename, so the next one is owner-only again.
pub async fn atomic_write(path: PathBuf, tmp_path: PathBuf, data: &[u8]) -> io::Result<()> {
    // Write to the temporary file
    {
        let mut opts = OpenOptions::new();
        opts.create(true).write(true).truncate(true);
        #[cfg(unix)]
        opts.mode(OWNER_ONLY_MODE);
        let mut tmp_file = opts.open(&tmp_path).await?;
        tmp_file.write_all(data).await?;
        tmp_file.sync_all().await?;
    }

    atomic_replace(tmp_path, path).await
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IsUnique {
    Yes,
    SuggestedAlternative(String),
}

/// Finds a unique file name by appending a counter if the path already exists.
/// If the given path does not exist, returns it as-is.
/// If it exists, appends _2, _3, etc. before the extension until a non-existing path is found.
pub async fn is_filename_unique<P: AsRef<Path>>(path: &P) -> io::Result<IsUnique> {
    let path = path.as_ref();
    if !tokio::fs::try_exists(path).await? {
        return Ok(IsUnique::Yes);
    }

    let file_stem = path.file_stem().and_then(|s| s.to_str()).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "Path has no valid file stem")
    })?;
    let extension = path.extension().and_then(|e| e.to_str());
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Path has no parent directory",
            ));
        }
    };

    let mut counter = 2;
    loop {
        let new_file_name = if let Some(ext) = extension {
            format!("{}_{}.{}", file_stem, counter, ext)
        } else {
            format!("{}_{}", file_stem, counter)
        };
        let new_path = parent.join(new_file_name.clone());
        if !tokio::fs::try_exists(&new_path).await? {
            return Ok(IsUnique::SuggestedAlternative(new_file_name));
        }
        counter += 1;
    }
}

/// Canonicalizes a path using [`dunce`], avoiding unwanted `\\?\` verbatim prefixes
/// on Windows for paths shorter than 260 characters while preserving them for long paths.
#[allow(dead_code)]
pub fn canonicalize<P: AsRef<Path>>(path: P) -> io::Result<PathBuf> {
    dunce::canonicalize(path)
}

/// Simplifies a path using [`dunce`], stripping verbatim `\\?\` prefixes on Windows
/// when the path is safely below MAX_PATH.
#[allow(dead_code)]
pub fn simplified(path: &Path) -> &Path {
    dunce::simplified(path)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_cleanup_filename_basic() {
        assert_eq!(
            cleanup_filename("normal_filename.txt", false),
            "normal_filename.txt"
        );
        assert_eq!(
            cleanup_filename("file/with/slash.txt", false),
            "file_with_slash.txt"
        );
        assert_eq!(
            cleanup_filename("file\\with\\backslash.txt", false),
            "file_with_backslash.txt"
        );
        assert_eq!(
            cleanup_filename("file:with:colon.txt", false),
            "file_with_colon.txt"
        );
        assert_eq!(
            cleanup_filename("file*with*asterisk.txt", false),
            "file_with_asterisk.txt"
        );
        assert_eq!(
            cleanup_filename("file?with?question.txt", false),
            "file_with_question.txt"
        );
        assert_eq!(
            cleanup_filename("file\"with\"quote.txt", false),
            "file_with_quote.txt"
        );
        assert_eq!(
            cleanup_filename("file<with<less.txt", false),
            "file_with_less.txt"
        );
        assert_eq!(
            cleanup_filename("file>with>greater.txt", false),
            "file_with_greater.txt"
        );
        assert_eq!(
            cleanup_filename("file|with|pipe.txt", false),
            "file_with_pipe.txt"
        );
        assert_eq!(
            cleanup_filename("file^with^caret.txt", false),
            "file_with_caret.txt"
        );
    }

    #[test]
    fn test_cleanup_filename_trim() {
        assert_eq!(
            cleanup_filename("   filename.txt   ", false),
            "filename.txt"
        );
        assert_eq!(
            cleanup_filename("...filename.txt...", false),
            "filename.txt"
        );
        assert_eq!(
            cleanup_filename("   ...filename.txt...   ", false),
            "filename.txt"
        );
    }

    #[test]
    fn test_cleanup_filename_forbidden_windows_names() {
        for &name in FORBIDDEN_WINDOWS_NAMES {
            assert_eq!(cleanup_filename(name, false), format!("{name}_"));
            assert_eq!(
                cleanup_filename(&name.to_ascii_lowercase(), false),
                format!("{}_", name.to_ascii_lowercase())
            );
        }
    }

    #[test]
    fn test_cleanup_filename_control_chars() {
        let input = "file\u{0000}name.txt";
        assert_eq!(cleanup_filename(input, false), "file_name.txt");
    }

    #[test]
    fn test_cleanup_filename_truncate() {
        let long_name = "a".repeat(300);
        let cleaned = cleanup_filename(&long_name, false);
        assert_eq!(cleaned.len(), 255);
    }

    #[tokio::test]
    async fn test_unique_filename_when_not_exists() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("file.txt");
        let result = is_filename_unique(&file_path).await.unwrap();
        assert_eq!(result, IsUnique::Yes);
    }

    #[tokio::test]
    async fn test_suggested_alternative_when_exists() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("file.txt");
        fs::write(file_path.clone(), b"test").unwrap();
        let result = is_filename_unique(&file_path).await.unwrap();
        assert_eq!(
            result,
            IsUnique::SuggestedAlternative("file_2.txt".to_string())
        );
    }

    #[tokio::test]
    async fn test_multiple_existing_files() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("file.txt");
        let file2_path = dir.path().join("file_2.txt");
        let file3_path = dir.path().join("file_3.txt");
        fs::write(&file_path, b"test").unwrap();
        fs::write(&file2_path, b"test2").unwrap();
        fs::write(&file3_path, b"test3").unwrap();
        let result = is_filename_unique(&file_path).await.unwrap();
        assert_eq!(
            result,
            IsUnique::SuggestedAlternative("file_4.txt".to_string())
        );
    }

    #[tokio::test]
    async fn test_no_extension() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("file");
        fs::write(&file_path, b"test").unwrap();
        let result = is_filename_unique(&file_path).await.unwrap();
        assert_eq!(result, IsUnique::SuggestedAlternative("file_2".to_string()));
    }

    #[tokio::test]
    async fn test_path_with_no_parent() {
        // If the file does not exist, it should return Ok(IsUnique::Yes)
        let file_path = std::path::Path::new("file.txt");
        let result = is_filename_unique(&file_path).await;
        assert_eq!(result.unwrap(), IsUnique::Yes);

        // If the file exists, it should error due to missing parent
        std::fs::write(file_path, b"test").unwrap();
        let result = is_filename_unique(&file_path).await;
        let _ = std::fs::remove_file(file_path);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unicode_filename() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("файл.txt");
        fs::write(&file_path, b"test").unwrap();
        let result = is_filename_unique(&file_path).await.unwrap();
        assert_eq!(
            result,
            IsUnique::SuggestedAlternative("файл_2.txt".to_string())
        );
    }

    #[tokio::test]
    async fn test_hidden_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join(".hiddenfile");
        fs::write(&file_path, b"test").unwrap();
        let result = is_filename_unique(&file_path).await.unwrap();
        assert_eq!(
            result,
            IsUnique::SuggestedAlternative(".hiddenfile_2".to_string())
        );
    }

    #[tokio::test]
    async fn test_set_file_mtime_async_sets_mtime() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("mtime_test.txt");
        fs::write(&file_path, b"test").unwrap();

        // Set mtime to a known value
        let unix_time = 1_600_000_000i64;
        set_file_mtime_async(&file_path, unix_time).await.unwrap();

        // Check mtime in a platform-independent way by using `modified()`
        // which returns a `SystemTime` that can be converted to UNIX seconds.
        let metadata = fs::metadata(&file_path).unwrap();
        use std::time::{SystemTime, UNIX_EPOCH};
        let modified: SystemTime = metadata.modified().unwrap();
        let actual_unix_secs = modified.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        assert_eq!(actual_unix_secs, unix_time);
    }

    #[tokio::test]
    async fn test_set_file_mtime_async_nonexistent_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("does_not_exist.txt");
        let result = set_file_mtime_async(&file_path, 1_600_000_000).await;
        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_atomic_write_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        let path = dir.path().join("secret.pb");
        let tmp_path = dir.path().join("secret.pb.temp");

        atomic_write(path.clone(), tmp_path.clone(), b"payload")
            .await
            .unwrap();
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, OWNER_ONLY_MODE, "fresh write must be owner-only");
        assert_eq!(fs::read(&path).unwrap(), b"payload");

        // Overwriting an existing target stays owner-only: the temp file is
        // created fresh and the rename replaces the old inode.
        atomic_write(path.clone(), tmp_path, b"payload2")
            .await
            .unwrap();
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, OWNER_ONLY_MODE, "overwrite must stay owner-only");
        assert_eq!(fs::read(&path).unwrap(), b"payload2");
    }

    #[test]
    fn a_long_non_latin_name_is_truncated_not_crashed() {
        // `String::truncate` panics off a character boundary, and any title
        // long enough to need truncating in a non-Latin script lands there.
        let title = "داستان کامل سامانه نان ".repeat(12);
        assert!(title.len() > MAX_FILENAME_BYTES);

        let out = cleanup_filename(&title, false);
        assert!(out.len() <= MAX_FILENAME_BYTES);
        assert!(!out.is_empty());
        // Still valid text, and still the beginning of the title.
        assert!(title.starts_with(&out));
    }

    #[test]
    fn truncation_does_not_leave_a_trailing_dot_or_space() {
        // Windows rejects both, and cutting a long name can expose either.
        let title = format!("{}. more", "a".repeat(MAX_FILENAME_BYTES - 1));
        let out = cleanup_filename(&title, false);
        assert!(!out.ends_with('.') && !out.ends_with(' '), "got {out:?}");
    }

    #[test]
    fn reserved_windows_names_are_escaped_even_with_an_extension() {
        // `NUL.mkv` is as reserved as `NUL`, and a media file always has an
        // extension — checking only the whole string missed every real case.
        assert_eq!(cleanup_filename("NUL.mkv", false), "NUL.mkv_");
        assert_eq!(cleanup_filename("con.txt", false), "con.txt_");
        assert_eq!(cleanup_filename("COM1.mp4", false), "COM1.mp4_");
        // Names that merely start with those letters are fine.
        assert_eq!(cleanup_filename("console.log", false), "console.log");
        assert_eq!(cleanup_filename("NULL.mkv", false), "NULL.mkv");
    }

    #[test]
    fn a_name_that_sanitises_to_nothing_falls_back() {
        // Joining an empty component onto a directory yields the directory
        // itself, so this must never return "".
        for input in ["...", "   ", ".", " . . ", ""] {
            assert_eq!(
                cleanup_filename(input, false),
                FALLBACK_FILENAME,
                "{input:?} must not sanitise to an empty name"
            );
        }
        // A name made only of forbidden characters still has content.
        assert_eq!(cleanup_filename("///", false), "___");
    }

    #[test]
    fn transliteration_is_off_unless_asked_for() {
        // The default must stay byte-identical: the sanitised title is the
        // per-download directory name, so changing it strands partial data.
        assert_eq!(cleanup_filename("Café Münster", false), "Café Münster");
        assert_eq!(cleanup_filename("日本語", false), "日本語");
    }

    #[test]
    fn transliteration_reaches_every_script() {
        assert_eq!(cleanup_filename("Café Münster", true), "Cafe Munster");
        assert_eq!(cleanup_filename("Приветствие", true), "Privetstvie");
        assert_eq!(cleanup_filename("Tiếng Việt", true), "Tieng Viet");
        for name in ["日本語", "한국어", "Ελληνικά", "داستان"] {
            let out = cleanup_filename(name, true);
            assert!(out.is_ascii(), "{name} transliterated to {out:?}");
            assert_ne!(out, FALLBACK_FILENAME, "{name} produced nothing");
        }
    }

    #[test]
    fn transliteration_output_is_still_sanitised() {
        // Transliteration runs first precisely because it can emit characters
        // the sanitiser then has to deal with — a colon is illegal on Windows,
        // and a name that transliterates to nothing must still get a name.
        assert!(!cleanup_filename("🎬 ⁄ 🎵", true).contains([':', '/', '\\']));
        assert_eq!(cleanup_filename("。。。", true), FALLBACK_FILENAME);
    }

    #[test]
    fn truncation_cannot_empty_a_name() {
        // The leading trim guarantees a non-dot first character, so cutting
        // and re-trimming always leaves at least that one.
        let input = format!("a{}b", ".".repeat(300));
        assert_eq!(cleanup_filename(&input, false), "a");
    }

    #[test]
    fn test_dunce_canonicalize_and_simplified() {
        let temp = tempdir().unwrap();
        let path = temp.path();
        let canon = canonicalize(path).expect("canonicalize must succeed");
        let simp = simplified(&canon);
        assert!(!simp.as_os_str().is_empty());
        #[cfg(windows)]
        {
            // For a normal short temp path, simplified/canonicalize strips \\?\
            let s = canon.to_string_lossy();
            assert!(!s.starts_with(r"\\?\"), "short path should not start with verbatim prefix: {}", s);
        }
    }
}
