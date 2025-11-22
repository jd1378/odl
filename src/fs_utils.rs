use filetime::{FileTime, set_file_mtime};
use prost::Message;
use std::{io, path::Path, path::PathBuf};

use tokio::{fs::OpenOptions, io::AsyncWriteExt};

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
pub fn cleanup_filename(input: &str) -> String {
    let mut result = String::from(input);
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

    // avoid forbidden windows names by adding an underscore at the end if found
    let upper_result = result.to_ascii_uppercase();
    if FORBIDDEN_WINDOWS_NAMES
        .iter()
        .any(|&name| name == upper_result)
    {
        result.push('_');
    }

    if result.len() > 255 {
        result.truncate(255);
    }

    // remove non utf-8 chars
    result = String::from_utf8_lossy(result.as_bytes()).to_string();
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
    M::decode_length_delimited(&*buf).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

pub async fn atomic_replace(src: PathBuf, dst: PathBuf) -> io::Result<()> {
    tokio::task::spawn_blocking(move || atomicwrites::replace_atomic(&src, &dst))
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;

    Ok(())
}

/// Atomically writes the given bytes to the specified path.
/// Writes to a temporary file in the same directory and then renames it over the target file.
/// Ensures that either the entire file is written or not changed at all.
/// Truncates the tmp_file if it exists
pub async fn atomic_write(path: PathBuf, tmp_path: PathBuf, data: &[u8]) -> io::Result<()> {
    // Write to the temporary file
    {
        let mut tmp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .await?;
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

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_cleanup_filename_basic() {
        assert_eq!(
            cleanup_filename("normal_filename.txt"),
            "normal_filename.txt"
        );
        assert_eq!(
            cleanup_filename("file/with/slash.txt"),
            "file_with_slash.txt"
        );
        assert_eq!(
            cleanup_filename("file\\with\\backslash.txt"),
            "file_with_backslash.txt"
        );
        assert_eq!(
            cleanup_filename("file:with:colon.txt"),
            "file_with_colon.txt"
        );
        assert_eq!(
            cleanup_filename("file*with*asterisk.txt"),
            "file_with_asterisk.txt"
        );
        assert_eq!(
            cleanup_filename("file?with?question.txt"),
            "file_with_question.txt"
        );
        assert_eq!(
            cleanup_filename("file\"with\"quote.txt"),
            "file_with_quote.txt"
        );
        assert_eq!(cleanup_filename("file<with<less.txt"), "file_with_less.txt");
        assert_eq!(
            cleanup_filename("file>with>greater.txt"),
            "file_with_greater.txt"
        );
        assert_eq!(cleanup_filename("file|with|pipe.txt"), "file_with_pipe.txt");
        assert_eq!(
            cleanup_filename("file^with^caret.txt"),
            "file_with_caret.txt"
        );
    }

    #[test]
    fn test_cleanup_filename_trim() {
        assert_eq!(cleanup_filename("   filename.txt   "), "filename.txt");
        assert_eq!(cleanup_filename("...filename.txt..."), "filename.txt");
        assert_eq!(cleanup_filename("   ...filename.txt...   "), "filename.txt");
    }

    #[test]
    fn test_cleanup_filename_forbidden_windows_names() {
        for &name in FORBIDDEN_WINDOWS_NAMES {
            assert_eq!(cleanup_filename(name), format!("{name}_"));
            assert_eq!(
                cleanup_filename(&name.to_ascii_lowercase()),
                format!("{}_", &name.to_ascii_lowercase())
            );
        }
    }

    #[test]
    fn test_cleanup_filename_control_chars() {
        let input = "file\u{0000}name.txt";
        assert_eq!(cleanup_filename(input), "file_name.txt");
    }

    #[test]
    fn test_cleanup_filename_truncate() {
        let long_name = "a".repeat(300);
        let cleaned = cleanup_filename(&long_name);
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
        fs::write(&file_path, b"test").unwrap();
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
        std::fs::write(&file_path, b"test").unwrap();
        let result = is_filename_unique(&file_path).await;
        let _ = std::fs::remove_file(&file_path);
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

        // Check mtime
        let metadata = fs::metadata(&file_path).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(metadata.mtime(), unix_time);
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            let filetime = metadata.last_write_time();
            let expected = filetime::FileTime::from_unix_time(unix_time, 0).seconds();
            // Windows returns 100-nanosecond intervals since 1601-01-01, so just check it's close
            assert!((filetime as i64 - expected).abs() < 5);
        }
    }

    #[tokio::test]
    async fn test_set_file_mtime_async_nonexistent_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("does_not_exist.txt");
        let result = set_file_mtime_async(&file_path, 1_600_000_000).await;
        assert!(result.is_err());
    }
}
