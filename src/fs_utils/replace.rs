//! Renaming a finished temp file over its destination in one step, on disk
//! before the call returns.
//!
//! This replaces the `atomicwrites` crate, of which odl used this one
//! function. Its Windows backend handed `MoveFileExW` the path as given, and
//! Win32 limits a path without the `\\?\` prefix to `MAX_PATH` (260
//! characters). A long filename makes a work directory longer than that, so
//! every metadata write in it failed. std adds the prefix for its own calls,
//! but not for a Win32 call made directly.
//!
//! Otherwise this does what that function did: on Unix, `renameat` on the
//! open parent directories and then an fsync of each; on Windows,
//! `MoveFileExW` with write-through, now given the verbatim path.

use std::{io, path::Path};

/// Rename `src` over `dst`, replacing `dst` if it exists, and return once the
/// rename has reached the disk. A reader sees either the old file or the new
/// one, never a mix of the two.
pub(super) fn replace_atomic(src: &Path, dst: &Path) -> io::Result<()> {
    imp::replace_atomic(src, dst)
}

#[cfg(unix)]
mod imp {
    use std::{ffi::OsStr, fs::File, io, path::Path};

    pub(super) fn replace_atomic(src: &Path, dst: &Path) -> io::Result<()> {
        let (src_dir, src_name) = split(src)?;
        let (dst_dir, dst_name) = split(dst)?;
        // Opened before the rename: a directory that cannot be opened fails
        // the call before anything moves, and the rename and the syncs act on
        // the same directories even if a path to them changes meanwhile.
        let src_parent = File::open(src_dir)?;
        let dst_parent = if dst_dir == src_dir {
            None
        } else {
            Some(File::open(dst_dir)?)
        };
        rustix::fs::renameat(
            &src_parent,
            src_name,
            dst_parent.as_ref().unwrap_or(&src_parent),
            dst_name,
        )?;
        // rename(2) is atomic but not durable: the changed directory entries
        // reach the disk when their directories are synced.
        src_parent.sync_all()?;
        if let Some(dst_parent) = dst_parent {
            dst_parent.sync_all()?;
        }
        Ok(())
    }

    /// The directory holding `path`, `.` for a bare filename, and the name
    /// of `path` within it.
    pub(super) fn split(path: &Path) -> io::Result<(&Path, &OsStr)> {
        let name = path.file_name().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{} does not name a file", path.display()),
            )
        })?;
        let dir = match path.parent() {
            Some(dir) if !dir.as_os_str().is_empty() => dir,
            _ => Path::new("."),
        };
        Ok((dir, name))
    }
}

#[cfg(windows)]
mod imp {
    use std::{io, os::windows::ffi::OsStrExt, path::Path};

    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    pub(super) fn replace_atomic(src: &Path, dst: &Path) -> io::Result<()> {
        let (src, dst) = (long_path(src)?, long_path(dst)?);
        // SAFETY: both are NUL-terminated UTF-16 strings that outlive the call.
        let moved = unsafe {
            MoveFileExW(
                src.as_ptr(),
                dst.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if moved == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// `path` as Win32 accepts it past `MAX_PATH`: absolute, verbatim and
    /// NUL-terminated.
    fn long_path(path: &Path) -> io::Result<Vec<u16>> {
        let absolute: Vec<u16> = std::path::absolute(path)?
            .as_os_str()
            .encode_wide()
            .collect();
        if absolute.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "path contains a NUL character",
            ));
        }
        let mut wide = super::verbatim(&absolute);
        wide.push(0);
        Ok(wide)
    }
}

/// The verbatim form of an absolute Windows path, by the rules std applies to
/// its own calls: `C:\x` becomes `\\?\C:\x`, `\\server\share\x` becomes
/// `\\?\UNC\server\share\x`, `\\.\x` becomes `\\?\x`, and a path that is
/// already verbatim is left alone.
///
/// Win32 does not normalise a verbatim path, so this takes one that is
/// already normalised, as `std::path::absolute` returns it.
#[cfg(any(windows, test))]
fn verbatim(absolute: &[u16]) -> Vec<u16> {
    const SEP: u16 = b'\\' as u16;
    const QUERY: u16 = b'?' as u16;
    const DOT: u16 = b'.' as u16;
    const COLON: u16 = b':' as u16;
    const VERBATIM_PREFIX: &[u16] = &[SEP, SEP, QUERY, SEP];
    const UNC_PREFIX: &[u16] = &[
        SEP,
        SEP,
        QUERY,
        SEP,
        b'U' as u16,
        b'N' as u16,
        b'C' as u16,
        SEP,
    ];

    let (prefix, rest): (&[u16], &[u16]) = match absolute {
        [_, COLON, SEP, ..] => (VERBATIM_PREFIX, absolute),
        [SEP, SEP, DOT, SEP, rest @ ..] => (VERBATIM_PREFIX, rest),
        [SEP, SEP, QUERY, SEP, ..] | [SEP, QUERY, QUERY, SEP, ..] => (&[], absolute),
        [SEP, SEP, rest @ ..] => (UNC_PREFIX, rest),
        _ => (&[], absolute),
    };
    [prefix, rest].concat()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wide(s: &str) -> Vec<u16> {
        s.encode_utf16().collect()
    }

    #[test]
    fn verbatim_follows_the_prefix_rules_std_uses() {
        let cases = [
            (r"C:\dl\file", r"\\?\C:\dl\file"),
            (r"\\server\share\file", r"\\?\UNC\server\share\file"),
            (r"\\.\C:\file", r"\\?\C:\file"),
            (r"\\?\C:\file", r"\\?\C:\file"),
            (r"\??\C:\file", r"\??\C:\file"),
        ];
        for (input, expected) in cases {
            assert_eq!(verbatim(&wide(input)), wide(expected), "{input}");
        }
    }

    #[test]
    fn replaces_an_existing_destination() {
        let dir = tempfile::tempdir().unwrap();
        let (src, dst) = (dir.path().join("new.tmp"), dir.path().join("file"));
        std::fs::write(&src, b"new").unwrap();
        std::fs::write(&dst, b"old").unwrap();

        replace_atomic(&src, &dst).unwrap();

        assert_eq!(std::fs::read(&dst).unwrap(), b"new");
        assert!(!src.exists());
    }

    #[test]
    fn creates_a_missing_destination() {
        let dir = tempfile::tempdir().unwrap();
        let (src, dst) = (dir.path().join("new.tmp"), dir.path().join("file"));
        std::fs::write(&src, b"new").unwrap();

        replace_atomic(&src, &dst).unwrap();

        assert_eq!(std::fs::read(&dst).unwrap(), b"new");
    }

    #[test]
    fn keeps_a_unicode_name() {
        let dir = tempfile::tempdir().unwrap();
        let (src, dst) = (dir.path().join("Дмитрий.tmp"), dir.path().join("Дмитрий"));
        std::fs::write(&src, "Привет").unwrap();

        replace_atomic(&src, &dst).unwrap();

        assert_eq!(std::fs::read_to_string(&dst).unwrap(), "Привет");
    }

    #[cfg(unix)]
    #[test]
    fn a_bare_filename_lives_in_the_current_directory() {
        use std::{ffi::OsStr, path::Path};

        assert_eq!(
            imp::split(Path::new("file")).unwrap(),
            (Path::new("."), OsStr::new("file"))
        );
        assert_eq!(
            imp::split(Path::new("dir/file")).unwrap(),
            (Path::new("dir"), OsStr::new("file"))
        );
        assert!(imp::split(Path::new("/")).is_err());
        assert!(imp::split(Path::new("dir/..")).is_err());
    }

    /// The case that failed on Windows: a work directory named after a long
    /// filename, past `MAX_PATH`, which std creates without complaint.
    #[test]
    fn handles_a_path_longer_than_max_path() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("a".repeat(200)).join("b".repeat(100));
        std::fs::create_dir_all(&dir).unwrap();
        let (src, dst) = (dir.join("metadata.pb.tmp"), dir.join("metadata.pb"));
        assert!(dst.as_os_str().len() > 260);
        std::fs::write(&src, b"new").unwrap();

        replace_atomic(&src, &dst).unwrap();

        assert_eq!(std::fs::read(&dst).unwrap(), b"new");
    }
}
