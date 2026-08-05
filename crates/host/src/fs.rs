//! Filesystem service — M5's file browser and chunked upload/download.
//!
//! # Scope is the whole filesystem, on purpose
//!
//! Per the design's recorded decision, access is **not** restricted to an
//! allowlist of roots. The same connection already hands out interactive
//! shells; a roots allowlist on top of that would be security theater, giving
//! the appearance of a boundary that `cd /` in the terminal next to it walks
//! straight through. The real boundary is Cloudflare Access, one layer up.
//!
//! Every path is nevertheless canonicalized before it is touched. That guard
//! is aimed at **our own bugs** — a malformed `..` join landing on the wrong
//! file, a client sending a path assembled from two half-joined halves — not
//! at the user, who is not the adversary here.
//!
//! # Transfers are byte-transparent
//!
//! `read_chunk` and `write_chunk` move `Vec<u8>` and `&[u8]`. Nothing on this
//! path decodes, validates or re-encodes. A single `String::from_utf8_lossy`
//! anywhere here would corrupt every binary file that crossed it, and unlike
//! the terminal's scrollback the corruption would be written to disk.
//!
//! # `remove` is never recursive
//!
//! See [`FsService::remove`]. This is a deliberate constraint, not an
//! omission.

use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;
use thiserror::Error;

/// One entry in a directory listing, or the result of a `stat`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirEntry {
    /// The final path component. For a filesystem root, which has no final
    /// component, this is the root itself (`/`, `C:\`).
    pub name: String,
    /// Absolute and canonicalized: no `.`, no `..`, no symlinked ancestors.
    pub path: String,
    pub is_dir: bool,
    pub is_symlink: bool,
    /// Byte length. Always `0` for directories — the platform's notion of a
    /// directory's "size" is a block count that means nothing in a browse UI.
    pub size: u64,
    /// Modification time in unix milliseconds. `None` if the platform does not
    /// record one, or if the timestamp predates the epoch.
    pub modified_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FsError {
    #[error("no such file or directory")]
    NotFound,
    #[error("permission denied")]
    PermissionDenied,
    #[error("not a directory")]
    NotADirectory,
    #[error("is a directory")]
    IsADirectory,
    // NOTE: these fields are NOT named `source`. thiserror reserves that name
    // for the error-source chain and requires it to implement
    // `std::error::Error`; a `String` there fails with a thoroughly misleading
    // "the method `as_dyn_error` exists for reference `&String`".
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error("io error: {0}")]
    Io(String),
}

impl FsError {
    /// The stable wire code for this error.
    ///
    /// `transport` puts this straight into the control frame's `error.code`,
    /// so these strings are API surface and must not drift.
    pub fn code(&self) -> &'static str {
        match self {
            FsError::NotFound => "NOT_FOUND",
            FsError::PermissionDenied => "PERMISSION_DENIED",
            FsError::NotADirectory => "NOT_A_DIRECTORY",
            FsError::IsADirectory => "IS_A_DIRECTORY",
            FsError::InvalidPath(_) => "INVALID_PATH",
            FsError::Io(_) => "IO",
        }
    }

    fn from_io(e: std::io::Error) -> Self {
        match e.kind() {
            ErrorKind::NotFound => FsError::NotFound,
            ErrorKind::PermissionDenied => FsError::PermissionDenied,
            ErrorKind::NotADirectory => FsError::NotADirectory,
            ErrorKind::IsADirectory => FsError::IsADirectory,
            _ => FsError::Io(e.to_string()),
        }
    }
}

/// The largest chunk [`FsService::read_chunk`] will materialize in one call,
/// however much the caller asks for.
///
/// `len` arrives from the wire, and the read is otherwise bounded only by the
/// file's own size — so a single request naming a multi-gigabyte file (a VM
/// image, a database) would allocate the whole thing at once. The cap costs a
/// caller nothing: the method only ever promised "up to `len`" bytes, and a
/// download loop already advances by the length it actually received.
///
/// 4 MiB matches the transport's send-queue high-water mark, so one chunk can
/// never on its own be what trips backpressure.
pub const MAX_CHUNK: usize = 4 * 1024 * 1024;

/// Whole-filesystem browse and transfer. Stateless: every call resolves its
/// own path, so there is no cursor, no open-handle table, and nothing to leak
/// when a connection drops mid-transfer.
#[derive(Debug, Default, Clone, Copy)]
pub struct FsService;

impl FsService {
    pub fn new() -> Self {
        Self
    }

    /// The filesystem roots a browser should offer as starting points.
    pub fn roots(&self) -> Vec<String> {
        platform_roots()
    }

    /// The user's home directory, canonicalized so it composes with paths
    /// returned by [`list`](Self::list) — on macOS `$HOME` routinely traverses
    /// the symlinked `/var`, and an uncanonicalized value would not compare
    /// equal to the same directory reached by browsing.
    pub fn home(&self) -> Option<String> {
        let home = dirs::home_dir()?;
        Some(match fs::canonicalize(&home) {
            Ok(c) => display_path(&c),
            Err(_) => display_path(&home),
        })
    }

    /// List a directory. Entries are sorted directories-first, then by name
    /// case-insensitively, so a pane's contents do not reshuffle between
    /// refreshes on a filesystem with unordered `readdir`.
    pub fn list(&self, path: &str) -> Result<Vec<DirEntry>, FsError> {
        let dir = resolve_existing(path)?;
        if !dir.is_dir() {
            return Err(FsError::NotADirectory);
        }

        let mut entries = Vec::new();
        for item in fs::read_dir(&dir).map_err(FsError::from_io)? {
            let item = item.map_err(FsError::from_io)?;
            // Join onto the already-canonical parent rather than canonicalizing
            // each child: one syscall instead of N, and it preserves a
            // symlink's own identity. Canonicalizing a child would rewrite it
            // to its target — and would fail outright for a broken one,
            // dropping the entry from a listing that should still show it.
            let child = dir.join(item.file_name());
            let is_symlink = item.file_type().map(|t| t.is_symlink()).unwrap_or(false);
            entries.push(describe(child, is_symlink));
        }

        entries.sort_by(|a, b| {
            b.is_dir
                .cmp(&a.is_dir)
                .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
                .then_with(|| a.name.cmp(&b.name))
        });
        Ok(entries)
    }

    /// Describe a single entry. A symlink is described as a symlink, but its
    /// type and size are read through to its target — that is what a user
    /// clicking it would get.
    pub fn stat(&self, path: &str) -> Result<DirEntry, FsError> {
        let p = resolve_leaf(path)?;
        let link = fs::symlink_metadata(&p).map_err(FsError::from_io)?;
        Ok(describe(p, link.is_symlink()))
    }

    /// Read up to `len` bytes at `offset`. Returns `(bytes, total_file_size)`.
    ///
    /// The total accompanies every chunk so a client can render progress
    /// without a prior `stat`, and so it notices a file that changed size
    /// underneath a transfer.
    ///
    /// Reading at or past EOF yields an **empty** chunk rather than an error:
    /// that is the ordinary way a download loop finds out it is done, and it
    /// keeps a racing client (asking for one chunk more than exists) off the
    /// error path.
    ///
    /// A single call never returns more than [`MAX_CHUNK`] bytes, whatever
    /// `len` says.
    pub fn read_chunk(
        &self,
        path: &str,
        offset: u64,
        len: usize,
    ) -> Result<(Vec<u8>, u64), FsError> {
        let p = resolve_existing(path)?;
        let meta = fs::metadata(&p).map_err(FsError::from_io)?;
        if meta.is_dir() {
            return Err(FsError::IsADirectory);
        }
        let total = meta.len();
        if len == 0 || offset >= total {
            return Ok((Vec::new(), total));
        }

        // Bounded by the remaining bytes, by what the caller asked for, and
        // finally by MAX_CHUNK — `len` comes off the wire, so without that
        // last clamp one request could allocate an entire large file.
        let want = usize::try_from(total - offset)
            .unwrap_or(usize::MAX)
            .min(len)
            .min(MAX_CHUNK);
        let mut file = fs::File::open(&p).map_err(FsError::from_io)?;
        file.seek(SeekFrom::Start(offset))
            .map_err(FsError::from_io)?;

        let mut buf = vec![0u8; want];
        let mut filled = 0;
        while filled < want {
            match file.read(&mut buf[filled..]) {
                // Short of `want`: the file shrank mid-read. Return what is
                // really there rather than zero-padding to the promised length.
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(FsError::from_io(e)),
            }
        }
        buf.truncate(filled);
        Ok((buf, total))
    }

    /// Write `bytes` at `offset`, creating the file if needed and extending it
    /// if the write runs past the end.
    ///
    /// `offset` is what makes resume work: a client that reconnects mid-upload
    /// `stat`s the partial file, learns how many bytes actually landed, and
    /// continues from there instead of re-sending everything.
    pub fn write_chunk(&self, path: &str, offset: u64, bytes: &[u8]) -> Result<(), FsError> {
        let p = resolve_leaf(path)?;
        if p.is_dir() {
            return Err(FsError::IsADirectory);
        }

        // `create(true)` with `truncate(false)`: chunk 2 of an upload must
        // extend the file, never erase chunk 1. `append(true)` would be just
        // as wrong — it ignores the seek, so a resumed upload would duplicate
        // bytes instead of filling the gap.
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&p)
            .map_err(FsError::from_io)?;
        file.seek(SeekFrom::Start(offset))
            .map_err(FsError::from_io)?;
        file.write_all(bytes).map_err(FsError::from_io)
    }

    /// Create one directory.
    ///
    /// Deliberately not `create_dir_all`: the browser creates a directory
    /// inside one it is already looking at, so a missing parent means the
    /// client sent a path it did not mean. Materializing the whole ancestor
    /// chain would bury that bug under a directory tree nobody asked for.
    pub fn mkdir(&self, path: &str) -> Result<(), FsError> {
        let p = resolve_leaf(path)?;
        fs::create_dir(&p).map_err(FsError::from_io)
    }

    /// Delete a file, a symlink, or an **empty** directory.
    ///
    /// **This never recurses, and that is deliberate.** Deleting a directory
    /// tree is catastrophic-by-accident from a browse UI — one misplaced click
    /// on a row the user thought was a file, and there is nothing to undo. So
    /// the API simply cannot express it: a non-empty directory is refused with
    /// an error that says why. A client that genuinely wants a tree gone must
    /// walk it and delete each entry, which makes the cost visible and keeps
    /// the decision on the client's side of the wire.
    ///
    /// A symlink is unlinked, never followed — removing a link that points at
    /// a populated tree must not touch the tree.
    pub fn remove(&self, path: &str) -> Result<(), FsError> {
        let p = resolve_leaf(path)?;
        let meta = fs::symlink_metadata(&p).map_err(FsError::from_io)?;

        if meta.is_symlink() {
            // `remove_file` unlinks any symlink on unix and a file symlink on
            // Windows; a Windows *directory* symlink needs `remove_dir`, which
            // removes the link alone and does not descend into the target.
            return fs::remove_file(&p)
                .or_else(|first| fs::remove_dir(&p).map_err(|_| FsError::from_io(first)));
        }

        if meta.is_dir() {
            return fs::remove_dir(&p).map_err(|e| match e.kind() {
                ErrorKind::DirectoryNotEmpty => FsError::Io(format!(
                    "directory is not empty: {} — remove is never recursive",
                    display_path(&p)
                )),
                _ => FsError::from_io(e),
            });
        }

        fs::remove_file(&p).map_err(FsError::from_io)
    }

    /// Rename or move an entry. Both sides are resolved first, so a `..` in
    /// either one lands where the filesystem says it lands.
    pub fn rename(&self, from: &str, to: &str) -> Result<(), FsError> {
        let src = resolve_leaf(from)?;
        // Check the source before resolving the destination, so a typo in the
        // source reports NOT_FOUND for the source rather than for the
        // destination's parent.
        fs::symlink_metadata(&src).map_err(FsError::from_io)?;
        let dst = resolve_leaf(to)?;
        fs::rename(&src, &dst).map_err(FsError::from_io)
    }
}

// ------------------------------------------------------------------- paths

/// Reject anything that is not an absolute path.
///
/// A relative path would resolve against the *host process's* working
/// directory, which no client knows and none of them mean. On Windows this
/// also rejects drive-relative paths like `\foo`, whose meaning depends on the
/// process's current drive — the same ambiguity.
fn require_absolute(raw: &str) -> Result<&Path, FsError> {
    if raw.is_empty() {
        return Err(FsError::InvalidPath("path is empty".into()));
    }
    let p = Path::new(raw);
    if !p.is_absolute() {
        return Err(FsError::InvalidPath(format!("path is not absolute: {raw}")));
    }
    Ok(p)
}

/// Canonicalize a path that must already exist, resolving `.`, `..` and every
/// symlinked ancestor against the real filesystem.
fn resolve_existing(raw: &str) -> Result<PathBuf, FsError> {
    let p = require_absolute(raw)?;
    fs::canonicalize(p).map_err(FsError::from_io)
}

/// Canonicalize a path whose **final component** may not exist yet: an upload
/// target, a `mkdir`, a rename destination.
///
/// The parent is canonicalized against the filesystem and the final component
/// is appended verbatim. Resolving the parent is what makes a `..` anywhere in
/// the path mean what the filesystem says it means rather than what naive
/// string surgery would produce.
///
/// A path whose last component is `..` has no `file_name`, so it falls through
/// to a full canonicalization — correct, since such a path can only denote an
/// already-existing directory.
fn resolve_leaf(raw: &str) -> Result<PathBuf, FsError> {
    let p = require_absolute(raw)?;
    match (p.parent(), p.file_name()) {
        (Some(parent), Some(name)) => {
            let parent = fs::canonicalize(parent).map_err(FsError::from_io)?;
            if !parent.is_dir() {
                return Err(FsError::NotADirectory);
            }
            Ok(parent.join(name))
        }
        _ => fs::canonicalize(p).map_err(FsError::from_io),
    }
}

/// Render a canonical path for the wire.
///
/// `fs::canonicalize` returns verbatim (`\\?\C:\…`) paths on Windows. They are
/// correct but alien: users do not recognize them and many Windows APIs and
/// tools that a client might hand them to reject them. Strip the prefix back
/// to the ordinary form, which `canonicalize` accepts again on the way in.
fn display_path(p: &Path) -> String {
    let s = p.to_string_lossy();
    #[cfg(windows)]
    {
        if let Some(rest) = s.strip_prefix(r"\\?\UNC\") {
            return format!(r"\\{rest}");
        }
        if let Some(rest) = s.strip_prefix(r"\\?\") {
            return rest.to_string();
        }
    }
    s.into_owned()
}

/// Build an entry for a path that is already canonical.
///
/// Metadata is read through the symlink so type and size describe what opening
/// the entry would give. When that fails — a broken symlink, or an entry whose
/// target is unreadable — it degrades to the link's own metadata and finally to
/// a zero-sized non-directory, rather than dropping the entry. A browse pane
/// that silently hides what it cannot stat is lying about the directory.
fn describe(path: PathBuf, is_symlink: bool) -> DirEntry {
    let meta = fs::metadata(&path)
        .ok()
        .or_else(|| fs::symlink_metadata(&path).ok());

    let name = match path.file_name() {
        Some(n) => n.to_string_lossy().into_owned(),
        // A filesystem root has no final component; name it after itself.
        None => display_path(&path),
    };
    let is_dir = meta.as_ref().is_some_and(|m| m.is_dir());

    DirEntry {
        name,
        path: display_path(&path),
        is_dir,
        is_symlink,
        size: if is_dir {
            0
        } else {
            meta.as_ref().map(|m| m.len()).unwrap_or(0)
        },
        modified_ms: meta.as_ref().and_then(modified_ms),
    }
}

fn modified_ms(meta: &fs::Metadata) -> Option<u64> {
    let t = meta.modified().ok()?;
    let since = t.duration_since(UNIX_EPOCH).ok()?;
    u64::try_from(since.as_millis()).ok()
}

#[cfg(windows)]
fn platform_roots() -> Vec<String> {
    // Probing beats a Windows API dependency here: an unmounted letter simply
    // fails to stat, which is exactly the test we want, and it keeps the
    // module free of platform crates.
    ('A'..='Z')
        .map(|c| format!("{c}:\\"))
        .filter(|p| fs::metadata(p).is_ok())
        .collect()
}

#[cfg(not(windows))]
fn platform_roots() -> Vec<String> {
    vec!["/".to_string()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    /// Fixtures go under `env::temp_dir()` with a uuid suffix rather than via
    /// `tempfile`, which is not a dev-dependency of this crate. Cleanup runs
    /// from `Drop` so a panicking test still leaves nothing behind.
    struct Fixture {
        root: PathBuf,
    }

    impl Fixture {
        fn new(label: &str) -> Self {
            let root =
                std::env::temp_dir().join(format!("omnireach-fsu-{label}-{}", Uuid::new_v4()));
            fs::create_dir_all(&root).unwrap();
            Self { root }
        }
        fn s(&self, rel: &str) -> String {
            self.root.join(rel).to_string_lossy().into_owned()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn error_codes_match_the_wire_contract() {
        assert_eq!(FsError::NotFound.code(), "NOT_FOUND");
        assert_eq!(FsError::PermissionDenied.code(), "PERMISSION_DENIED");
        assert_eq!(FsError::NotADirectory.code(), "NOT_A_DIRECTORY");
        assert_eq!(FsError::IsADirectory.code(), "IS_A_DIRECTORY");
        assert_eq!(FsError::InvalidPath("x".into()).code(), "INVALID_PATH");
        assert_eq!(FsError::Io("x".into()).code(), "IO");
    }

    #[test]
    fn io_errors_map_onto_the_typed_variants() {
        let mk = |k: ErrorKind| FsError::from_io(std::io::Error::new(k, "boom"));
        assert_eq!(mk(ErrorKind::NotFound), FsError::NotFound);
        assert_eq!(mk(ErrorKind::PermissionDenied), FsError::PermissionDenied);
        assert_eq!(mk(ErrorKind::NotADirectory), FsError::NotADirectory);
        assert_eq!(mk(ErrorKind::IsADirectory), FsError::IsADirectory);
        assert!(matches!(mk(ErrorKind::UnexpectedEof), FsError::Io(_)));
    }

    #[test]
    fn relative_and_empty_paths_are_rejected_before_any_syscall() {
        assert!(matches!(require_absolute(""), Err(FsError::InvalidPath(_))));
        assert!(matches!(
            require_absolute("relative/path"),
            Err(FsError::InvalidPath(_))
        ));
        assert!(matches!(
            require_absolute("./here"),
            Err(FsError::InvalidPath(_))
        ));
    }

    #[test]
    fn resolve_leaf_resolves_dot_dot_through_the_filesystem() {
        let fx = Fixture::new("resolve");
        fs::create_dir(fx.root.join("x")).unwrap();

        let direct = resolve_leaf(&fx.s("x/new.bin")).unwrap();
        let detoured = resolve_leaf(&fx.s("x/../x/new.bin")).unwrap();
        assert_eq!(direct, detoured);
        assert!(!direct.to_string_lossy().contains(".."));
    }

    #[test]
    fn resolve_leaf_reports_a_missing_parent_as_not_found() {
        let fx = Fixture::new("noparent");
        assert_eq!(
            resolve_leaf(&fx.s("absent-dir/file.bin")),
            Err(FsError::NotFound)
        );
    }

    #[test]
    fn resolve_leaf_rejects_a_file_used_as_a_parent() {
        let fx = Fixture::new("fileparent");
        fs::write(fx.root.join("plain"), b"x").unwrap();
        assert_eq!(
            resolve_leaf(&fx.s("plain/child")),
            Err(FsError::NotADirectory)
        );
    }

    #[test]
    fn roots_are_non_empty_and_absolute() {
        let roots = FsService::new().roots();
        assert!(!roots.is_empty());
        assert!(roots.iter().all(|r| Path::new(r).is_absolute()));
    }

    #[test]
    fn describe_reports_zero_size_for_a_directory() {
        let fx = Fixture::new("describe");
        let entry = describe(fs::canonicalize(&fx.root).unwrap(), false);
        assert!(entry.is_dir);
        assert_eq!(entry.size, 0, "a directory's block count is not a size");
        assert!(entry.modified_ms.is_some());
    }
}
