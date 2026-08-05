//! Integration tests for the M5 filesystem service (`omnireach_host::fs`).
//!
//! Fixtures live in a uuid-suffixed directory under `std::env::temp_dir()`
//! rather than using `tempfile`, which is not a dev-dependency of this crate.
//! `Fixture` cleans up from `Drop`, so a panicking test still leaves nothing
//! behind.
//!
//! Three properties these tests exist to pin down:
//!
//! 1. **Transfers are byte-transparent.** Every payload here is deliberately
//!    invalid UTF-8. A transfer path that decodes anywhere would silently
//!    corrupt real files — the same trap the scrollback ring documents, with
//!    worse consequences, because a corrupted download is written to disk.
//! 2. **Resume is real.** A client that reconnects mid-upload asks the service
//!    how much arrived and continues from there; the result must be identical
//!    to an uninterrupted upload.
//! 3. **`remove` never recurses.** A non-empty directory must be refused, and
//!    a symlink must be unlinked rather than followed into the tree it points
//!    at.

use omnireach_host::fs::{FsError, FsService};
use std::path::{Path, PathBuf};
use uuid::Uuid;

// ---------------------------------------------------------------- fixtures

struct Fixture {
    root: PathBuf,
}

impl Fixture {
    fn new(label: &str) -> Self {
        let root = std::env::temp_dir().join(format!("omnireach-fs-{label}-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("fixture root should be creatable");
        Self { root }
    }

    fn path(&self, rel: &str) -> PathBuf {
        self.root.join(rel)
    }

    /// The same path as a `&str`, which is what the service API takes.
    fn s(&self, rel: &str) -> String {
        self.path(rel).to_string_lossy().into_owned()
    }

    fn root_str(&self) -> String {
        self.root.to_string_lossy().into_owned()
    }

    fn write(&self, rel: &str, bytes: &[u8]) -> PathBuf {
        let p = self.path(rel);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent).expect("parent should be creatable");
        }
        std::fs::write(&p, bytes).expect("fixture file should be writable");
        p
    }

    fn mkdir(&self, rel: &str) -> PathBuf {
        let p = self.path(rel);
        std::fs::create_dir_all(&p).expect("fixture dir should be creatable");
        p
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Best effort: a failed cleanup must not mask the real assertion.
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// The oracle for what the service should report as a canonical path.
///
/// This mirrors the normalization `fs.rs` performs rather than calling into it:
/// `std::fs::canonicalize` yields verbatim (`\\?\C:\…`) paths on Windows and
/// the service strips that prefix, so an oracle that did not strip it too would
/// only agree on unix.
fn canonical(p: &Path) -> String {
    let c = std::fs::canonicalize(p).expect("path should exist to be canonicalized");
    let s = c.to_string_lossy().into_owned();
    #[cfg(windows)]
    {
        if let Some(rest) = s.strip_prefix(r"\\?\UNC\") {
            return format!(r"\\{rest}");
        }
        if let Some(rest) = s.strip_prefix(r"\\?\") {
            return rest.to_string();
        }
    }
    s
}

/// Bytes that are emphatically **not** valid UTF-8: the run includes NUL, lone
/// continuation bytes (0x80–0xBF with no lead byte) and 0xFE/0xFF, which never
/// appear in UTF-8 at all. Any lossy decode on the transfer path mangles this.
///
/// It counts *down* from 0xFF rather than up from 0x00 so that even a very
/// short payload is invalid UTF-8. Counting up would make `binary_payload(10)`
/// plain ASCII, and the byte-transparency tests using it would quietly prove
/// nothing.
fn binary_payload(len: usize) -> Vec<u8> {
    let bytes: Vec<u8> = (0..len)
        .map(|i| 0xFFu8.wrapping_sub((i % 256) as u8))
        .collect();
    assert!(
        len == 0 || String::from_utf8(bytes.clone()).is_err(),
        "the payload must be invalid UTF-8 or this test proves nothing"
    );
    bytes
}

// ------------------------------------------------------------ browse: list

#[test]
fn list_reports_subdirectories_and_files_with_their_sizes() {
    let fx = Fixture::new("list");
    let svc = FsService::new();
    fx.mkdir("sub");
    fx.write("data.bin", &binary_payload(1234));

    let entries = svc.list(&fx.root_str()).expect("listing the fixture root");

    assert_eq!(entries.len(), 2, "expected exactly the two entries created");

    // Directories sort ahead of files so a browse pane reads sensibly.
    let dir = &entries[0];
    assert_eq!(dir.name, "sub");
    assert!(dir.is_dir);
    assert!(!dir.is_symlink);
    assert_eq!(dir.path, canonical(&fx.path("sub")));

    let file = &entries[1];
    assert_eq!(file.name, "data.bin");
    assert!(!file.is_dir);
    assert_eq!(file.size, 1234, "size must be the real byte length");
    assert_eq!(file.path, canonical(&fx.path("data.bin")));
    assert!(
        file.modified_ms.is_some(),
        "a file just written must have a readable mtime"
    );
}

#[test]
fn listed_paths_are_absolute_and_carry_no_dot_segments() {
    let fx = Fixture::new("abs");
    let svc = FsService::new();
    fx.write("a.bin", b"a");

    for e in svc.list(&fx.root_str()).unwrap() {
        assert!(
            Path::new(&e.path).is_absolute(),
            "entry path must be absolute: {}",
            e.path
        );
        assert!(
            !Path::new(&e.path)
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir)),
            "entry path must already be resolved: {}",
            e.path
        );
    }
}

#[test]
fn stat_describes_a_file_and_a_directory() {
    let fx = Fixture::new("stat");
    let svc = FsService::new();
    fx.mkdir("sub");
    fx.write("data.bin", &binary_payload(77));

    let file = svc.stat(&fx.s("data.bin")).unwrap();
    assert_eq!(file.name, "data.bin");
    assert!(!file.is_dir);
    assert_eq!(file.size, 77);
    assert_eq!(file.path, canonical(&fx.path("data.bin")));

    let dir = svc.stat(&fx.s("sub")).unwrap();
    assert_eq!(dir.name, "sub");
    assert!(dir.is_dir);
}

// -------------------------------------------------------- download: chunks

#[test]
fn reading_a_file_in_chunks_reassembles_it_byte_identically() {
    let fx = Fixture::new("download");
    let svc = FsService::new();
    let payload = binary_payload(9_871); // not a multiple of the chunk size
    fx.write("blob.bin", &payload);
    let target = fx.s("blob.bin");

    let mut got = Vec::new();
    let mut offset = 0u64;
    loop {
        let (chunk, total) = svc.read_chunk(&target, offset, 512).unwrap();
        assert_eq!(
            total,
            payload.len() as u64,
            "every chunk must report the whole file's size"
        );
        if chunk.is_empty() {
            break;
        }
        offset += chunk.len() as u64;
        got.extend_from_slice(&chunk);
    }

    assert_eq!(
        got, payload,
        "a chunked download must be byte-identical to the file on disk"
    );
}

#[test]
fn a_chunk_is_clamped_to_the_end_of_the_file() {
    let fx = Fixture::new("clamp");
    let svc = FsService::new();
    let payload = binary_payload(100);
    fx.write("small.bin", &payload);

    let (chunk, total) = svc.read_chunk(&fx.s("small.bin"), 60, 4096).unwrap();
    assert_eq!(total, 100);
    assert_eq!(chunk, payload[60..], "must return exactly the remainder");
}

#[test]
fn reading_past_the_end_returns_empty_rather_than_erroring() {
    let fx = Fixture::new("eof");
    let svc = FsService::new();
    fx.write("short.bin", &binary_payload(10));
    let target = fx.s("short.bin");

    // Exactly at EOF: the normal termination of a download loop.
    let (chunk, total) = svc.read_chunk(&target, 10, 512).unwrap();
    assert!(chunk.is_empty(), "reading at EOF must yield no bytes");
    assert_eq!(total, 10);

    // Well past it, which a buggy or racing client can ask for.
    let (chunk, total) = svc.read_chunk(&target, 9_000, 512).unwrap();
    assert!(chunk.is_empty(), "reading past EOF must yield no bytes");
    assert_eq!(total, 10);
}

#[test]
fn an_oversized_request_is_capped_and_still_reassembles() {
    // `len` reaches the service straight off the wire. Without a cap, one
    // request naming a large file would allocate the whole thing; with it, the
    // download simply takes another turn round the loop.
    let fx = Fixture::new("cap");
    let svc = FsService::new();
    let payload = binary_payload(omnireach_host::fs::MAX_CHUNK + 4_096);
    fx.write("big.bin", &payload);
    let target = fx.s("big.bin");

    let (first, total) = svc.read_chunk(&target, 0, usize::MAX).unwrap();
    assert_eq!(total, payload.len() as u64);
    assert_eq!(
        first.len(),
        omnireach_host::fs::MAX_CHUNK,
        "an unbounded request must be clamped to MAX_CHUNK"
    );

    // ...and the loop that any client already runs still completes the file.
    let (rest, _) = svc
        .read_chunk(&target, first.len() as u64, usize::MAX)
        .unwrap();
    let mut got = first;
    got.extend_from_slice(&rest);
    assert_eq!(got, payload, "capping must not lose or reorder bytes");
}

#[test]
fn reading_an_empty_file_terminates_immediately() {
    let fx = Fixture::new("empty");
    let svc = FsService::new();
    fx.write("nothing.bin", b"");

    let (chunk, total) = svc.read_chunk(&fx.s("nothing.bin"), 0, 512).unwrap();
    assert!(chunk.is_empty());
    assert_eq!(total, 0);
}

// ---------------------------------------------------------- upload: chunks

#[test]
fn writing_two_chunks_produces_the_whole_file() {
    let fx = Fixture::new("upload");
    let svc = FsService::new();
    let payload = binary_payload(4_096);
    let target = fx.s("uploaded.bin");
    let split = 1_500;

    svc.write_chunk(&target, 0, &payload[..split]).unwrap();
    svc.write_chunk(&target, split as u64, &payload[split..])
        .unwrap();

    let on_disk = std::fs::read(fx.path("uploaded.bin")).unwrap();
    assert_eq!(
        on_disk, payload,
        "an uploaded file must match the source byte for byte"
    );
}

#[test]
fn a_resumed_upload_matches_an_uninterrupted_one() {
    let fx = Fixture::new("resume");
    let svc = FsService::new();
    let payload = binary_payload(9_000);
    let target = fx.s("resumed.bin");

    // First attempt: two chunks land, then the connection dies.
    svc.write_chunk(&target, 0, &payload[..3_000]).unwrap();
    svc.write_chunk(&target, 3_000, &payload[3_000..5_000])
        .unwrap();

    // The client reconnects and asks how much actually arrived, which is the
    // whole point of resume: the offset comes from the file on disk, never
    // from what the client believes it sent.
    let resume_at = svc.stat(&target).unwrap().size;
    assert_eq!(resume_at, 5_000, "resume point must be read from disk");

    // ...and continues from there rather than starting over.
    svc.write_chunk(&target, resume_at, &payload[resume_at as usize..])
        .unwrap();

    let on_disk = std::fs::read(fx.path("resumed.bin")).unwrap();
    assert_eq!(
        on_disk, payload,
        "a resumed upload must be byte-identical to an uninterrupted one"
    );
    assert_eq!(on_disk.len(), payload.len(), "no bytes lost or duplicated");
}

#[test]
fn a_second_chunk_never_truncates_the_first() {
    // Guards the single most damaging way to open the file: `truncate(true)`
    // on chunk 2 silently destroys everything already uploaded.
    let fx = Fixture::new("notrunc");
    let svc = FsService::new();
    let target = fx.s("keep.bin");

    svc.write_chunk(&target, 0, &[0xFFu8; 512]).unwrap();
    svc.write_chunk(&target, 512, &[0xFEu8; 512]).unwrap();

    let on_disk = std::fs::read(fx.path("keep.bin")).unwrap();
    assert_eq!(on_disk.len(), 1_024);
    assert!(on_disk[..512].iter().all(|&b| b == 0xFF));
    assert!(on_disk[512..].iter().all(|&b| b == 0xFE));
}

#[test]
fn writing_over_an_existing_range_replaces_only_that_range() {
    let fx = Fixture::new("overwrite");
    let svc = FsService::new();
    let target = fx.s("patch.bin");
    svc.write_chunk(&target, 0, &[0xAAu8; 300]).unwrap();

    svc.write_chunk(&target, 100, &[0xBBu8; 100]).unwrap();

    let on_disk = std::fs::read(fx.path("patch.bin")).unwrap();
    assert_eq!(on_disk.len(), 300, "an in-place patch must not resize");
    assert!(on_disk[..100].iter().all(|&b| b == 0xAA));
    assert!(on_disk[100..200].iter().all(|&b| b == 0xBB));
    assert!(on_disk[200..].iter().all(|&b| b == 0xAA));
}

// ------------------------------------------------------------------ errors

#[test]
fn a_missing_path_is_not_found() {
    let fx = Fixture::new("missing");
    let svc = FsService::new();
    let ghost = fx.s("no-such-entry");

    assert!(matches!(svc.list(&ghost), Err(FsError::NotFound)));
    assert!(matches!(svc.stat(&ghost), Err(FsError::NotFound)));
    assert!(matches!(
        svc.read_chunk(&ghost, 0, 16),
        Err(FsError::NotFound)
    ));
    assert!(matches!(svc.remove(&ghost), Err(FsError::NotFound)));
}

#[test]
fn listing_a_file_is_not_a_directory() {
    let fx = Fixture::new("notdir");
    let svc = FsService::new();
    fx.write("plain.bin", b"x");

    let err = svc.list(&fx.s("plain.bin")).unwrap_err();
    assert!(
        matches!(err, FsError::NotADirectory),
        "listing a file must report NOT_A_DIRECTORY, got {err:?}"
    );
    assert_eq!(err.code(), "NOT_A_DIRECTORY");
}

#[test]
fn reading_a_directory_is_a_directory() {
    let fx = Fixture::new("isdir");
    let svc = FsService::new();
    fx.mkdir("folder");

    let err = svc.read_chunk(&fx.s("folder"), 0, 16).unwrap_err();
    assert!(
        matches!(err, FsError::IsADirectory),
        "downloading a directory must report IS_A_DIRECTORY, got {err:?}"
    );
    assert_eq!(err.code(), "IS_A_DIRECTORY");
}

#[test]
fn a_relative_path_is_rejected_rather_than_resolved_against_the_process_cwd() {
    let svc = FsService::new();
    assert!(matches!(
        svc.list("relative/path"),
        Err(FsError::InvalidPath(_))
    ));
    assert!(matches!(svc.stat(""), Err(FsError::InvalidPath(_))));
}

#[test]
fn error_codes_are_stable_for_the_wire() {
    // transport.rs sends these verbatim as the control-frame error code, so
    // they are API surface and cannot drift silently.
    assert_eq!(FsError::NotFound.code(), "NOT_FOUND");
    assert_eq!(FsError::PermissionDenied.code(), "PERMISSION_DENIED");
    assert_eq!(FsError::NotADirectory.code(), "NOT_A_DIRECTORY");
    assert_eq!(FsError::IsADirectory.code(), "IS_A_DIRECTORY");
    assert_eq!(FsError::InvalidPath("x".into()).code(), "INVALID_PATH");
    assert_eq!(FsError::Io("x".into()).code(), "IO");
}

// ------------------------------------------------------------ mutate: tree

#[test]
fn mkdir_creates_a_directory_that_then_lists() {
    let fx = Fixture::new("mkdir");
    let svc = FsService::new();

    svc.mkdir(&fx.s("fresh")).unwrap();

    let entry = svc.stat(&fx.s("fresh")).unwrap();
    assert!(entry.is_dir);
    assert!(svc.list(&fx.s("fresh")).unwrap().is_empty());
}

#[test]
fn remove_refuses_a_non_empty_directory_and_leaves_it_intact() {
    let fx = Fixture::new("rmdir");
    let svc = FsService::new();
    fx.mkdir("tree");
    fx.write("tree/child.bin", b"precious");

    let err = svc.remove(&fx.s("tree")).unwrap_err();
    assert!(
        format!("{err}").contains("not empty"),
        "the error must say why it refused, got: {err}"
    );

    // The whole point: nothing was deleted.
    assert!(fx.path("tree").is_dir(), "the directory must survive");
    assert_eq!(
        std::fs::read(fx.path("tree/child.bin")).unwrap(),
        b"precious",
        "the contents must survive untouched"
    );
}

#[test]
fn remove_deletes_a_file_and_then_the_emptied_directory() {
    let fx = Fixture::new("rm");
    let svc = FsService::new();
    fx.mkdir("tree");
    fx.write("tree/child.bin", b"gone soon");

    svc.remove(&fx.s("tree/child.bin")).unwrap();
    assert!(!fx.path("tree/child.bin").exists());

    svc.remove(&fx.s("tree")).unwrap();
    assert!(!fx.path("tree").exists());
}

#[test]
fn rename_moves_an_entry_and_preserves_its_bytes() {
    let fx = Fixture::new("rename");
    let svc = FsService::new();
    let payload = binary_payload(600);
    fx.write("before.bin", &payload);

    svc.rename(&fx.s("before.bin"), &fx.s("after.bin")).unwrap();

    assert!(!fx.path("before.bin").exists(), "the old name must be gone");
    assert_eq!(
        std::fs::read(fx.path("after.bin")).unwrap(),
        payload,
        "renaming must not touch the contents"
    );
}

#[test]
fn renaming_a_missing_entry_is_not_found() {
    let fx = Fixture::new("rename-missing");
    let svc = FsService::new();
    let err = svc
        .rename(&fx.s("no-such-file"), &fx.s("dest"))
        .unwrap_err();
    assert!(matches!(err, FsError::NotFound));
    assert!(
        !fx.path("dest").exists(),
        "a failed rename must not create the destination"
    );
}

// -------------------------------------------------------- canonicalization

#[test]
fn a_path_containing_dot_dot_resolves_to_the_same_entry() {
    // Canonicalization here guards against OUR bugs — a malformed `..` join
    // reaching the wrong file — not against the user, who already has a shell.
    let fx = Fixture::new("dotdot");
    let svc = FsService::new();
    fx.mkdir("x");
    fx.write("x/inside.bin", b"here");

    let direct = svc.stat(&fx.s("x/inside.bin")).unwrap();
    let detoured = svc.stat(&fx.s("x/../x/inside.bin")).unwrap();

    assert_eq!(
        detoured.path, direct.path,
        "`..` must be resolved against the filesystem, not left in the path"
    );
    assert_eq!(detoured.name, "inside.bin");

    // The same must hold for a directory listing.
    let a = svc.list(&fx.s("x")).unwrap();
    let b = svc.list(&fx.s("x/../x")).unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].path, b[0].path);
}

#[test]
fn a_dot_dot_upload_target_is_resolved_before_it_is_created() {
    let fx = Fixture::new("dotdot-write");
    let svc = FsService::new();
    fx.mkdir("x");

    svc.write_chunk(&fx.s("x/../x/new.bin"), 0, b"landed")
        .unwrap();

    assert_eq!(std::fs::read(fx.path("x/new.bin")).unwrap(), b"landed");
    assert_eq!(
        svc.stat(&fx.s("x/new.bin")).unwrap().path,
        canonical(&fx.path("x/new.bin"))
    );
}

// -------------------------------------------------------------- roots/home

#[test]
fn roots_are_absolute_and_listable() {
    let svc = FsService::new();
    let roots = svc.roots();
    assert!(!roots.is_empty(), "there is always at least one root");

    for r in &roots {
        assert!(Path::new(r).is_absolute(), "root must be absolute: {r}");
        let entry = svc.stat(r).unwrap_or_else(|e| panic!("stat({r}): {e}"));
        assert!(entry.is_dir, "a root must be a directory: {r}");
    }
}

#[test]
fn home_when_present_is_an_absolute_directory() {
    let svc = FsService::new();
    if let Some(home) = svc.home() {
        assert!(Path::new(&home).is_absolute(), "home must be absolute");
        assert!(
            svc.stat(&home).unwrap().is_dir,
            "home must resolve to a directory"
        );
    }
}

// ------------------------------------------------------------ symlinks (unix)

#[cfg(unix)]
#[test]
fn removing_a_symlink_unlinks_it_without_following_it() {
    // The catastrophic case `remove` must never allow: deleting a link that
    // points at a populated tree must not touch the tree.
    let fx = Fixture::new("symlink-rm");
    let svc = FsService::new();
    fx.mkdir("real");
    fx.write("real/keep.bin", b"do not delete me");
    std::os::unix::fs::symlink(fx.path("real"), fx.path("link")).unwrap();

    svc.remove(&fx.s("link")).unwrap();

    assert!(!fx.path("link").exists(), "the link itself must be gone");
    assert!(fx.path("real").is_dir(), "the target must survive");
    assert_eq!(
        std::fs::read(fx.path("real/keep.bin")).unwrap(),
        b"do not delete me",
        "the target's contents must survive"
    );
}

#[cfg(unix)]
#[test]
fn a_symlink_is_reported_as_one_while_still_describing_its_target() {
    let fx = Fixture::new("symlink-stat");
    let svc = FsService::new();
    fx.write("real.bin", &binary_payload(42));
    std::os::unix::fs::symlink(fx.path("real.bin"), fx.path("alias.bin")).unwrap();

    let entry = svc.stat(&fx.s("alias.bin")).unwrap();
    assert!(entry.is_symlink, "a symlink must be flagged as one");
    assert!(!entry.is_dir);
    assert_eq!(entry.size, 42, "size should follow through to the target");
    assert_eq!(
        entry.name, "alias.bin",
        "the link keeps its own name, not the target's"
    );

    // ...and the same when reached through a listing.
    let listed = svc.list(&fx.root_str()).unwrap();
    let alias = listed.iter().find(|e| e.name == "alias.bin").unwrap();
    assert!(alias.is_symlink);
}

#[cfg(unix)]
#[test]
fn a_broken_symlink_is_still_listed() {
    // A browse pane that hides entries it cannot stat is lying about the
    // directory's contents.
    let fx = Fixture::new("symlink-broken");
    let svc = FsService::new();
    std::os::unix::fs::symlink(fx.path("nowhere"), fx.path("dangling")).unwrap();

    let listed = svc.list(&fx.root_str()).unwrap();
    let dangling = listed
        .iter()
        .find(|e| e.name == "dangling")
        .expect("a broken symlink must still appear in the listing");
    assert!(dangling.is_symlink);
}
