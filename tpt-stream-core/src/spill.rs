//! Private scratch directory for spill-to-disk stages (sort, join, group-by).
//!
//! Spill files hold copies of pipeline data. Placing them directly in the
//! shared system temp directory under predictable names (`…-join-0-p3.tptcol`)
//! let another local user pre-create a file or symlink there and have it
//! appended to or read back into the results. Instead every process gets one
//! directory with an unguessable name, created exclusively (so it cannot
//! pre-exist) and owner-only (0700) on Unix; the spill files inside inherit
//! that protection and can no longer collide with another process.

use std::hash::{BuildHasher, Hasher};
use std::path::PathBuf;
use std::sync::OnceLock;

/// An unpredictable 64-bit value without a RNG dependency: `RandomState` is
/// seeded from the OS, and we mix in the clock and pid for good measure.
fn unguessable() -> u64 {
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u32(std::process::id());
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    );
    h.finish()
}

fn create_private_dir() -> Option<PathBuf> {
    for _ in 0..16 {
        let dir = std::env::temp_dir().join(format!(
            "tpt-streamforge-{}-{:016x}",
            std::process::id(),
            unguessable()
        ));
        // `mode` is only set on Unix; without the cfg-gated call the binding
        // would be unused there and trip `-D warnings`.
        #[cfg_attr(not(unix), allow(unused_mut))]
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        // `create` (not `create_dir_all`) fails if the path already exists, so
        // we never adopt a directory someone else planted.
        if builder.create(&dir).is_ok() {
            return Some(dir);
        }
    }
    None
}

/// The per-process private spill directory.
///
/// Falls back to the system temp directory only if a private directory cannot
/// be created at all (e.g. read-only temp), in which case spilling would fail
/// on its own anyway.
pub(crate) fn spill_dir() -> &'static PathBuf {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| create_private_dir().unwrap_or_else(std::env::temp_dir))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spill_dir_is_private_and_unpredictable() {
        let dir = spill_dir();
        assert!(dir.is_dir());
        assert_ne!(
            dir,
            &std::env::temp_dir(),
            "must not spill into shared temp"
        );
        let name = dir.file_name().unwrap().to_string_lossy().into_owned();
        assert!(name.starts_with("tpt-streamforge-"), "{name}");
        // pid + 16 hex digits of entropy after the prefix.
        assert!(
            name.len() >= "tpt-streamforge-1-0123456789abcdef".len(),
            "{name}"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir).unwrap().permissions().mode();
            assert_eq!(mode & 0o077, 0, "group/other must have no access: {mode:o}");
        }
    }

    #[test]
    fn two_directories_never_collide() {
        assert_ne!(unguessable(), unguessable());
    }
}
