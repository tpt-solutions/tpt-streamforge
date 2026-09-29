# Packaging manifests

Templates for package managers. They are **not** published automatically.

After the `Release` workflow finishes for tag `vX.Y.Z`:

1. Download `SHA256SUMS` from the GitHub release.
2. Scoop (`scoop/tptforge.json`): set `version`, the URLs, and `hash` to the
   `tptforge-vX.Y.Z-x86_64-pc-windows-msvc.zip` line. With `checkver` /
   `autoupdate` present, a Scoop bucket's `checkver -u` can refresh this
   itself.
3. Homebrew (`homebrew/tptforge.rb`): set `version`, the URLs, and each
   `sha256` (`REPLACE_WITH_SHA256_*`) from the matching archive line.
4. Commit the result to your bucket / tap repository.

Verify provenance of any download with
`gh attestation verify <file> --repo tpt-solutions/tpt-streamforge`.
