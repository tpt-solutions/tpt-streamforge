#!/bin/sh
# Install the prebuilt tptforge binary from a GitHub release.
#   curl -fsSLO https://github.com/tpt-solutions/tpt-streamforge/releases/latest/download/install.sh
#   less install.sh && sh install.sh          # inspect first, then run
# Env: TPTFORGE_VERSION (default: latest, e.g. v0.1.0), TPTFORGE_INSTALL_DIR
# (default: $HOME/.local/bin). The archive is verified against the release's
# SHA256SUMS before anything is installed.
set -eu

REPO="tpt-solutions/tpt-streamforge"
VERSION="${TPTFORGE_VERSION:-latest}"
DEST="${TPTFORGE_INSTALL_DIR:-$HOME/.local/bin}"

os=$(uname -s)
arch=$(uname -m)
case "$os/$arch" in
  Linux/x86_64)  target=x86_64-unknown-linux-gnu ;;
  Darwin/arm64)  target=aarch64-apple-darwin ;;
  Darwin/x86_64) target=x86_64-apple-darwin ;;
  *) echo "unsupported platform $os/$arch; use 'cargo install tpt-stream-cli'" >&2; exit 1 ;;
esac

if [ "$VERSION" = latest ]; then
  VERSION=$(curl -fsSLI -o /dev/null -w '%{url_effective}' "https://github.com/$REPO/releases/latest" | sed 's|.*/||')
fi
base="https://github.com/$REPO/releases/download/$VERSION"
name="tptforge-$VERSION-$target"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "Downloading $name.tar.gz"
curl -fsSL -o "$tmp/$name.tar.gz" "$base/$name.tar.gz"
curl -fsSL -o "$tmp/SHA256SUMS" "$base/SHA256SUMS"

expected=$(grep " $name.tar.gz\$" "$tmp/SHA256SUMS" | awk '{print $1}')
[ -n "$expected" ] || { echo "no checksum for $name.tar.gz in SHA256SUMS" >&2; exit 1; }
if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$tmp/$name.tar.gz" | awk '{print $1}')
else
  actual=$(shasum -a 256 "$tmp/$name.tar.gz" | awk '{print $1}')
fi
[ "$expected" = "$actual" ] || { echo "checksum mismatch (expected $expected, got $actual)" >&2; exit 1; }

tar -xzf "$tmp/$name.tar.gz" -C "$tmp"
mkdir -p "$DEST"
install -m 755 "$tmp/$name/tptforge" "$DEST/tptforge"
echo "Installed $DEST/tptforge"
case ":$PATH:" in *":$DEST:"*) ;; *) echo "Note: $DEST is not on your PATH" ;; esac
