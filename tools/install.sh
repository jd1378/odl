#!/bin/sh
# odl installer. Detects OS/arch, downloads latest release tarball, installs binary.
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/jd1378/odl/main/tools/install.sh | sh
#   curl -fsSL https://raw.githubusercontent.com/jd1378/odl/main/tools/install.sh | sh -s -- --dir /usr/local/bin
# Env:
#   ODL_INSTALL_DIR  install directory (default: $HOME/.local/bin)
#   ODL_VERSION      tag to install (default: latest)

set -eu

REPO="jd1378/odl"
INSTALL_DIR="${ODL_INSTALL_DIR:-$HOME/.local/bin}"
VERSION="${ODL_VERSION:-latest}"

while [ $# -gt 0 ]; do
  case "$1" in
    --dir) INSTALL_DIR="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    -h|--help)
      cat <<EOF
odl installer
  --dir <path>      install directory (default: \$HOME/.local/bin)
  --version <tag>   release tag (default: latest)
EOF
      exit 0
      ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

err() { echo "error: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || err "missing required tool: $1"; }

need uname
need tar
if command -v curl >/dev/null 2>&1; then
  DL='curl -fsSL'
elif command -v wget >/dev/null 2>&1; then
  DL='wget -qO-'
else
  err "need curl or wget"
fi

OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
  Linux)
    LIBC=gnu
    if ldd --version 2>&1 | grep -qi musl || [ -n "$(ls /lib/ld-musl-* 2>/dev/null || true)" ]; then
      LIBC=musl
    fi
    case "$ARCH" in
      x86_64|amd64)        TARGET="x86_64-unknown-linux-${LIBC}" ;;
      aarch64|arm64)       TARGET="aarch64-unknown-linux-${LIBC}" ;;
      armv7l|armv7|armhf)
        if [ "$LIBC" = musl ]; then TARGET="arm-unknown-linux-musleabihf"
        else TARGET="arm-unknown-linux-gnueabihf"; fi ;;
      i386|i686)           TARGET="i686-unknown-linux-${LIBC}" ;;
      *) err "unsupported linux arch: $ARCH" ;;
    esac
    ASSET_EXT="tar.gz"
    ;;
  Darwin)
    case "$ARCH" in
      x86_64) TARGET="x86_64-apple-darwin" ;;
      arm64|aarch64) TARGET="aarch64-apple-darwin" ;;
      *) err "unsupported macos arch: $ARCH" ;;
    esac
    ASSET_EXT="tar.gz"
    ;;
  *) err "unsupported OS: $OS (use install.ps1 on windows)" ;;
esac

if [ "$VERSION" = latest ]; then
  TAG="$($DL "https://api.github.com/repos/$REPO/releases/latest" \
    | grep -m1 '"tag_name"' | sed -E 's/.*"tag_name"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/')"
  [ -n "$TAG" ] || err "could not resolve latest tag"
else
  TAG="$VERSION"
fi

ASSET="odl-${TAG}-${TARGET}.${ASSET_EXT}"
URL="https://github.com/$REPO/releases/download/${TAG}/${ASSET}"

echo "downloading $ASSET"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

$DL "$URL" > "$TMP/$ASSET" || err "download failed: $URL"
tar -xzf "$TMP/$ASSET" -C "$TMP"

BIN="$(find "$TMP" -type f -name odl -perm -u+x | head -n1)"
[ -n "$BIN" ] || BIN="$(find "$TMP" -type f -name odl | head -n1)"
[ -n "$BIN" ] || err "binary 'odl' not found in archive"

mkdir -p "$INSTALL_DIR"
install -m 0755 "$BIN" "$INSTALL_DIR/odl"

echo "installed: $INSTALL_DIR/odl"
case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *) echo "note: $INSTALL_DIR not in PATH. Add: export PATH=\"$INSTALL_DIR:\$PATH\"" ;;
esac

"$INSTALL_DIR/odl" --version 2>/dev/null || true
