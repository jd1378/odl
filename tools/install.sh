#!/bin/sh
# odl installer. Detects OS/arch, downloads latest release tarball, installs binary.
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/jd1378/odl/main/tools/install.sh | sh
#   curl -fsSL https://raw.githubusercontent.com/jd1378/odl/main/tools/install.sh | sh -s -- --dir /usr/local/bin
# Env:
#   ODL_INSTALL_DIR  install directory (default: $HOME/.local/bin)
#   ODL_VERSION      tag to install (default: latest)
#   NO_COLOR         disable color output

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

# UI helpers.
if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
  C_BOLD=$(printf '\033[1m')
  C_DIM=$(printf '\033[2m')
  C_CYAN=$(printf '\033[36m')
  C_GREEN=$(printf '\033[32m')
  C_RED=$(printf '\033[31m')
  C_RESET=$(printf '\033[0m')
else
  C_BOLD= C_DIM= C_CYAN= C_GREEN= C_RED= C_RESET=
fi
step() { printf '%s==>%s %s%s%s\n' "$C_CYAN$C_BOLD" "$C_RESET" "$C_BOLD" "$*" "$C_RESET"; }
info() { printf '    %s%s%s\n' "$C_DIM" "$*" "$C_RESET"; }
ok()   { printf '%s✓%s %s\n' "$C_GREEN" "$C_RESET" "$*"; }
err()  { printf '%serror:%s %s\n' "$C_RED$C_BOLD" "$C_RESET" "$*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || err "missing required tool: $1"; }

# Two download modes: silent fetch-to-stdout, and progress download-to-file.
if command -v curl >/dev/null 2>&1; then
  HAVE=curl
elif command -v wget >/dev/null 2>&1; then
  HAVE=wget
else
  err "need curl or wget"
fi

fetch() { # url -> stdout (silent, for small API responses)
  case "$HAVE" in
    curl) curl -fsSL "$1" ;;
    wget) wget -qO- "$1" ;;
  esac
}
download() { # url, out-path (with progress)
  case "$HAVE" in
    curl)
      if [ -t 2 ]; then curl -fL --progress-bar -o "$2" "$1"
      else curl -fsSL -o "$2" "$1"; fi ;;
    wget)
      if [ -t 2 ]; then wget --show-progress -q -O "$2" "$1"
      else wget -q -O "$2" "$1"; fi ;;
  esac
}

need uname
need tar

step "Detecting platform"
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
info "$OS / $ARCH → $TARGET"

step "Resolving release"
if [ "$VERSION" = latest ]; then
  REL_JSON="$(fetch "https://api.github.com/repos/$REPO/releases/latest")" \
    || err "failed to query latest release"
  TAG="$(printf '%s\n' "$REL_JSON" \
    | grep '"tag_name"' \
    | head -n1 \
    | sed -E 's/.*"tag_name"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/')"
  [ -n "$TAG" ] || err "could not resolve latest tag"
  info "latest = $TAG"
else
  TAG="$VERSION"
  info "pinned = $TAG"
fi

ASSET="odl-${TAG}-${TARGET}.${ASSET_EXT}"
URL="https://github.com/$REPO/releases/download/${TAG}/${ASSET}"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

step "Downloading $ASSET"
info "$URL"
download "$URL" "$TMP/$ASSET" || err "download failed: $URL"

step "Extracting"
tar -xzf "$TMP/$ASSET" -C "$TMP"

BIN="$(find "$TMP" -type f -name odl -perm -u+x | head -n1)"
[ -n "$BIN" ] || BIN="$(find "$TMP" -type f -name odl | head -n1)"
[ -n "$BIN" ] || err "binary 'odl' not found in archive"

step "Installing to $INSTALL_DIR"
mkdir -p "$INSTALL_DIR"
install -m 0755 "$BIN" "$INSTALL_DIR/odl"
ok "installed: $INSTALL_DIR/odl"

case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *) info "note: $INSTALL_DIR not in PATH. Add: export PATH=\"$INSTALL_DIR:\$PATH\"" ;;
esac

VER_OUT="$("$INSTALL_DIR/odl" --version 2>/dev/null || true)"
[ -n "$VER_OUT" ] && info "$VER_OUT"
