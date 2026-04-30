#!/bin/sh
# odl uninstaller. Removes the odl binary.
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/jd1378/odl/main/tools/uninstall.sh | sh
#   curl -fsSL https://raw.githubusercontent.com/jd1378/odl/main/tools/uninstall.sh | sh -s -- --dir /usr/local/bin
# Env:
#   ODL_INSTALL_DIR  directory where odl was installed (default: $HOME/.local/bin)
#   ODL_PURGE        if "1", also remove user config dir

set -eu

INSTALL_DIR="${ODL_INSTALL_DIR:-$HOME/.local/bin}"
PURGE="${ODL_PURGE:-0}"

while [ $# -gt 0 ]; do
  case "$1" in
    --dir) INSTALL_DIR="$2"; shift 2 ;;
    --purge) PURGE=1; shift ;;
    -h|--help)
      cat <<EOF
odl uninstaller
  --dir <path>   directory where odl was installed (default: \$HOME/.local/bin)
  --purge        also remove user config (~/.config/odl, macOS app support, etc.)
EOF
      exit 0
      ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

removed=0

# Try the directory first, then fall back to PATH lookup.
TARGET="$INSTALL_DIR/odl"
if [ -e "$TARGET" ] || [ -L "$TARGET" ]; then
  rm -f "$TARGET"
  echo "removed: $TARGET"
  removed=1
fi

if [ "$removed" -eq 0 ]; then
  if FOUND="$(command -v odl 2>/dev/null)"; then
    rm -f "$FOUND" && echo "removed: $FOUND" && removed=1 || \
      echo "could not remove $FOUND (try with sudo)" >&2
  fi
fi

if [ "$removed" -eq 0 ]; then
  echo "odl not found in $INSTALL_DIR or PATH" >&2
fi

if [ "$PURGE" = "1" ]; then
  case "$(uname -s)" in
    Darwin) CFG_DIRS="$HOME/Library/Application Support/odl $HOME/.config/odl" ;;
    *)      CFG_DIRS="${XDG_CONFIG_HOME:-$HOME/.config}/odl" ;;
  esac
  for d in $CFG_DIRS; do
    if [ -d "$d" ]; then
      rm -rf "$d"
      echo "removed config: $d"
    fi
  done
fi
