# odl installer for Windows (PowerShell 5+).
# Usage:
#   irm https://raw.githubusercontent.com/jd1378/odl/main/tools/install.ps1 | iex
# Env:
#   $env:ODL_INSTALL_DIR  install directory (default: %LOCALAPPDATA%\Programs\odl)
#   $env:ODL_VERSION      tag (default: latest)
#   $env:NO_COLOR         disable color output

$ErrorActionPreference = 'Stop'

$Repo = 'jd1378/odl'
$InstallDir = if ($env:ODL_INSTALL_DIR) { $env:ODL_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA 'Programs\odl' }
$Version = if ($env:ODL_VERSION) { $env:ODL_VERSION } else { 'latest' }

# UI helpers. Use raw ESC for PS 5.1 compat (no `e escape).
$ESC = [char]27
$supportsVT = $false
try { $supportsVT = [bool]$Host.UI.RawUI -and ($PSVersionTable.PSVersion.Major -ge 6 -or $env:WT_SESSION -or $env:TERM_PROGRAM) } catch {}
if (-not $env:NO_COLOR -and $supportsVT) {
  $C_BOLD = "$ESC[1m"; $C_DIM = "$ESC[2m"; $C_CYAN = "$ESC[36m"
  $C_GREEN = "$ESC[32m"; $C_RED = "$ESC[31m"; $C_RESET = "$ESC[0m"
} else {
  $C_BOLD = ''; $C_DIM = ''; $C_CYAN = ''; $C_GREEN = ''; $C_RED = ''; $C_RESET = ''
}
function Step($msg) { Write-Host "$C_CYAN$C_BOLD==>$C_RESET $C_BOLD$msg$C_RESET" }
function Info($msg) { Write-Host "    $C_DIM$msg$C_RESET" }
function Ok($msg)   { Write-Host "$C_GREEN+$C_RESET $msg" }
function Fail($msg) { Write-Host "$C_RED${C_BOLD}error:$C_RESET $msg" -ForegroundColor Red; exit 1 }

# Show progress for Invoke-WebRequest. PS 5.1 progress is slow; suppress it but log steps ourselves.
$ProgressPreference = 'SilentlyContinue'

Step 'Detecting platform'
$archEnv = $env:PROCESSOR_ARCHITECTURE
switch ($archEnv) {
  'AMD64' { $target = 'x86_64-pc-windows-msvc' }
  'ARM64' { $target = 'aarch64-pc-windows-msvc' }
  'x86'   { $target = 'i686-pc-windows-msvc' }
  default { Fail "unsupported arch: $archEnv" }
}
Info "Windows / $archEnv -> $target"

Step 'Resolving release'
try {
  if ($Version -eq 'latest') {
    $rel = Invoke-RestMethod "https://api.github.com/repos/$Repo/releases/latest" -Headers @{ 'User-Agent' = 'odl-installer' }
    $tag = $rel.tag_name
    Info "latest = $tag"
  } else {
    $tag = $Version
    Info "pinned = $tag"
  }
} catch {
  Fail "failed to query release: $_"
}
if (-not $tag) { Fail 'could not resolve tag' }

$asset = "odl-$tag-$target.zip"
$url = "https://github.com/$Repo/releases/download/$tag/$asset"

$tmp = Join-Path ([System.IO.Path]::GetTempPath()) ([System.IO.Path]::GetRandomFileName())
New-Item -ItemType Directory -Path $tmp | Out-Null
$zip = Join-Path $tmp $asset

try {
  Step "Downloading $asset"
  Info $url
  Invoke-WebRequest -Uri $url -OutFile $zip -UseBasicParsing
  $size = (Get-Item $zip).Length
  Info ("downloaded {0:N0} bytes" -f $size)

  Step 'Extracting'
  Expand-Archive -Path $zip -DestinationPath $tmp -Force
  $exe = Get-ChildItem -Path $tmp -Recurse -Filter 'odl.exe' | Select-Object -First 1
  if (-not $exe) { Fail "odl.exe not found in $asset" }

  Step "Installing to $InstallDir"
  New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
  $dest = Join-Path $InstallDir 'odl.exe'
  Copy-Item -Force $exe.FullName $dest
  Ok "installed: $dest"
} finally {
  if (Test-Path $tmp) { Remove-Item -Recurse -Force $tmp }
}

Step 'Updating PATH'
$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
$inPath = $false
if ($userPath) {
  $inPath = ($userPath -split ';' | Where-Object { $_ -ieq $InstallDir }).Count -gt 0
}
if (-not $inPath) {
  $newPath = if ($userPath) { "$userPath;$InstallDir" } else { $InstallDir }
  [Environment]::SetEnvironmentVariable('Path', $newPath, 'User')
  Info "added $InstallDir to user PATH (open a new shell to use 'odl')"
} else {
  Info "$InstallDir already in user PATH"
}

try {
  $verOut = & $dest --version 2>$null
  if ($verOut) { Info $verOut }
} catch {}
