# odl installer for Windows (PowerShell 5+).
# Usage:
#   irm https://raw.githubusercontent.com/jd1378/odl/main/tools/install.ps1 | iex
# Params via env:
#   $env:ODL_INSTALL_DIR  install directory (default: %LOCALAPPDATA%\Programs\odl)
#   $env:ODL_VERSION      tag (default: latest)

$ErrorActionPreference = 'Stop'

$Repo = 'jd1378/odl'
$InstallDir = if ($env:ODL_INSTALL_DIR) { $env:ODL_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA 'Programs\odl' }
$Version = if ($env:ODL_VERSION) { $env:ODL_VERSION } else { 'latest' }

$arch = (Get-CimInstance Win32_Processor).Architecture
# 0=x86, 9=x64, 12=arm64
switch ($arch) {
  9  { $target = 'x86_64-pc-windows-msvc' }
  12 { $target = 'aarch64-pc-windows-msvc' }
  0  { $target = 'i686-pc-windows-msvc' }
  default { throw "unsupported arch: $arch" }
}

if ($Version -eq 'latest') {
  $rel = Invoke-RestMethod "https://api.github.com/repos/$Repo/releases/latest" -Headers @{ 'User-Agent' = 'odl-installer' }
  $tag = $rel.tag_name
} else {
  $tag = $Version
}

$asset = "odl-$tag-$target.zip"
$url = "https://github.com/$Repo/releases/download/$tag/$asset"

$tmp = Join-Path ([System.IO.Path]::GetTempPath()) ([System.IO.Path]::GetRandomFileName())
New-Item -ItemType Directory -Path $tmp | Out-Null
$zip = Join-Path $tmp $asset

Write-Host "downloading $asset"
Invoke-WebRequest -Uri $url -OutFile $zip -UseBasicParsing

Expand-Archive -Path $zip -DestinationPath $tmp -Force
$exe = Get-ChildItem -Path $tmp -Recurse -Filter 'odl.exe' | Select-Object -First 1
if (-not $exe) { throw "odl.exe not found in $asset" }

New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
Copy-Item -Force $exe.FullName (Join-Path $InstallDir 'odl.exe')
Remove-Item -Recurse -Force $tmp

Write-Host "installed: $(Join-Path $InstallDir 'odl.exe')"

$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
if (-not ($userPath -split ';' | Where-Object { $_ -ieq $InstallDir })) {
  [Environment]::SetEnvironmentVariable('Path', "$userPath;$InstallDir", 'User')
  Write-Host "added $InstallDir to user PATH (open a new shell to use 'odl')"
}
