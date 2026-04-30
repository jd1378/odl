# odl uninstaller for Windows (PowerShell 5+).
# Usage:
#   irm https://raw.githubusercontent.com/jd1378/odl/main/tools/uninstall.ps1 | iex
# Env:
#   $env:ODL_INSTALL_DIR  directory where odl was installed (default: %LOCALAPPDATA%\Programs\odl)
#   $env:ODL_PURGE        if "1", also remove user config dir

$ErrorActionPreference = 'Stop'

$InstallDir = if ($env:ODL_INSTALL_DIR) { $env:ODL_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA 'Programs\odl' }
$Purge = ($env:ODL_PURGE -eq '1')

$exe = Join-Path $InstallDir 'odl.exe'
if (Test-Path $exe) {
  Remove-Item -Force $exe
  Write-Host "removed: $exe"
} else {
  $cmd = Get-Command odl.exe -ErrorAction SilentlyContinue
  if ($cmd) {
    Remove-Item -Force $cmd.Source
    Write-Host "removed: $($cmd.Source)"
  } else {
    Write-Warning "odl.exe not found in $InstallDir or PATH"
  }
}

# Drop empty install dir + remove from user PATH.
if ((Test-Path $InstallDir) -and -not (Get-ChildItem -Force $InstallDir)) {
  Remove-Item -Force $InstallDir
  Write-Host "removed empty dir: $InstallDir"
}

$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
if ($userPath) {
  $parts = $userPath -split ';' | Where-Object { $_ -and ($_ -ine $InstallDir) }
  $newPath = ($parts -join ';')
  if ($newPath -ne $userPath) {
    [Environment]::SetEnvironmentVariable('Path', $newPath, 'User')
    Write-Host "removed $InstallDir from user PATH"
  }
}

if ($Purge) {
  $cfg = Join-Path $env:APPDATA 'odl'
  if (Test-Path $cfg) {
    Remove-Item -Recurse -Force $cfg
    Write-Host "removed config: $cfg"
  }
}
