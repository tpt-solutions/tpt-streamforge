# Install the prebuilt tptforge binary from a GitHub release (Windows x64).
#   irm https://github.com/tpt-solutions/tpt-streamforge/releases/latest/download/install.ps1 -OutFile install.ps1
#   Get-Content install.ps1   # inspect first, then: .\install.ps1
# Env: TPTFORGE_VERSION (default: latest, e.g. v0.1.0), TPTFORGE_INSTALL_DIR
# (default: $env:LOCALAPPDATA\tptforge\bin). Verified against SHA256SUMS.
$ErrorActionPreference = 'Stop'

$repo = 'tpt-solutions/tpt-streamforge'
$version = if ($env:TPTFORGE_VERSION) { $env:TPTFORGE_VERSION } else { 'latest' }
$dest = if ($env:TPTFORGE_INSTALL_DIR) { $env:TPTFORGE_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA 'tptforge\bin' }
$target = 'x86_64-pc-windows-msvc'

if ($version -eq 'latest') {
    $resp = Invoke-WebRequest -Uri "https://github.com/$repo/releases/latest" -MaximumRedirection 0 -SkipHttpErrorCheck
    $version = ($resp.Headers.Location | Select-Object -First 1) -replace '.*/', ''
}
$base = "https://github.com/$repo/releases/download/$version"
$name = "tptforge-$version-$target"
$tmp = Join-Path ([IO.Path]::GetTempPath()) ([Guid]::NewGuid())
New-Item -ItemType Directory -Path $tmp | Out-Null
try {
    Write-Host "Downloading $name.zip"
    Invoke-WebRequest "$base/$name.zip" -OutFile "$tmp\$name.zip"
    Invoke-WebRequest "$base/SHA256SUMS" -OutFile "$tmp\SHA256SUMS"

    $pattern = ' \*?' + [regex]::Escape("$name.zip") + '$'
    $line = Get-Content "$tmp\SHA256SUMS" | Where-Object { $_ -match $pattern } | Select-Object -First 1
    if (-not $line) { throw "no checksum for $name.zip in SHA256SUMS" }
    $expected = ($line -split '\s+')[0]
    $actual = (Get-FileHash "$tmp\$name.zip" -Algorithm SHA256).Hash
    if ($expected -ne $actual) { throw "checksum mismatch (expected $expected, got $actual)" }

    Expand-Archive "$tmp\$name.zip" -DestinationPath $tmp
    New-Item -ItemType Directory -Force -Path $dest | Out-Null
    Copy-Item "$tmp\$name\tptforge.exe" (Join-Path $dest 'tptforge.exe') -Force
    Write-Host "Installed $(Join-Path $dest 'tptforge.exe')"
    if (($env:PATH -split ';') -notcontains $dest) { Write-Host "Note: $dest is not on your PATH" }
} finally {
    Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
}
