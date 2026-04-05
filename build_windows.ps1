$ErrorActionPreference = "Stop"

if (-not (Test-Path ".venv")) {
    python -m venv .venv
}

. .\.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
python -m pip install -r requirements-desktop.txt

if (Test-Path "build") {
    Remove-Item -Recurse -Force "build"
}

if (Test-Path "dist") {
    Remove-Item -Recurse -Force "dist"
}

pyinstaller --clean s3uploader_ui.spec

Write-Host ""
Write-Host "Build complete. Executable available at dist\S3Uploader.exe"
