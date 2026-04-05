# S3Uploader

Simple S3 transfer tool with two faces:

- a Python CLI for scripted uploads
- a desktop UI for browsing local files, browsing S3, and moving data without touching the terminal

The project is intentionally practical. It focuses on moving files reliably, handling multipart uploads, and giving a clearer desktop workflow for Windows users.

## What It Does

- uploads files and folders to Amazon S3
- preserves subfolder structure during directory uploads
- supports multipart uploads for large files
- resumes existing multipart uploads
- skips objects that already exist
- browses buckets and prefixes in a desktop UI
- creates S3 folders from the UI
- downloads and deletes S3 objects from the UI
- deletes S3 folders from the UI
- builds a Windows executable with PyInstaller

## Desktop UI

The desktop app is designed around a simple transfer flow:

- local file browser on the left
- transfer queue in the middle of the local pane
- S3 browser on the right
- action buttons above the S3 browser, closer to the AWS Console mental model
- log panel and transfer status at the bottom

Current UI features:

- queue files and folders before starting a transfer
- start a transfer with one button
- browse buckets
- browse S3 prefixes as folders
- create buckets
- create folders
- download objects
- delete objects
- delete folders

Current defaults for real AWS:

- region defaults to `eu-west-1`
- endpoint URL is left blank
- session token is optional unless you use temporary credentials

## Windows Quick Start

Copy these files into the same folder on your Windows machine:

- `s3uploader.py`
- `s3uploader_core.py`
- `s3uploader_ui.py`
- `requirements-desktop.txt`
- `build_windows.ps1`
- `s3uploader_ui.spec`
- `README.md`

You do not need:

- `.venv/`
- `tmp/`
- `.git/`
- `__pycache__/`

Install and run from source:

```powershell
python -m venv .venv
Set-ExecutionPolicy -Scope Process Bypass
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements-desktop.txt
python .\s3uploader_ui.py
```

If `python` is not found, install the Windows 64-bit Python installer from python.org and make sure `Add Python to PATH` is checked during setup.

## Build A Windows Executable

```powershell
.\build_windows.ps1
```

This builds:

```text
dist\S3Uploader.exe
```

## Create A Clean Release Bundle

From the repo:

```bash
python3 make_release_bundle.py
```

This creates:

- `release/s3-uploader-desktop/`
- `release/s3-uploader-desktop.zip`

You can also generate the same bundle from GitHub Actions with the `Release Bundle` workflow.
If you push a tag like `v1.0.0`, the workflow also attaches `s3-uploader-desktop.zip` to the GitHub release.

## Credentials

You can use the desktop app in three ways.

1. Enter credentials directly in the UI.
   Use `Access Key`, `Secret Key`, optional `Session Token`, and `Region`.
2. Use the standard AWS files.
   On Windows:
   `C:\Users\<your-user>\.aws\credentials`
   `C:\Users\<your-user>\.aws\config`
3. Use a local S3-compatible service such as MinIO.
   Example:
   `Access Key`: `minioadmin`
   `Secret Key`: `minioadmin`
   `Endpoint URL`: `http://localhost:9000`
   `Region`: `us-east-1`

## CLI

The CLI is still useful for scripted transfers and testing.

Example:

```bash
python3 ./s3uploader.py your-bucket-name -d /path/to/folder -p uploads/archive
```

Supported inputs:

- `-d` for a single file or a directory
- `-f` for a text file containing one source path per line

Important behavior:

- directory uploads are recursive
- relative paths are preserved in S3
- multipart chunk size must be at least `5 MB`
- lifecycle policy is off by default

## Lifecycle Policy

Lifecycle policy is not applied unless explicitly enabled.

When enabled, the project adds a bucket lifecycle rule scoped to the selected prefix. It is not stored on each object individually. In practice, that means objects uploaded under that prefix inherit the bucket rule.

## Local S3 Testing

You can test against MinIO:

```bash
docker run -d --name minio-s3-test -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address :9001

AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
python3 ./s3uploader.py test-bucket \
  -d /path/to/data \
  -p uploads/test-run \
  -s 5 \
  --endpoint-url http://localhost:9000 \
  --region us-east-1
```

## Python API

The upload and S3 browser logic lives in `s3uploader_core.py`.

Main pieces:

- `S3UploaderSettings`
- `S3Uploader`

That split keeps the CLI thin and makes the desktop UI possible without shelling out to the script.

## Project Layout

```text
s3uploader.py           CLI entry point
s3uploader_core.py      upload and S3 logic
s3uploader_ui.py        desktop UI
build_windows.ps1       Windows build helper
s3uploader_ui.spec      PyInstaller spec
make_release_bundle.py  release bundle helper
```

## What This README Is Not Claiming

This project is useful, but it is not pretending to be a full S3 management suite.

Today it does not try to be:

- a full replacement for the AWS Console
- a sync engine
- a multi-user enterprise admin tool

It is a focused uploader/browser with a cleaner Windows workflow.
