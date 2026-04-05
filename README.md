# S3Uploader

## Windows quick start
Copy these files into the same folder on your Windows machine:

- `s3uploader.py`
- `s3uploader_core.py`
- `s3uploader_ui.py`
- `requirements-desktop.txt`
- `build_windows.ps1`
- `s3uploader_ui.spec`
- `README.md`

You do not need to copy `.venv/`, `tmp/`, `.git/`, or `__pycache__/`.

To run the desktop app directly from source on Windows:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements-desktop.txt
python .\s3uploader_ui.py
```

If PowerShell blocks activation in that terminal, run:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
```

If `python` is not found, install the Windows 64-bit Python installer from python.org first and make sure `Add Python to PATH` is checked during setup.

To build a Windows executable:

```powershell
.\build_windows.ps1
```

That produces `dist\S3Uploader.exe`.

To generate a clean transfer bundle from this repo before copying to Windows:

```bash
python3 make_release_bundle.py
```

That creates:

- `release/s3-uploader-desktop/`
- `release/s3-uploader-desktop.zip`

## Requirements
Create a credential file.

~/.aws/credentials
```
[default]
aws_access_key_id=your_access_key
aws_secret_access_key=your_key
region = your_region
```

Example:
```
python3 ./s3uploader.py your-bucket-name -d /Users/Mark/photo-folder/
```

## Uploading files to AWS S3
S3Uploader was designed to upload large files into AWS S3.
The tool performs a multipart upload if the total size is greater than the chunk size.
If the upload is interrupted the tool automatically restores the upload from where it stopped.

S3Uploader can upload a list of files declared in a text file or upload the content of a folder.
Directory uploads are recursive and preserve subfolder paths in S3.

## S3 lifecycle configuration
No lifecycle rule is applied by default. If you want one, use `--apply-lifecycle-policy` to add a rule scoped to the selected prefix that moves objects to Deep Archive after 1 day and aborts incomplete multipart uploads after 1 day.

## Using module from cli

```
S3 uploader help

positional arguments:
  --bucket-name    The S3 bucket name where data will be copied to

optional arguments:
  -h, --help       show this help message and exit
  -p --s3-prefix   The prefix where the data will be stored in S3
  -s --chunk-size  The chunk size in MB. Default is 64MB. Minimum value is 5 MB
  --apply-lifecycle-policy
                   Apply a lifecycle rule for the selected prefix and abort incomplete uploads after 1 day
  --endpoint-url   Custom S3 endpoint URL, useful for local testing such as
                   http://localhost:9000
  --region         AWS region override. Defaults to the current AWS
                   configuration or us-east-1

Data to be copied. Provide at least one parameter:
  -d --src-dir     The directory to be copied to S3
  -f --src-file    The file containing a list of files to be copied to S3
```

## Use it as a Python class
```python
    # only mandatory parameter is the bucket_name
    settings = S3UploaderSettings(src_dir="your_source_directory",
                                  chunk_size_mb="the_size_of_chunks",
                                  bucket_name="the_bucket_name",
                                  src_file='your_index_file',
                                  s3_prefix="the_S3_prefix",
                                  apply_lifecycle_policy=False,
                                  endpoint_url=None,
                                  region_name=None)
    s3_uploader = S3Uploader(settings)
    
    # depending on how you init the S3Uploader you can either run:
    s3_uploader.upload_files_to_s3(src_dir='/your/dir/', 
                                   s3_prefix='prefix/in/S3',
                                   src_file="/home/sample/Documents/listofpaths.txt")
    # or just:
    s3_uploader.upload_files_to_s3()
```
### Arguments:            
- "src_dir" can be either a directory or a single file path.
- When "src_dir" is a directory, files are discovered recursively and uploaded using their path relative to that directory.
- "src_file" is the path to a text file containing a file path per line. 
- "s3_prefix" is the path on S3. Default path is the root folder of the S3 bucket. If you pass `photos`, it is normalized to `photos/`.
- "chunk_size_mb" must be at least 5 MB for valid S3 multipart uploads.
- "apply_lifecycle_policy" is optional and disabled by default.
- "endpoint_url" lets you target a local S3-compatible service for testing.
- "region_name" lets you override the AWS region.

## Runtime output
The CLI now logs a short upload summary, reports each file being processed, and shows multipart progress per chunk so interrupted uploads are easier to follow when resumed.

## Local testing
You can test against a local S3-compatible service such as MinIO:

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

## Desktop UI
A Windows-friendly desktop UI is now available in `s3uploader_ui.py`. It includes:

- a local file browser for selecting files or folders
- an explicit upload queue before you start an upload
- an S3 browser for listing buckets and browsing prefixes
- bucket creation
- folder creation in S3
- deletion of selected S3 files or prefixes
- an upload button wired to the same backend as the CLI
- a live log panel and upload progress

Install the UI dependency:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install boto3 PySide6
```

Launch the desktop app:

```bash
python3 s3uploader_ui.py
```

The desktop app lets you enter credentials or an endpoint URL directly, which is useful for Windows users and for local S3-compatible targets such as MinIO.

## Credentials
You can use the desktop app in three ways:

1. Enter credentials directly in the UI:
   `Access Key`, `Secret Key`, optional `Session Token`, and `Region`.
2. Use the standard AWS files on Windows:
   `C:\Users\<your-user>\.aws\credentials`
   `C:\Users\<your-user>\.aws\config`
   Then optionally enter `default` or another profile name in the `Profile` field.
3. Use a local S3-compatible service such as MinIO:
   `Access Key`: `minioadmin`
   `Secret Key`: `minioadmin`
   `Endpoint URL`: `http://localhost:9000`
   `Region`: `us-east-1`

For real AWS use, the desktop UI now defaults to:

- `Region`: `eu-west-1`
- `Endpoint URL`: empty, with a hint to leave it blank for AWS
- `Session Token`: optional unless you are using temporary credentials

## Windows packaging
To build a Windows executable, use the included PowerShell script:

```powershell
.\build_windows.ps1
```

This script creates a virtual environment if needed, installs the desktop dependencies from `requirements-desktop.txt`, and builds `dist\S3Uploader.exe` with PyInstaller using `s3uploader_ui.spec`.
