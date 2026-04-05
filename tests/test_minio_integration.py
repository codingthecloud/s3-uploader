import os
import sys
import time
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from s3uploader_core import S3Uploader
from s3uploader_core import S3UploaderSettings


MINIO_ENDPOINT_URL = os.environ.get("MINIO_ENDPOINT_URL", "http://127.0.0.1:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_REGION = os.environ.get("MINIO_REGION", "us-east-1")


def _bucket_name():
    return f"s3uploader-test-{uuid.uuid4().hex[:12]}"


def _make_settings(bucket_name=None, **overrides):
    values = {
        "bucket_name": bucket_name,
        "chunk_size_mb": 5,
        "endpoint_url": MINIO_ENDPOINT_URL,
        "region_name": MINIO_REGION,
        "aws_access_key_id": MINIO_ACCESS_KEY,
        "aws_secret_access_key": MINIO_SECRET_KEY,
    }
    values.update(overrides)
    return S3UploaderSettings(**values)


def _make_uploader(bucket_name=None, **overrides):
    return S3Uploader(_make_settings(bucket_name=bucket_name, **overrides))


def _collect_events():
    events = []

    def callback(event_type, payload):
        events.append((event_type, payload))

    return events, callback


@pytest.fixture(scope="session")
def minio_ready():
    uploader = _make_uploader()
    deadline = time.time() + 30
    last_error = None
    while time.time() < deadline:
        try:
            uploader.list_buckets()
            return
        except Exception as err:  # pragma: no cover - only hit when service is starting up
            last_error = err
            time.sleep(1)
    raise RuntimeError(f"MinIO did not become ready: {last_error}")


@pytest.fixture
def bucket(minio_ready):
    uploader = _make_uploader()
    bucket_name = _bucket_name()
    uploader.create_bucket(bucket_name)
    try:
        yield bucket_name
    finally:
        try:
            uploader.delete_prefix(bucket_name, "")
        except Exception:
            pass
        try:
            uploader.delete_bucket(bucket_name)
        except Exception:
            pass


def test_upload_directory_and_skip_existing_objects(tmp_path, bucket):
    source_dir = tmp_path / "source"
    nested_dir = source_dir / "nested"
    nested_dir.mkdir(parents=True)
    (source_dir / "root.txt").write_text("root-data\n", encoding="utf-8")
    (nested_dir / "child.txt").write_text("child-data\n", encoding="utf-8")
    (nested_dir / "empty.txt").write_text("", encoding="utf-8")

    uploader = _make_uploader(bucket)

    events, callback = _collect_events()
    result = uploader.upload_paths_to_s3([str(source_dir)], bucket, "uploads/test-run", callback)

    assert result["files"] == 2
    top_level = uploader.list_prefix(bucket, "uploads/test-run")
    assert top_level["files"] == []
    assert [item["name"] for item in top_level["folders"]] == ["source"]

    source_listing = uploader.list_prefix(bucket, "uploads/test-run/source")
    assert [item["name"] for item in source_listing["files"]] == ["root.txt"]
    assert [item["name"] for item in source_listing["folders"]] == ["nested"]

    nested_listing = uploader.list_prefix(bucket, "uploads/test-run/source/nested")
    assert [item["name"] for item in nested_listing["files"]] == ["child.txt"]
    assert not any(name == "empty.txt" for name in [item["name"] for item in nested_listing["files"]])

    assert [event for event, _ in events if event == "file_completed"] == ["file_completed", "file_completed"]

    second_events, second_callback = _collect_events()
    second_result = uploader.upload_paths_to_s3([str(source_dir)], bucket, "uploads/test-run", second_callback)

    assert second_result["files"] == 2
    skipped = [payload["key"] for event, payload in second_events if event == "file_skipped"]
    assert sorted(skipped) == [
        "uploads/test-run/source/nested/child.txt",
        "uploads/test-run/source/root.txt",
    ]


def test_src_file_upload_and_bucket_rules(tmp_path, bucket):
    data_file = tmp_path / "payload.txt"
    data_file.write_text("lifecycle-data\n", encoding="utf-8")
    index_file = tmp_path / "files.txt"
    index_file.write_text(f"{data_file}\n", encoding="utf-8")

    uploader = _make_uploader(
        bucket,
        src_file=str(index_file),
        s3_prefix="archive",
    )

    uploader.upload_files_to_s3()

    listing = uploader.list_prefix(bucket, "archive")
    assert [item["name"] for item in listing["files"]] == ["payload.txt"]

    uploader.create_folder(bucket, "archive/manual-folder")
    after_folder = uploader.list_prefix(bucket, "archive")
    assert [item["name"] for item in after_folder["folders"]] == ["manual-folder"]

    with pytest.raises(ValueError, match="Bucket is not empty"):
        uploader.delete_bucket(bucket)

    uploader.delete_prefix(bucket, "archive/manual-folder")
    uploader.delete_prefix(bucket, "archive")
    assert uploader.list_prefix(bucket, "archive") == {"folders": [], "files": []}


def test_lifecycle_policy_reports_minio_storage_class_limit(bucket):
    uploader = _make_uploader(bucket)

    with pytest.raises(Exception, match="InvalidStorageClass"):
        uploader.apply_lifecycle_policy(bucket, "archive/")


def test_resume_multipart_upload_and_download_delete(tmp_path, bucket):
    source_file = tmp_path / "big.bin"
    source_file.write_bytes(os.urandom((5 * 1024 * 1024 * 2) + 12345))

    uploader = _make_uploader(bucket, chunk_size_mb=5)
    key = "resume/big.bin"
    multipart = uploader.s3_cli.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = multipart["UploadId"]

    with source_file.open("rb") as handle:
        first_part = handle.read(5 * 1024 * 1024)
    uploader.s3_cli.upload_part(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        PartNumber=1,
        Body=first_part,
        ContentLength=len(first_part),
    )

    events, callback = _collect_events()
    result = uploader.upload_paths_to_s3([str(source_file)], bucket, "resume", callback)

    assert result["files"] == 1
    resume_events = [payload for event, payload in events if event == "multipart_resumed"]
    assert len(resume_events) == 1
    assert resume_events[0]["next_part_number"] == 2

    download_target = tmp_path / "downloads" / "big.bin"
    uploader.download_object(bucket, key, download_target)
    assert download_target.read_bytes() == source_file.read_bytes()

    uploader.delete_object(bucket, key)
    exists = uploader.object_exists(key, bucket_name=bucket)
    assert exists["code"] == 1
