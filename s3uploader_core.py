import hashlib
import logging
import math
import ntpath
import os
import re
from pathlib import Path

import boto3
import botocore
from botocore.config import Config


class S3UploaderSettings:
    MB = 1048576  # 1MB in bytes
    DEFAULT_CHUNK_SIZE = 64
    S3_PREFIX_REGEX = r"^([a-zA-Z0-9!\-_.*'()/]+/?)?$"
    S3_PREFIX_REGEX_ERR_MSG = "Invalid prefix. Valid values look like folder or folder1/folder2/."
    INPUT_DATA_ERR_MSG = "Provide at least either one or both of the input parameters: src_file, src_dir"

    def __init__(self,
                 s3_prefix=None,
                 chunk_size_mb=None,
                 src_dir=None,
                 bucket_name=None,
                 src_file=None,
                 apply_lifecycle_policy=False,
                 endpoint_url=None,
                 region_name=None,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 aws_session_token=None,
                 profile_name=None):
        self.bucket_name = bucket_name
        self.chunk_split_size = chunk_size_mb if chunk_size_mb else self.DEFAULT_CHUNK_SIZE
        self.prefix = s3_prefix
        self.src_dir = src_dir
        self.src_file = src_file
        self.apply_lifecycle_policy = apply_lifecycle_policy
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.profile_name = profile_name

    @property
    def chunk_split_size(self):
        return self.__chunk_split_size

    @chunk_split_size.setter
    def chunk_split_size(self, chunk_size_mb):
        if chunk_size_mb < 5:
            raise ValueError("Error: chunk size must be at least 5 MB for S3 multipart uploads.")
        self.__chunk_split_size = chunk_size_mb * self.MB

    @property
    def bucket_name(self):
        return self.__bucket_name

    @bucket_name.setter
    def bucket_name(self, bucket_name):
        self.__bucket_name = bucket_name

    @property
    def prefix(self):
        return self.__prefix

    @prefix.setter
    def prefix(self, prefix):
        prefix = "" if not prefix else prefix
        if not re.match(self.S3_PREFIX_REGEX, prefix):
            logging.error("Wrong value for parameter prefix -> %s", prefix)
            raise Exception(self.S3_PREFIX_REGEX_ERR_MSG)
        if prefix and not prefix.endswith("/"):
            prefix = f"{prefix}/"
        self.__prefix = prefix

    @property
    def src_dir(self):
        return self.__src_dir

    @src_dir.setter
    def src_dir(self, src_dir):
        self.__src_dir = src_dir

    @property
    def src_file(self):
        return self.__src_file

    @src_file.setter
    def src_file(self, src_file):
        self.__src_file = src_file

    def input_validation(self):
        if not self.src_dir and not self.src_file:
            raise Exception(self.INPUT_DATA_ERR_MSG)


class S3Uploader:

    def __init__(self, settings):
        self.conf = settings
        self.session = self._build_session()
        self.AWS_DEFAULT_REGION = self.conf.region_name or self.session.region_name or 'us-east-1'
        self.s3_cli = self._build_s3_client()
        if self.conf.bucket_name:
            logging.info("The following bucket will be used: %s", self.conf.bucket_name)
            self.ensure_bucket_exists(self.conf.bucket_name)
            if self.conf.apply_lifecycle_policy:
                self.apply_lifecycle_policy(self.conf.bucket_name, self.conf.prefix)

    def _build_session(self):
        session_kwargs = {}
        if self.conf.profile_name:
            session_kwargs['profile_name'] = self.conf.profile_name
        return boto3.session.Session(**session_kwargs)

    def _build_s3_client(self):
        client_kwargs = {
            'endpoint_url': self.conf.endpoint_url,
            'region_name': self.AWS_DEFAULT_REGION,
            'config': Config(s3={'addressing_style': 'path'})
        }
        if self.conf.aws_access_key_id:
            client_kwargs['aws_access_key_id'] = self.conf.aws_access_key_id
        if self.conf.aws_secret_access_key:
            client_kwargs['aws_secret_access_key'] = self.conf.aws_secret_access_key
        if self.conf.aws_session_token:
            client_kwargs['aws_session_token'] = self.conf.aws_session_token
        return self.session.client('s3', **client_kwargs)

    @staticmethod
    def _format_size(size_bytes):
        if size_bytes < S3UploaderSettings.MB:
            return f"{size_bytes} B"
        if size_bytes < 1024 * S3UploaderSettings.MB:
            return f"{size_bytes / S3UploaderSettings.MB:.2f} MB"
        return f"{size_bytes / (1024 * S3UploaderSettings.MB):.2f} GB"

    @staticmethod
    def _build_file_descriptor(filepath, chunk_split_size, display_name=None):
        tot_bytes = Path(filepath).stat().st_size
        chunks_number = max(1, math.ceil(tot_bytes / chunk_split_size))
        return {
            "filepath": filepath,
            "filename": display_name if display_name else ntpath.basename(filepath),
            "size": tot_bytes,
            "chunks_number": chunks_number
        }

    @staticmethod
    def get_files_from_index_file(index_file_path, chunk_split_size):
        files = []
        with open(index_file_path, encoding='utf-8') as fp:
            for raw_line in fp:
                line = raw_line.strip()
                if not line:
                    continue
                if os.path.isfile(line):
                    files.append(S3Uploader._build_file_descriptor(line, chunk_split_size))
                else:
                    logging.error("%s not a valid file. Skipping.", line)
        return files

    @staticmethod
    def get_files_from_directory(src_dir, chunk_split_size, remove_zero_bytes_files=True):
        logging.info("Indexing files located at: %s", src_dir)
        files = []
        if os.path.isfile(src_dir):
            files.append(S3Uploader._build_file_descriptor(src_dir, chunk_split_size))
            return files

        src_root = Path(src_dir).resolve()
        for root, _, filenames in os.walk(src_root):
            for filename in sorted(filenames):
                file_path = Path(root) / filename
                file_size = file_path.stat().st_size
                if file_size == 0 and remove_zero_bytes_files:
                    logging.info("Skipping empty file %s", file_path)
                    continue
                relative_path = file_path.relative_to(src_root).as_posix()
                files.append(
                    S3Uploader._build_file_descriptor(
                        str(file_path),
                        chunk_split_size,
                        display_name=relative_path
                    )
                )
        logging.info("Finished indexing directory. Found %s files.", len(files))
        return files

    @staticmethod
    def get_files_from_paths(paths, chunk_split_size, remove_zero_bytes_files=True):
        files = []
        for raw_path in paths:
            path = Path(raw_path).resolve()
            if path.is_file():
                files.append(S3Uploader._build_file_descriptor(str(path), chunk_split_size))
                continue
            if not path.is_dir():
                logging.error("%s is not a valid path. Skipping.", path)
                continue
            parent = path.parent
            for file_path in sorted(path.rglob('*')):
                if not file_path.is_file():
                    continue
                file_size = file_path.stat().st_size
                if file_size == 0 and remove_zero_bytes_files:
                    logging.info("Skipping empty file %s", file_path)
                    continue
                relative_path = file_path.relative_to(parent).as_posix()
                files.append(
                    S3Uploader._build_file_descriptor(
                        str(file_path),
                        chunk_split_size,
                        display_name=relative_path
                    )
                )
        return files

    @staticmethod
    def _normalize_folder_prefix(prefix):
        prefix = prefix.strip().strip('/')
        return f"{prefix}/" if prefix else ""

    def _emit(self, callback, event_type, **payload):
        if callback:
            callback(event_type, payload)

    def ensure_bucket_exists(self, bucket_name):
        try:
            self.s3_cli.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as err:
            status = err.response["ResponseMetadata"]["HTTPStatusCode"]
            errcode = err.response["Error"]["Code"]
            if status == 404:
                logging.info("Creating a new bucket.")
                create_bucket_params = {'Bucket': bucket_name}
                if self.AWS_DEFAULT_REGION and self.AWS_DEFAULT_REGION != 'us-east-1':
                    create_bucket_params['CreateBucketConfiguration'] = {
                        'LocationConstraint': self.AWS_DEFAULT_REGION
                    }
                self.s3_cli.create_bucket(**create_bucket_params)
            elif status == 403:
                raise PermissionError(f"Bucket access denied {errcode}") from err
            else:
                raise
        except botocore.exceptions.ParamValidationError as err:
            raise ValueError(str(err)) from err

    def create_bucket(self, bucket_name):
        self.ensure_bucket_exists(bucket_name)
        return bucket_name

    def apply_lifecycle_policy(self, bucket_name, prefix=""):
        lifecycle_conf = {'Rules': [
            {
                'ID': 'S3Uploader',
                'Filter': {
                    'Prefix': prefix
                },
                'Status': 'Enabled',
                'Transitions': [
                    {
                        'Days': 1,
                        'StorageClass': 'DEEP_ARCHIVE'
                    }
                ],
                'AbortIncompleteMultipartUpload': {
                    'DaysAfterInitiation': 1
                }
            }
        ]}
        self.s3_cli.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_conf
        )

    def get_lifecycle_policy(self, bucket_name):
        try:
            return self.s3_cli.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        except botocore.exceptions.ClientError as err:
            error_code = err.response.get("Error", {}).get("Code")
            if error_code in {"NoSuchLifecycleConfiguration", "NoSuchBucket"}:
                return None
            raise

    def describe_lifecycle_policy(self, bucket_name):
        config = self.get_lifecycle_policy(bucket_name)
        if not config or not config.get('Rules'):
            return "No lifecycle policy configured for this bucket."

        lines = []
        for index, rule in enumerate(config['Rules'], start=1):
            status = rule.get('Status', 'Unknown')
            filter_conf = rule.get('Filter', {})
            prefix = filter_conf.get('Prefix', '/') or '/'
            lines.append(f"Rule {index}: {rule.get('ID', 'Unnamed rule')}")
            lines.append(f"  Status: {status}")
            lines.append(f"  Prefix: {prefix}")

            transitions = rule.get('Transitions', [])
            if transitions:
                transition_parts = []
                for transition in transitions:
                    days = transition.get('Days')
                    storage_class = transition.get('StorageClass', 'unknown')
                    if days is not None:
                        transition_parts.append(f"{storage_class} after {days} day(s)")
                    else:
                        transition_parts.append(storage_class)
                lines.append(f"  Transitions: {', '.join(transition_parts)}")

            expiration = rule.get('Expiration', {})
            expiration_days = expiration.get('Days')
            if expiration_days is not None:
                lines.append(f"  Expiration: after {expiration_days} day(s)")

            abort_conf = rule.get('AbortIncompleteMultipartUpload', {})
            abort_days = abort_conf.get('DaysAfterInitiation')
            if abort_days is not None:
                lines.append(
                    f"  Abort incomplete multipart uploads after {abort_days} day(s)"
                )
        return "\n\n".join(lines)

    def list_buckets(self):
        response = self.s3_cli.list_buckets()
        return sorted(bucket['Name'] for bucket in response.get('Buckets', []))

    def list_prefix(self, bucket_name, prefix=""):
        prefix = self._normalize_folder_prefix(prefix)
        paginator = self.s3_cli.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
        folders = []
        files = []
        for page in pages:
            for folder in page.get('CommonPrefixes', []):
                full_prefix = folder['Prefix']
                name = full_prefix[len(prefix):].rstrip('/')
                folders.append({'name': name, 'prefix': full_prefix})
            for item in page.get('Contents', []):
                key = item['Key']
                if key == prefix:
                    continue
                name = key[len(prefix):]
                if '/' in name:
                    continue
                files.append({'name': name, 'key': key, 'size': item['Size']})
        return {
            'folders': sorted(folders, key=lambda item: item['name'].lower()),
            'files': sorted(files, key=lambda item: item['name'].lower())
        }

    def create_folder(self, bucket_name, prefix):
        folder_prefix = self._normalize_folder_prefix(prefix)
        self.s3_cli.put_object(Bucket=bucket_name, Key=folder_prefix, Body=b'')
        return folder_prefix

    def delete_object(self, bucket_name, key):
        self.s3_cli.delete_object(Bucket=bucket_name, Key=key)
        return key

    def download_object(self, bucket_name, key, destination_path):
        destination = Path(destination_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.s3_cli.download_file(bucket_name, key, str(destination))
        return str(destination)

    def delete_prefix(self, bucket_name, prefix):
        prefix = self._normalize_folder_prefix(prefix)
        paginator = self.s3_cli.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        objects = []
        for page in pages:
            for item in page.get('Contents', []):
                objects.append({'Key': item['Key']})
        if objects:
            for start in range(0, len(objects), 1000):
                self.s3_cli.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': objects[start:start + 1000]}
                )
        return prefix

    def delete_bucket(self, bucket_name):
        listing = self.list_prefix(bucket_name, "")
        if listing['folders'] or listing['files']:
            raise ValueError("Bucket is not empty.")
        self.s3_cli.delete_bucket(Bucket=bucket_name)
        return bucket_name

    def get_existing_multiparts(self, bucket_name=None, prefix=None):
        bucket_name = bucket_name or self.conf.bucket_name
        prefix = self.conf.prefix if prefix is None else prefix
        multiparts = {}
        paginator = self.s3_cli.get_paginator('list_multipart_uploads')
        pages = paginator.paginate(Bucket=bucket_name)
        for page in pages:
            for item in page.get('Uploads', []):
                if prefix and not item['Key'].startswith(prefix):
                    continue
                multiparts[item['Key']] = item['UploadId']
        return multiparts

    def object_exists(self, key, bucket_name=None):
        bucket_name = bucket_name or self.conf.bucket_name
        try:
            self.s3_cli.head_object(Bucket=bucket_name, Key=key)
        except botocore.exceptions.ClientError as err:
            status = err.response["ResponseMetadata"]["HTTPStatusCode"]
            errcode = err.response["Error"]["Code"]
            if status == 404:
                logging.info("Missing object %s", errcode)
                return {'message': 'Object does not exists in the bucket', 'code': 1}
            if status == 403:
                logging.error("Access denied %s", errcode)
                return {'message': 'Access denied', 'code': 2}
            raise
        except botocore.exceptions.ParamValidationError as err:
            logging.error(err)
            return {'message': str(err), 'code': 3}
        return {'message': f'File {key} already exists', 'code': 0}

    def collect_archives(self, src_dir=None, src_file=None, paths=None):
        archives = []
        if paths:
            archives.extend(self.get_files_from_paths(paths, self.conf.chunk_split_size))
        if src_file:
            archives.extend(self.get_files_from_index_file(src_file, self.conf.chunk_split_size))
        if src_dir:
            archives.extend(self.get_files_from_directory(src_dir, self.conf.chunk_split_size))
        return archives

    def upload_files_to_s3(self, src_dir=None, s3_prefix=None, src_file=None, progress_callback=None):
        if s3_prefix:
            self.conf.prefix = s3_prefix
        if src_dir:
            self.conf.src_dir = src_dir
        if src_file:
            self.conf.src_file = src_file
        self.conf.input_validation()
        archives = self.collect_archives(src_dir=self.conf.src_dir, src_file=self.conf.src_file)
        return self._upload_archives(archives, self.conf.bucket_name, self.conf.prefix, progress_callback)

    def upload_paths_to_s3(self, paths, bucket_name, s3_prefix="", progress_callback=None):
        if not paths:
            return {'files': 0, 'bytes': 0}
        archives = self.collect_archives(paths=paths)
        return self._upload_archives(archives, bucket_name, self._normalize_folder_prefix(s3_prefix), progress_callback)

    def _upload_archives(self, archives, bucket_name, prefix, progress_callback=None):
        if not archives:
            logging.warning("Nothing to do.")
            self._emit(progress_callback, 'summary', files=0, bytes=0, bucket=bucket_name, prefix=prefix)
            return {'files': 0, 'bytes': 0}

        total_size = sum(item['size'] for item in archives)
        logging.info(
            "Preparing to upload %s files totalling %s to s3://%s/%s",
            len(archives),
            self._format_size(total_size),
            bucket_name,
            prefix
        )
        self.ensure_bucket_exists(bucket_name)
        multiparts = self.get_existing_multiparts(bucket_name=bucket_name, prefix=prefix)
        self._emit(
            progress_callback,
            'summary',
            files=len(archives),
            bytes=total_size,
            bucket=bucket_name,
            prefix=prefix
        )

        for index, item in enumerate(archives, start=1):
            key = prefix + item['filename']
            logging.info(
                "[%s/%s] Processing %s (%s) -> s3://%s/%s",
                index,
                len(archives),
                item['filename'],
                self._format_size(item['size']),
                bucket_name,
                key
            )
            self._emit(
                progress_callback,
                'file_started',
                index=index,
                total=len(archives),
                filename=item['filename'],
                key=key,
                size=item['size']
            )
            existing_object = self.object_exists(key=key, bucket_name=bucket_name)
            if existing_object['code'] == 0:
                logging.info(existing_object['message'])
                self._emit(
                    progress_callback,
                    'file_skipped',
                    index=index,
                    total=len(archives),
                    filename=item['filename'],
                    key=key,
                    message=existing_object['message']
                )
                continue
            if item['chunks_number'] == 1:
                self.s3_cli.upload_file(Bucket=bucket_name, Key=key, Filename=item['filepath'])
                self._emit(
                    progress_callback,
                    'file_completed',
                    index=index,
                    total=len(archives),
                    filename=item['filename'],
                    key=key
                )
                continue

            all_parts = []
            next_part_number = 1
            if key in multiparts:
                upload_id = multiparts[key]
                paginator = self.s3_cli.get_paginator('list_parts')
                pages = paginator.paginate(Bucket=bucket_name, Key=key, UploadId=upload_id)
                for page in pages:
                    for uploaded_part in page.get('Parts', []):
                        all_parts.append({
                            'PartNumber': uploaded_part['PartNumber'],
                            'ETag': uploaded_part['ETag']
                        })
                if all_parts:
                    next_part_number = max(part['PartNumber'] for part in all_parts) + 1
                    self._emit(
                        progress_callback,
                        'multipart_resumed',
                        index=index,
                        total=len(archives),
                        filename=item['filename'],
                        next_part_number=next_part_number,
                        part_total=item['chunks_number']
                    )
            else:
                mpu = self.s3_cli.create_multipart_upload(Bucket=bucket_name, Key=key)
                upload_id = mpu['UploadId']

            part_size_bytes = self.conf.chunk_split_size
            with open(item['filepath'], 'rb') as upload:
                start_offset = (next_part_number - 1) * part_size_bytes
                if start_offset:
                    upload.seek(start_offset)
                for p in range(next_part_number - 1, item['chunks_number']):
                    lower = p * part_size_bytes
                    upper = (((p + 1) * part_size_bytes) - 1) if (p + 1 < item['chunks_number']) else (
                        item['size'] - 1
                    )
                    read_size = upper - lower + 1
                    file_part = upload.read(read_size)
                    my_hash = hashlib.md5(file_part).hexdigest()
                    part = self.s3_cli.upload_part(
                        Bucket=bucket_name,
                        UploadId=upload_id,
                        PartNumber=p + 1,
                        ContentLength=read_size,
                        Body=file_part,
                        Key=key
                    )
                    all_parts.append({'PartNumber': p + 1, 'ETag': part['ETag']})
                    if my_hash != part['ETag'].strip('"'):
                        raise Exception("Uploaded file is corrupted. Aborting.")
                    completed_pct = ((upper + 1) * 100) / item['size']
                    self._emit(
                        progress_callback,
                        'part_progress',
                        index=index,
                        total=len(archives),
                        filename=item['filename'],
                        part_number=p + 1,
                        part_total=item['chunks_number'],
                        bytes_uploaded=upper + 1,
                        size=item['size'],
                        completed_pct=completed_pct
                    )
                self.s3_cli.complete_multipart_upload(
                    UploadId=upload_id,
                    Bucket=bucket_name,
                    Key=key,
                    MultipartUpload={'Parts': sorted(all_parts, key=lambda part: part['PartNumber'])}
                )
            self._emit(
                progress_callback,
                'file_completed',
                index=index,
                total=len(archives),
                filename=item['filename'],
                key=key
            )
        return {'files': len(archives), 'bytes': total_size}
