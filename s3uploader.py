import boto3
import math
import os
from pathlib import Path
import hashlib
import botocore
import logging
import ntpath
import re
import argparse


class S3UploaderSettings:
    MB = 1048576  # 1MB in bytes
    DEFAULT_CHUNK_SIZE = 64
    S3_PREFIX_REGEX = "^(([a-zA-Z0-9]+[/])+)?$"
    S3_PREFIX_REGEX_ERR_MSG = "Invalid prefix. Valid values are folder/ or folder1/folder2/... "
    INPUT_DATA_ERR_MSG = "Provide at least either one or both of the input parameters: src_file, src_dir"

    def __init__(self,
                 s3_prefix=None,
                 chunk_size_mb=None,
                 src_dir=None,
                 bucket_name=None,
                 src_file=None):
        self.bucket_name = bucket_name
        self.chunk_split_size = chunk_size_mb if chunk_size_mb else self.DEFAULT_CHUNK_SIZE
        self.prefix = s3_prefix
        self.src_dir = src_dir
        self.src_file = src_file

    @property
    def chunk_split_size(self):
        return self.__chunk_split_size

    @chunk_split_size.setter
    def chunk_split_size(self, chunk_size_mb):
        if chunk_size_mb not in [2, 4, 8, 16, 32, 64, 128, 256]:
            raise ValueError("Error: possible values are 2, 4, 8, 16, 32, 64, 128, 256 MB.")
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
            logging.error(f"Wrong value for parameter prefix -> {prefix}")
            raise Exception(self.S3_PREFIX_REGEX_ERR_MSG)
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
            print("both none")
            raise Exception(self.INPUT_DATA_ERR_MSG)


class S3Uploader:

    def __init__(self, settings):
        self.conf = settings
        logging.info(f'The following bucket will be used: {self.conf.bucket_name}')

        self.s3_cli = boto3.client('s3')
        self.sns_cli = boto3.client('sns')
        my_session = boto3.session.Session()
        self.AWS_DEFAULT_REGION = my_session.region_name

        # Determine if bucket exists and you have permission to access it
        try:
            self.s3_cli.head_bucket(Bucket=self.conf.bucket_name)
        except botocore.exceptions.ClientError as err:
            status = err.response["ResponseMetadata"]["HTTPStatusCode"]
            errcode = err.response["Error"]["Code"]
            if status == 404:
                logging.info(f"Creating a new bucket.")
                self.s3_cli.create_bucket(Bucket=self.conf.bucket_name,
                                          CreateBucketConfiguration={'LocationConstraint': self.AWS_DEFAULT_REGION})
            elif status == 403:
                logging.error(f"Bucket access denied {errcode}")
                exit()
        except botocore.exceptions.ParamValidationError as err:
            logging.error(err)
            exit()
        lifecycle_conf = {'Rules': [
            {
                'ID': 'S3Uploader',
                'Filter': {
                    'Prefix': ''
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
        response = self.s3_cli.put_bucket_lifecycle_configuration(Bucket=self.conf.bucket_name,
                                                                  LifecycleConfiguration=lifecycle_conf)

    @staticmethod
    def get_files_from_index_file(index_file_path, chunk_split_size):
        files = []
        with open(index_file_path) as fp:
            while True:
                line = fp.readline()
                if os.path.isfile(line):
                    tot_bytes = Path(line).stat().st_size
                    chunks_number = math.ceil(tot_bytes / chunk_split_size)
                    files.append({
                        "filepath": line,
                        "filename": ntpath.basename(line),
                        "size": tot_bytes,
                        "chunks_number": chunks_number
                    })
                else:
                    logging.error(f"{line} not a valid file. Skipping.")
                if not line:
                    break
        return files

    @staticmethod
    def get_files_from_directory(src_dir, chunk_split_size, remove_zero_bytes_files=True):
        """Return information about files in the given directory (path, file name, hash, size and number of chuncks)."""
        logging.info(f"Indexing files located at: {src_dir}")
        files = []
        # If the the data_path is a single file
        if os.path.isfile(src_dir):
            tot_bytes = Path(src_dir).stat().st_size
            chunks_number = math.ceil(tot_bytes / chunk_split_size)
            files.append({
                "filepath": src_dir,
                "filename": ntpath.basename(src_dir),
                "size": tot_bytes,
                "chunks_number": chunks_number
            })
            return files
        # If the the data_path is a directory
        files_list = [f for f in os.scandir(src_dir) if f.is_file()]
        for f in files_list:
            tot_bytes = f.stat().st_size
            if tot_bytes == 0 and remove_zero_bytes_files:
                continue
            chunks_number = math.ceil(tot_bytes / chunk_split_size)
            files.append({
                "filepath": f.path,
                "filename": f.name,
                "size": tot_bytes,
                "chunks_number": chunks_number
            })
        logging.info("Finished indexing directory.")
        return files

    def get_existing_multiparts(self, prefix=None):
        """Retrieve all the existing multipart upload from a given bucket and prefix."""
        if prefix:
            self.conf.prefix = prefix

        multiparts = {}
        paginator = self.s3_cli.get_paginator('list_multipart_uploads')
        pages = paginator.paginate(Bucket=self.conf.bucket_name, Prefix=self.conf.prefix)
        for page in pages:
            if 'Uploads' in page.keys():
                for item in page['Uploads']:
                    multiparts[item['Key']] = item['UploadId']
        return multiparts

    def object_exists(self, key):
        """Determine if object exists and you have permission to access it"""
        try:
            response = self.s3_cli.head_object(Bucket=self.conf.bucket_name, Key=key)
            # TODO check etag for md5 comparison
        except botocore.exceptions.ClientError as err:
            status = err.response["ResponseMetadata"]["HTTPStatusCode"]
            errcode = err.response["Error"]["Code"]
            if status == 404:
                logging.info(f"Missing object {errcode}")
                return {'message': 'Object does not exists in the bucket', 'code': 1}
            elif status == 403:
                logging.error(f"Access denied {errcode}")
                return {'message': 'Access denied', 'code': 2}
        except botocore.exceptions.ParamValidationError as err:
            logging.error(err)
            return {'message': f'{err}', 'code': 3}
        return {'message': f'File {key} already exists', 'code': 0}

    def upload_files_to_s3(self, src_dir=None, s3_prefix=None, src_file=None):
        if s3_prefix:
            self.conf.prefix = s3_prefix
        if src_dir:
            self.conf.src_dir = src_dir
        if src_file:
            self.conf.src_file = src_file
        # Provide either a source directory or a source file
        self.conf.input_validation()

        archives = []
        if self.conf.src_file:
            try:
                archives.extend(S3Uploader.get_files_from_index_file(self.conf.src_file, self.conf.chunk_split_size))
            except FileNotFoundError:
                logging.error("Invalid source file.")
        if self.conf.src_dir:
            try:
                archives.extend(
                    S3Uploader.get_files_from_directory(src_dir=self.conf.src_dir,
                                                        chunk_split_size=self.conf.chunk_split_size))
            except FileNotFoundError:
                logging.error("Invalid working directory.")

        if not archives:
            logging.warning("Nothing to do.")

        multiparts = self.get_existing_multiparts(prefix=self.conf.prefix)
        for item in archives:
            key = self.conf.prefix + item['filename']
            if self.object_exists(key=key)['code'] == 0:
                print(self.object_exists(key=key)['message'])
                continue
            # if item doesn't require multipart upload it directly
            if item['chunks_number'] == 1:
                print(f"Uploading file {item['filename']}. Wait...")
                self.s3_cli.upload_file(Bucket=self.conf.bucket_name, Key=key, Filename=item['filepath'])
                print(f"Done")
            elif item['chunks_number'] > 1:
                print(f"Uploading file {item['filename']}. {item['chunks_number']} chunks.")
                all_parts = []
                p_max = 1
                if key in multiparts.keys():
                    upload_id = multiparts[key]
                    # Retrieve list of uploaded parts
                    paginator = self.s3_cli.get_paginator('list_parts')
                    pages = paginator.paginate(Bucket=self.conf.bucket_name, Key=key, UploadId=upload_id)
                    for page in pages:
                        if 'Parts' in page.keys():
                            for uploaded_part in page['Parts']:
                                all_parts.append({'PartNumber': uploaded_part['PartNumber'],
                                                  'ETag': uploaded_part['ETag']})
                                p_max += 1
                else:
                    mpu = self.s3_cli.create_multipart_upload(Bucket=self.conf.bucket_name, Key=key)
                    upload_id = mpu['UploadId']

                part_size_bytes = self.conf.chunk_split_size
                with open(item['filepath'], 'rb') as upload:
                    if p_max > 0:
                        for j in all_parts:
                            print(f"Part {j['PartNumber']} already uploaded.")
                    for p in range(p_max - 1, item['chunks_number']):
                        # Calculate lower and upper bounds for the byte ranges. The last range
                        # is now smaller than the ones that come before.
                        lower = p * part_size_bytes
                        upper = (((p + 1) * part_size_bytes) - 1) if (p + 1 < item['chunks_number']) else (
                                item['size'] - 1)
                        read_size = upper - lower + 1
                        file_part = upload.read(read_size)
                        my_hash = (hashlib.md5(file_part)).hexdigest()
                        print(f"Uploading {p + 1} /{item['chunks_number']} chunk...")
                        part = self.s3_cli.upload_part(Bucket=self.conf.bucket_name,
                                                       UploadId=upload_id,
                                                       PartNumber=p + 1,
                                                       ContentLength=read_size,
                                                       Body=file_part,
                                                       Key=key)
                        all_parts.append({'PartNumber': p + 1, 'ETag': part['ETag']})
                        if my_hash != part['ETag'].strip('"'):
                            raise Exception("Uploaded file is corrupted. Aborting.")
                        print(f"Done -  ({(upper * 100) / item['size']:.2f}%)")
                    self.s3_cli.complete_multipart_upload(UploadId=upload_id,
                                                          Bucket=self.conf.bucket_name,
                                                          Key=key,
                                                          MultipartUpload={'Parts': all_parts})


def main():
    def regex_s3_prefix(arg_value, pat=re.compile(S3UploaderSettings.S3_PREFIX_REGEX)):
        if not pat.match(arg_value):
            raise argparse.ArgumentTypeError(S3UploaderSettings.S3_PREFIX_REGEX_ERR_MSG)
        return arg_value

    my_parser = argparse.ArgumentParser(description='S3 uploader help')
    my_parser.add_argument('bucket_name',
                           metavar='--bucket-name',
                           type=str,
                           help='The S3 bucket name where data will be copied to')
    my_parser.add_argument('-p',
                           metavar='--s3-prefix',
                           type=regex_s3_prefix,
                           help='The prefix where the data will be stored in S3')
    my_parser.add_argument('-s',
                           metavar='--chunk-size',
                           type=int,
                           choices=[4, 8, 16, 32, 64, 128, 256],
                           help='The chunk size in MB. Default is 64MB. '
                                'Possible values are 4, 8, 16, 32, 64, 128, 256')
    group = my_parser.add_argument_group('Data to be copied. Provide at least one parameter')
    group.add_argument('-d',
                       metavar='--src-dir',
                       type=str,
                       help='The directory to be copied to S3')
    group.add_argument('-f',
                       metavar='--src-file',
                       type=str,
                       help='The file containing a list of files to be copied to S3')

    args = my_parser.parse_args()

    if not (args.d or args.f):
        my_parser.error(S3UploaderSettings.INPUT_DATA_ERR_MSG)

    settings = S3UploaderSettings(src_dir=args.d,
                                  chunk_size_mb=args.s,
                                  bucket_name=args.bucket_name,
                                  src_file=args.f,
                                  s3_prefix=args.p)

    s3_uploader = S3Uploader(settings)
    s3_uploader.upload_files_to_s3()


if __name__ == "__main__":
    main()
