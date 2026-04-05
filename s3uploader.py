import argparse
import logging
import re

from s3uploader_core import S3Uploader
from s3uploader_core import S3UploaderSettings


def build_arg_parser():
    def regex_s3_prefix(arg_value, pat=re.compile(S3UploaderSettings.S3_PREFIX_REGEX)):
        if not pat.match(arg_value):
            raise argparse.ArgumentTypeError(S3UploaderSettings.S3_PREFIX_REGEX_ERR_MSG)
        return arg_value

    parser = argparse.ArgumentParser(description='S3 uploader help')
    parser.add_argument('bucket_name',
                        metavar='--bucket-name',
                        type=str,
                        help='The S3 bucket name where data will be copied to')
    parser.add_argument('-p',
                        metavar='--s3-prefix',
                        type=regex_s3_prefix,
                        help='The prefix where the data will be stored in S3')
    parser.add_argument('-s',
                        metavar='--chunk-size',
                        type=int,
                        help='The chunk size in MB. Default is 64MB. Minimum value is 5 MB')
    group = parser.add_argument_group('Data to be copied. Provide at least one parameter')
    group.add_argument('-d',
                       metavar='--src-dir',
                       type=str,
                       help='The directory to be copied to S3')
    group.add_argument('-f',
                       metavar='--src-file',
                       type=str,
                       help='The file containing a list of files to be copied to S3')
    parser.add_argument('--apply-lifecycle-policy',
                        action='store_true',
                        help='Apply a lifecycle rule for the selected prefix and abort incomplete uploads after 1 day')
    parser.add_argument('--endpoint-url',
                        type=str,
                        help='Custom S3 endpoint URL, useful for local testing such as http://localhost:4566')
    parser.add_argument('--region',
                        type=str,
                        help='AWS region override. Defaults to the current AWS configuration or us-east-1')
    parser.add_argument('--profile',
                        type=str,
                        help='AWS profile name to use')
    parser.add_argument('--access-key-id',
                        type=str,
                        help='Explicit AWS access key ID')
    parser.add_argument('--secret-access-key',
                        type=str,
                        help='Explicit AWS secret access key')
    parser.add_argument('--session-token',
                        type=str,
                        help='Explicit AWS session token')
    return parser


def _cli_progress(event_type, payload):
    if event_type == 'summary':
        return
    if event_type == 'file_started':
        print(
            f"[{payload['index']}/{payload['total']}] Uploading {payload['filename']}"
        )
        return
    if event_type == 'file_skipped':
        print(f"[{payload['index']}/{payload['total']}] {payload['message']}")
        return
    if event_type == 'multipart_resumed':
        print(
            f"[{payload['index']}/{payload['total']}] Resuming from part "
            f"{payload['next_part_number']} of {payload['part_total']}"
        )
        return
    if event_type == 'part_progress':
        print(
            f"[{payload['index']}/{payload['total']}] Part {payload['part_number']}/"
            f"{payload['part_total']} uploaded ({payload['completed_pct']:.2f}%)"
        )
        return
    if event_type == 'file_completed':
        print(f"[{payload['index']}/{payload['total']}] Completed {payload['filename']}")


def main():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    parser = build_arg_parser()
    args = parser.parse_args()

    if not (args.d or args.f):
        parser.error(S3UploaderSettings.INPUT_DATA_ERR_MSG)

    settings = S3UploaderSettings(
        src_dir=args.d,
        chunk_size_mb=args.s,
        bucket_name=args.bucket_name,
        src_file=args.f,
        s3_prefix=args.p,
        apply_lifecycle_policy=args.apply_lifecycle_policy,
        endpoint_url=args.endpoint_url,
        region_name=args.region,
        aws_access_key_id=args.access_key_id,
        aws_secret_access_key=args.secret_access_key,
        aws_session_token=args.session_token,
        profile_name=args.profile
    )

    uploader = S3Uploader(settings)
    uploader.upload_files_to_s3(progress_callback=_cli_progress)


if __name__ == "__main__":
    main()
