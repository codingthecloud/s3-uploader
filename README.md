# S3Uploader

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
python ./s3uploader.py -d /Users/Mark/photo-folder/
```

## Uploading files to AWS S3
S3Uploader was designed to upload large files into AWS S3.
The tool performs a multipart upload if the total size is greater than the chunk size.
If the upload is interrupted the tool automatically restores the upload from where it stopped.

S3Uploader can upload a list of files declared in a text file or upload the content of a folder.

## S3 lifecylcle configuration
A lifecycle configuration will be automatically applied to the bucket that move data to Deep Glacier after 1 day.

## Using module from cli

```
S3 uploader help

positional arguments:
  --bucket-name    The S3 bucket name where data will be copied to

optional arguments:
  -h, --help       show this help message and exit
  -p --s3-prefix   The prefix where the data will be stored in S3
  -s --chunk-size  The chunk size in MB. Default is 64MB. Possible values are
                   4, 8, 16, 32, 64, 128, 256

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
                                  s3_prefix="the_S3_prefix")
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
- "src_file" is the path to a text file containing a file path per line. 
- "s3_prefix" is the path on S3. Default path is the root folder of the S3 bucket.
