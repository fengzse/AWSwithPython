import os
import boto3
from botocore.exceptions import ClientError

bucket_name = 'mydrivestest'
access_key = 'add your IAM access key here'
secrets_key = 'add your IAM secret key here'

'''
Connect to S3 service
'''
client_s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secrets_key
)

'''
Upload files to S3 Bucket
'''
def upload_file(*filenames):
    data_files_upload_folder = os.path.join(os.getcwd(), 'static/upload')
    if filenames is None:
        for file in os.listdir(data_files_upload_folder):
            try:
                print('Uploading file {0}'.format(file))
                client_s3.upload_file(
                    os.path.join(data_files_upload_folder, file),
                    bucket_name,
                    file
                )
            except ClientError as e:
                print('Credential is incorrect')
                print(e)
            except Exception as ee:
                print(ee)
                raise ee
    else:
        for filename in filenames:
            try:
                print('Uploading file {0}'.format(filename))
                client_s3.upload_file(
                    os.path.join(data_files_upload_folder, filename),
                    bucket_name,
                    filename
                )
            except ClientError as e:
                print('Credential is incorrect')
                print(e)
            except Exception as ee:
                print(ee)
                raise ee


'''
Downloading files from S3 Bucket
'''
def download_file(*filenames):
    data_files_download_folder = os.path.join(os.getcwd(), 'static/download')
    if filenames is not None:
        for filename in filenames:
            try:
                print('Downloading file {0}'.format(filename))
                client_s3.download_file(bucket_name, filename, os.path.join(data_files_download_folder, filename))
            except Exception as e:
                print(e)
                raise e


# upload_file('shuttle.mp4')
download_file('shuttle.mp4')
