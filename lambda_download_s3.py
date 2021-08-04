import base64
import boto3

'''
This function is deployed on AWS Lambda
It handles an RESTful API which provides a filename and download the corresponding file from S3
'''
def lambda_handler(event, context):
    print(event)
    s3 = boto3.client('s3')
    bucket_name = "mydrivestest"
    file_name = event["pathParameters"]["filename"]

    fileObj = s3.get_object(Bucket=bucket_name, Key=file_name)
    file_content = fileObj["Body"].read()
    # print
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/jpg",
            "Content-Disposition": "attachment; filename={}".format(file_name)
        },
        "body": base64.b64encode(file_content),  # 文件必须先读取再转码然后响应
        "isBase64Encoded": True
    }


