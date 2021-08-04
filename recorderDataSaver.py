import json
import boto3
from boto3.dynamodb.conditions import Attr, Key

'''
This is function is to automatically save information of objects into DynamoDB
when they are uploaded to the S3-bucket
This function is deployed to the AWS Lambda
'''
def lambda_handler(event, context):
    print(event)
    s3Client = boto3.resource('s3')     # not used currently
    dynamo = boto3.resource('dynamodb').Table('recorderDataSave')

    '''
    try to get information from s3 which needs to be stored in the database
    '''
    bucket = event['Records'][0]['s3']['bucket']['name']    # not used currently
    object_key = event['Records'][0]['s3']['object']['key']

    # get userId which is the name of top folder in the bucket
    userId = object_key.split('/')[0]

    # get uploading time of object
    addObjectTime = event['Records'][0]['eventTime']

    # get eTag(all objects uploaded to the same folder share the same eTag)
    objects_tag = event['Records'][0]['s3']['object']['eTag']

    # get name of the uploaded file
    object_name = object_key.split('/')[-1]

    # get sequence number of uploaded file, which is unique
    object_sequence = event['Records'][0]['s3']['object']['sequencer']

    data_input = {
        'userId': userId,
        'recordTag': objects_tag,
        'Records': [
            {
                'recordTime': addObjectTime,
                'recordName': object_name,
                'recordSequence': object_sequence
            }
        ]
    }
    '''
    Store user and records into dynamodb
    '''
    # first, find the document, if found, update it
    searchResult = dynamo.scan(FilterExpression=Attr('userId').eq(userId))
    if searchResult['Items'] is not None and len(searchResult['Items']) == 1:
        records = searchResult['Items'][0]['Records']  # get a list of records
        for record in data_input['Records']:
            records.append(record)
        dynamo.update_item(
            Key={
                'userId': userId

            },
            UpdateExpression='SET Records = :val1',     # When updates, we must set UpdateExpression first
            ExpressionAttributeValues={                 # And then update by ExpressionAttributeValues
                ':val1': records                        # 'SET Records = :val1' is the point

            }
        )
        return {
            'statusCode': 200,
            'body': json.dumps('User\'s records has been updated!')
        }
    # if document is not found, create it
    else:
        dynamo.put_item(
            Item=data_input
        )
        return {
            'statusCode': 200,
            'body': json.dumps('User\'s records has been created!')
        }
