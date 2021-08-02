import json
import boto3
from boto3.dynamodb.conditions import Attr


def lambda_handler(event, context):
    connectionId = event['requestContext']['connectionId']
    body = json.loads(event['body'])
    receiverName = body['receiver']
    msg = body['command']
    dynamo = boto3.resource('dynamodb').Table('mydrivesConnection')

    searchResult = dynamo.scan(FilterExpression=Attr('User_Name').eq(receiverName))
    if searchResult['Items'] is not None and len(searchResult['Items']) == 1:
        receiverConnectionId = searchResult['Items'][0]['connectionId']
        senderResult = dynamo.scan(FilterExpression=Attr('connectionId').eq(connectionId))
        if senderResult['Items'] is not None and len(senderResult['Items']) == 1:
            senderName = senderResult['Items'][0]['User_Name']
            jsonData = {'commandFrom': senderName, 'commandItem': msg}
            sendCommandToRecorder(receiverConnectionId, jsonData)

            return {
                'statusCode': 200,
                'body': json.dumps('Command has been sent to recorder')
            }
        else:
            return {'statusCode': 200, 'body': 'Error Occurred!'}
    else:
        return {'statusCode': 200, 'body': 'Recorder Not Found!'}


def sendCommandToRecorder(receiverConnectionId, command):
    url = 'https://gy516xb0u1.execute-api.eu-north-1.amazonaws.com/dev'
    gatewayApi = boto3.client("apigatewaymanagementapi", endpoint_url=url)
    req = gatewayApi.post_to_connection(ConnectionId=receiverConnectionId, Data=json.dumps(command))
