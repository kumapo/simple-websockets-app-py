import json
import os
import sys
import logging

sys.path.insert(0, F"{os.environ['LAMBDA_TASK_ROOT']}/{os.environ['DIR_NAME']}")
import boto3
import botocore

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

dynamodb = boto3.resource('dynamodb')
connections = dynamodb.Table(os.environ['TABLE_NAME'])

def lambda_handler(event, context):
    logger.debug("sendmessage: %s" % event)

    post_data = json.loads(event.get('body', '{}')).get('data')
    domain_name = event.get('requestContext',{}).get('domainName')
    stage       = event.get('requestContext',{}).get('stage')
    if (post_data and domain_name and stage) is None:
        return { 'statusCode': 400, 
                 'body': 'bad request' }

    items = connections.scan(ProjectionExpression='id').get('Items')
    if items is None:
        return { 'statusCode': 500,
                 'body': 'something went wrong' }

    apigw_management = boto3.client('apigatewaymanagementapi',
                                    endpoint_url=F"https://{domain_name}/{stage}")
    for item in items:
        try:
            _ = apigw_management.post_to_connection(ConnectionId=item['id'],
                                                         Data=post_data)
        except botocore.exceptions.ClientError as e:
            if e.response.get('ResponseMetadata',{}).get('HTTPStatusCode') == 410:
                connections.delete_item(Key={'id': item['id']})
                logger.debug('post_to_connection skipped: %s removed from connections' % item['id'])
            else:
                logger.debug('post_to_connection failed: %s' % e)
                return { 'statusCode': 500,
                         'body': 'something went wrong' }
    return { 'statusCode': 200,
             'body': 'ok' }