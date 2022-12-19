import json
import boto3
from io import BytesIO
# from wkhtmltopdf import WKHtmlToPdf


def index(event, context):
    client = boto3.client('dynamodb')
    res = client.put_item(
        TableName='my-table',
        Item={
            'id': {
                'S': '1'
            },
            'username': {
                'S': 'Nguyen Van A'
            },
            'email': {
                'S': 'vana@gmail.com'
            }
        }
    )
    print(res)
    return {
        'body': json.dumps(event)
    }
