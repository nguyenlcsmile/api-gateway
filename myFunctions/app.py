import json
import boto3
from io import BytesIO
# from wkhtmltopdf import WKHtmlToPdf


def index(event, context):
    client = boto3.client('dynamodb')
    res = client.put_item(
        TableName='my-table',
        Item={
            'id': '1',
            'username': 'Nguyen Van A',
            'email': 'vana@gmail.com'
        }
    )
    print(res)
    return {
        'body': json.dumps(event)
    }
