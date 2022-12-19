import json
import boto3
import pandas as pd
from io import BytesIO


def index(event, context):
    client = boto3.client('dynamodb')
    df = pd.read_csv('data/data.csv', encoding='windows-1252').astype(str)
    listUsers = df.values.tolist()
    for user in listUsers:
        res = client.put_item(
            TableName='my-table',
            Item={
                'id': {
                    'S': user[0]
                },
                'username': {
                    'S': user[1]
                },
                'email': {
                    'S': user[2]
                },
                'gender': {
                    'S': user[3]
                },
                'country': {
                    'S': user[4]
                },
                'phone': {
                    'S': user[5]
                }
            }
        )
    return {
        'body': json.dumps(event)
    }
