import json
import boto3
import pandas as pd
from io import BytesIO


def index(event, context):
    clientdb = boto3.client('dynamodb')
    clients3 = boto3.client('s3')

    dataCsv = clients3.get_object(
        Bucket='customerss',
        Key='data.csv'
    )
    print(dataCsv.get('Body').read())
    # df = pd.read_csv('data/data.csv').astype(str)
    # listUsers = df.values.tolist()
    # for user in listUsers:
    #     res = clientdb.put_item(
    #         TableName='my-table',
    #         Item={
    #             'id': {
    #                 'S': user[0]
    #             },
    #             'username': {
    #                 'S': user[1]
    #             },
    #             'email': {
    #                 'S': user[2]
    #             },
    #             'gender': {
    #                 'S': user[3]
    #             },
    #             'country': {
    #                 'S': user[4]
    #             },
    #             'password': {
    #                 'S': user[5]
    #             },
    #             'phone': {
    #                 'S': user[6]
    #             }
    #         }
    #     )
    return {
        'body': json.dumps(event)
    }
