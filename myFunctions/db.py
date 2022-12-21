import boto3
import requests
import json
from s3 import S3_Bucket
from requests.auth import HTTPBasicAuth
from boto3.dynamodb.conditions import Key


class dynamoDB:
    def __init__(self):
        self.client = boto3.client('dynamodb')
        self.endpointOS = 'https://search-customers-goqen7vb6qjj7oe445jvw2tlwe.ap-southeast-1.es.amazonaws.com/'
        self.username = 'admin'
        self.password = 'Caolenguyencln@1'
        self.s3 = S3_Bucket()

    def putAllItems(self, items, nameTable, nameIndex, columns):
        # create index opensearch
        response = requests.put(
            self.endpointOS + nameIndex,
            auth=HTTPBasicAuth(self.username, self.password)
        )
        if (response):
            # insert dynamo
            data = ''
            for i in range(len(items)):
                itemDict = {}

                for j in range(len(items[i])):
                    itemDict[columns[j]] = items[i][j]

                index = "'_index': '{}', '_id': '{}'".format(
                    nameIndex, i).replace("\'", "\"")
                index = "{ \"index\": { " + index + " } }"
                data = data + index + '\n'
                data = data + json.dumps(itemDict) + '\n'

            response = requests.post(
                self.endpointOS + nameIndex + '/_bulk',
                auth=HTTPBasicAuth(self.username, self.password),
                headers={'Content-Type': 'application/json'},
                data=data
            )

        for item in items:
            urlImage = self.s3.putItemImage(
                item=item[-1], key=item[0], nameBucket='customerss')
            try:
                res = self.client.put_item(
                    TableName=nameTable,
                    Item={
                        'id': {
                            'S': item[0]
                        },
                        'username': {
                            'S': item[1]
                        },
                        'email': {
                            'S': item[2]
                        },
                        'gender': {
                            'S': item[3]
                        },
                        'country': {
                            'S': item[4]
                        },
                        'password': {
                            'S': item[5]
                        },
                        'phone': {
                            'S': item[6]
                        },
                        'urlImage': {
                            'S': urlImage
                        },
                        'imagebase64': {
                            'S': item[7]
                        },
                    }
                )

            except Exception as e:
                return {
                    'statusCode': 500,
                    'body': 'Something wrong from server!!!'
                }

        return {
            'statuscode': 200,
            'message': 'Post all items success.'
        }

    def getAllItems(self, nameTable):
        res = self.client.scan(
            TableName=nameTable,
            AttributesToGet=[
                'id', 'country', 'email', 'gender', 'imagebase64', 'phone', 'urlImage', 'username'
            ],
        )
        return {
            'statusCode': 200,
            'message': 'Get all items success.',
            'data': res.get("Items", "Not exists items in table!!!")
        }

    def getItem(self, nameTable, itemId):
        res = self.client.query(
            TableName=nameTable,
            ExpressionAttributeNames={
                '#id': 'id'
            },
            ExpressionAttributeValues={
                ':id': {
                    'S': str(itemId)
                }
            },
            KeyConditionExpression='#id = :id'
        )

        return {
            'statusCode': 200,
            'message': 'Get item success.',
            'data': res.get("Items", "Not exists items in table!!!")
        }

    def updateItem(self, nameTable, item):
        checkExitItem = self.client.query(
            TableName=nameTable,
            ExpressionAttributeNames={
                '#id': 'id'
            },
            ExpressionAttributeValues={
                ':id': item.get('id')
            },
            KeyConditionExpression='#id = :id'
        )

        if (checkExitItem.get("Count") != 0):
            res = self.client.update_item(
                TableName=nameTable,
                Key={
                    'id': item.get('id')
                },
                ExpressionAttributeNames={
                    '#username': 'username',
                    '#email': 'email',
                    '#address': 'address',
                    '#phone': 'phone',
                    '#urlImage': 'urlImage',
                    '#imagebase64': 'imagebase64',
                    '#country': 'country'
                },
                ExpressionAttributeValues={
                    ':username': item.get('username'),
                    ':email': item.get('email'),
                    ':address': item.get('address'),
                    ':phone': item.get('phone'),
                    ':urlImage': item.get('urlImage'),
                    ':imagebase64': item.get('imagebase64'),
                    ':country': item.get('country')
                },
                UpdateExpression='SET #username = :username, #email = :email,' +
                '#password = :password, #address = :address,' +
                '#phone = :phone, #urlImage = :urlImage, #imagebase64 = :imagebase64, #country = :country',
                ReturnValues='ALL_NEW',
            )
        else:
            return {
                'statusCode': 400,
                'message': 'Item is not exists.'
            }
