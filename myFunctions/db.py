import boto3
import requests
import json
from s3 import S3_Bucket
from requests.auth import HTTPBasicAuth
from boto3.dynamodb.conditions import Key


class dynamoDB:
    def __init__(self):
        self.client = boto3.client('dynamodb')
        self.endpointOS = 'https://search-domain-x2xckinl4dpgecalezurjk6jbi.ap-southeast-1.es.amazonaws.com/'
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

    def filterItem(self, nameIndex, item):
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"match_phrase": {"email": str(item.get('email'))}}
                    ]
                }
            }
        }

        response = requests.get(
            self.endpointOS + nameIndex + '/_search',
            auth=HTTPBasicAuth(self.username, self.password),
            headers={'Content-Type': 'application/json'},
            data=json.dumps(body)
        )

        return response.json()
    
    def updateItem(self, nameTable, nameIndex, item):
        checkExistItemDB = self.client.query(
            TableName=nameTable,
            ExpressionAttributeNames={
                '#id': 'id'
            },
            ExpressionAttributeValues={
                ':id': {
                    'S': item.get('id')
                }
            },
            KeyConditionExpression='#id = :id'
        )
        
        if (checkExistItemDB.get("Count") != 0):
            resDB = self.client.update_item(
                TableName=nameTable,
                Key={
                    'id': {
                        'S': item.get('id')
                    }
                },
                ExpressionAttributeNames={
                    '#username': 'username',
                    '#email': 'email',
                    '#gender': 'gender',
                    '#phone': 'phone',
                    '#imagebase64': 'imagebase64',
                    '#country': 'country'
                },
                ExpressionAttributeValues={
                    ':username': {
                        'S': item.get('username', checkExistItemDB.get('Items')[0].get('username').get('S'))
                    },
                    ':email': {
                        'S': item.get('email', checkExistItemDB.get('Items')[0].get('email').get('S'))
                    },
                    ':gender': {
                        'S': item.get('gender', checkExistItemDB.get('Items')[0].get('gender').get('S'))
                    },
                    ':phone': {
                        'S': item.get('phone', checkExistItemDB.get('Items')[0].get('phone').get('S'))
                    },
                    ':imagebase64': {
                        'S': item.get('imagebase64', checkExistItemDB.get('Items')[0].get('imagebase64').get('S'))
                    },
                    ':country': {
                        'S': item.get('country',  checkExistItemDB.get('Items')[0].get('country').get('S'))
                    }
                },
                UpdateExpression='SET #username = :username, #email = :email,' +
                '#country = :country, #gender = :gender,' +
                '#phone = :phone, #imagebase64 = :imagebase64',
                ReturnValues='ALL_NEW',
            )
            
            resOS = requests.post(
                self.endpointOS + nameIndex + '/_update/' + item.get('id'),
                auth=HTTPBasicAuth(self.username, self.password),
                headers={'Content-Type': 'application/json'},
                data=json.dumps({
                    "doc": item
                })
            )

            return {
                'statusCode': 200,
                'message': 'Update item success.'
            }
        else:
            return {
                'statusCode': 400,
                'message': 'Item is not exists.'
            }

    def postItem(self, nameTable, nameIndex, item):
        checkItemExistOS = self.filterItem(nameIndex=nameIndex, item=item)
        
        if (checkItemExistOS.get('hits').get('total').get('value') != 0):
            return {
                'statuscode': 400,
                'message': 'Item is exist.'
            }

        response = requests.post(
            self.endpointOS + nameIndex + '/_doc',
            auth=HTTPBasicAuth(self.username, self.password),
            headers={'Content-Type': 'application/json'},
            data=json.dumps(item)
        )

        if (response.json().get('result') == 'created'):
            urlImage = self.s3.putItemImage(
                item=item.get('imagebase64'), key=response.json().get('_id'), nameBucket='customerss')

            res = self.client.put_item(
                TableName=nameTable,
                Item={
                    'id': {
                        'S': response.json().get('_id')
                    },
                    'username': {
                        'S': item.get('username', '')
                    },
                    'email': {
                        'S': item.get('email', '')
                    },
                    'gender': {
                        'S': item.get('gender', '')
                    },
                    'country': {
                        'S': item.get('country', '')
                    },
                    'password': {
                        'S': item.get('password', '')
                    },
                    'phone': {
                        'S': item.get('phone', '')
                    },
                    'urlImage': {
                        'S': urlImage
                    },
                    'imagebase64': {
                        'S': item.get('imagebase64', '')
                    },
                }
            )

            return {
                'statusCode': 200,
                'message': 'Create item success.'
            }

        return {
            'statusCode': 500,
            'message': 'Somthing wrong from server.'
        }

    def postLogin(self, nameTable, nameIndex, item):
        checkItemExist = self.filterItem(nameIndex=nameIndex, item=item)
        # print(checkItemExist)
        
        if (checkItemExist.get('hits').get('total').get('value') != 0):

            idItem = checkItemExist.get('hits').get('hits')[0].get('_id')
            checkExitItemDB = self.client.query(
                TableName=nameTable,
                ExpressionAttributeNames={
                    '#id': 'id'
                },
                ExpressionAttributeValues={
                    ':id': {
                        'S': str(idItem)
                    }
                },
                KeyConditionExpression='#id = :id'
            )
            
            if (checkExitItemDB.get("Count") != 0) and checkExitItemDB.get('Items')[0].get('password').get('S') == item.get('password'):
                return {
                    'statusCode': 200,
                    'message': 'Login success.'
                }

        return {
            'statusCode': 500,
            'message': 'Somthing wrong.'
        }

    def postForgotPassword(self, nameTable, nameIndex, item):
        checkItemExist = self.filterItem(nameIndex=nameIndex, item=item)

        if (checkItemExist.get('hits').get('total').get('value') != 0):
            idItem = checkItemExist.get('hits').get('hits')[0].get('_id')

            checkExitItemDB = self.client.query(
                TableName=nameTable,
                ExpressionAttributeNames={
                    '#id': 'id'
                },
                ExpressionAttributeValues={
                    ':id': {
                        'S': str(idItem)
                    }
                },
                KeyConditionExpression='#id = :id'
            )

            if (checkExitItemDB.get("Count") != 0):
                res = self.client.update_item(
                    TableName=nameTable,
                    Key={
                        'id': {
                            'S': str(idItem)
                        }
                    },
                    ExpressionAttributeNames={
                        '#password': 'password',
                    },
                    ExpressionAttributeValues={
                        ':password': {
                            'S': item.get('newpassword')
                        },
                    },
                    UpdateExpression='SET #password = :password',
                    ReturnValues='ALL_NEW',
                )
                
                dataUpdateOS = {
                    "doc": {
                        "password": item.get('newpassword')
                    }
                }
                resOS = requests.post(
                    self.endpointOS + nameIndex + '/_update/' + str(idItem),
                    auth=HTTPBasicAuth(self.username, self.password),
                    headers={'Content-Type': 'application/json'},
                    data=json.dumps(dataUpdateOS)
                )
                return {
                    'statusCode': 200,
                    'message': 'Update password success'
                }

        return {
            'statusCode': 500,
            'message': 'Item is not exists.'
        }

    def deleteItem(self, nameTable, nameIndex, item):
        checkItemExist = self.filterItem(nameIndex=nameIndex, item=item)

        if (checkItemExist.get('hits').get('total').get('value') != 0):
            idItem = checkItemExist.get('hits').get('hits')[0].get('_id')

            checkExitItemDB = self.client.query(
                TableName=nameTable,
                ExpressionAttributeNames={
                    '#id': 'id'
                },
                ExpressionAttributeValues={
                    ':id': {
                        'S': str(idItem)
                    }
                },
                KeyConditionExpression='#id = :id'
            )

            if (checkExitItemDB.get("Count") != 0):
                res = self.client.delete_item(
                    TableName=nameTable,
                    Key={
                        'id': {
                            'S': str(idItem)
                        }
                    }
                )
                
                resOS = requests.delete(
                    self.endpointOS + nameIndex + '/_doc/' + str(idItem),
                    auth=HTTPBasicAuth(self.username, self.password),
                    headers={'Content-Type': 'application/json'}
                )
                return {
                    'statusCode': 200,
                    'message': 'Delete item success.'
                }

        return {
            'statusCode': 500,
            'message': 'Item is not exists.'
        }