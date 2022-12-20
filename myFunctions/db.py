import boto3
from s3 import S3_Bucket
from boto3.dynamodb.conditions import Key


class dynamoDB:
    def __init__(self):
        self.client = boto3.client('dynamodb')
        self.s3 = S3_Bucket()

    def putAllItems(self, items, nameTable):
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
                        'image': {
                            'S': urlImage
                        }
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
        res = self.client.scan(TableName=nameTable)
        return {
            'statusCode': 200,
            'message': 'Get all items success.',
            'data': res.get("Items", "Not exists items in table!!!")
        }

    def getItem(self, nameTable, itemId):
        res = self.client.query(
            TableName=nameTable,
            KeyConditions={
                'id': {
                    'AttributeValueList': [
                        {
                            'S': str(itemId),
                        }
                    ]
                }
            },
        )

        print(res)

        return {
            'statusCode': 200,
            'message': 'Get item success.',
            'data': res.get("Items", "Not exists items in table!!!")
        }
