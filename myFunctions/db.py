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
                        'id': str(item[0]),
                        'username': str(item[1]),
                        'email': str(item[2]),
                        'gender': str(item[3]),
                        'country': str(item[4]),
                        'password': str(item[5]),
                        'phone': str(item[6]),
                        'image': str(urlImage)
                    }
                )

                return {
                    'statuscode': 200,
                    'message': 'Post all items success.'
                }

            except Exception as e:
                return {
                    'statusCode': 500,
                    'body': 'Something wrong from server!!!'
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
            KeyConditionExpression=Key('id').eq(str(itemId))
        )

        print(res)

        return {
            'statusCode': 200,
            'message': 'Get item success.',
            'data': res.get("Items", "Not exists items in table!!!")
        }
