import boto3
from s3 import S3_Bucket


class dynamoDB:
    def __init__(self):
        self.client = boto3.client('dynamodb')
        self.s3 = S3_Bucket()

    def putAllItems(self, items):
        for item in items:
            urlImage = self.s3.putItemImage(
                item=item[-1], key=item[0], nameBucket='customerss')

            res = self.client.put_item(
                TableName='my-table',
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

        return {
            'statuscode': 200,
            'message': 'Post all items success.'
        }