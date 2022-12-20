import boto3
import base64
import io


class S3_Bucket:
    def __init__(self):
        self.client = boto3.client('s3')

    def getItem(self, key, nameBucket):
        data = self.client.get_object(
            Bucket=nameBucket,
            Key=key
        )
        dataRead = data.get('Body').read()

        return dataRead.decode('utf-8')

    def putItemImage(self, item, key, nameBucket):
        if (item is None):
            return ''

        data = base64.b64decode(str(item))
        file = io.BytesIO(data)

        res = self.client.put_object(
            Body=file,
            Bucket=nameBucket,
            ContentType='image/png',
            Key=key + '.png'
        )

        url = '{}/{}/{}'.format(self.client.meta.endpoint_url, nameBucket, key)

        return url
