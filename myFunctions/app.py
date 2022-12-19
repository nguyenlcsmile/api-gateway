import json
import boto3
from io import BytesIO
# from wkhtmltopdf import WKHtmlToPdf


def index(event, context):
    return {
        'body': json.dumps(event)
    }
