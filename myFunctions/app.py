import json
import pandas as pd
import io
from s3 import S3_Bucket
from db import dynamoDB


def body_data(data):
    # dataUsers = data.strip()
    # data = "".join(dataUsers.split())
    dataConvert = json.loads(data)
    return dataConvert


def param_data(data):
    dataJson = {}
    for key, val in data.items():
        dataJson[key] = val[0]
    return dataJson


def index(event, context):
    s3 = S3_Bucket()
    db = dynamoDB()

    urlRaw = event.get("path", None).split('/')
    urlReq = "/".join(urlRaw[2:])
    dataBody = None
    dataParam = None

    inforReq = {
        "header": event.get("headers", None),
        "path": event.get("path", None),
        "method": event.get("httpMethod", None),
        "body": event.get("body", None),
        "param": event.get("multiValueQueryStringParameters", None),
        "urlReq": urlReq
    }

    if (inforReq.get("param")):
        dataParam = param_data(inforReq.get("param"))

    # # Process data
    if (inforReq.get("body")):
        dataBody = body_data(inforReq.get("body"))

    #### =============================************============================####
    if inforReq.get("method") == "POST" and inforReq.get("path") == "/postAllItems":
        data = s3.getItem(key='data.csv', nameBucket='customerss')
        df = pd.read_csv(io.StringIO(data), sep=",").astype(str)
        columns = df.columns.tolist()
        listItems = df.values.tolist()
        res = db.putAllItems(
            items=listItems, nameTable='my-table', nameIndex='customers', columns=columns)

    elif inforReq.get("method") == "POST" and inforReq.get("path") == "/postItem" and dataParam and dataBody:
        res = db.postItem(nameTable=dataParam.get('table'),
                          nameIndex=dataParam.get('index'), item=dataBody)

    elif inforReq.get("method") == "GET" and inforReq.get("path") == "/getAllItems" and dataParam:
        res = db.getAllItems(nameTable=dataParam.get('table'))

    elif inforReq.get("method") == "GET" and inforReq.get("path") == "/getItem" and dataParam:
        res = db.getItem(nameTable=dataParam.get(
            'table'), itemId=dataParam.get('itemId'))

    elif inforReq.get("method") == "PUT" and inforReq.get("path") == "/updateItem" and dataParam and dataBody:
        res = db.updateItem(nameTable=dataParam.get('table'), nameIndex=dataParam.get('index'), item=dataBody)

    elif inforReq.get("method") == "POST" and inforReq.get("path") == "/login" and dataParam and dataBody:
        res = db.postLogin(nameTable=dataParam.get('table'),
                           nameIndex=dataParam.get('index'), item=dataBody)
        
    elif inforReq.get("method") == "POST" and inforReq.get("path") == "/forgot-password" and dataParam and dataBody:
        res = db.postForgotPassword(nameTable=dataParam.get('table'),
                           nameIndex=dataParam.get('index'), item=dataBody)
    
    elif inforReq.get("method") == "DELETE" and inforReq.get("path") == "/deleteItem" and dataParam and dataBody:
        res = db.deleteItem(nameTable=dataParam.get('table'),
                           nameIndex=dataParam.get('index'), item=dataBody)


    return {
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "*",
        },
        'body': json.dumps(res)
    }
