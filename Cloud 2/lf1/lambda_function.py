import json
import boto3
import urllib.parse
import requests
import base64
from requests_aws4auth import AWS4Auth

# credentials = boto3.Session().get_credentials()
# awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)


def rekognition_function(bucket_name, file_name):
    #adding comment in function
    print("Inside function")
    print(bucket_name)
    print(file_name)
    client = boto3.client('rekognition')
    print("client initialized")
    try:
        response = client.detect_labels(
            Image={
                'S3Object':{
                    'Bucket':bucket_name, 
                    'Name': file_name
                }
            }, 
            MaxLabels=10 
        )
    except Exception as e:
        print("Exception occurred:", e)
    print("got response")
    label_names = []

    label_names = list(map(lambda x:x['Name'], response['Labels']))
    label_names = [x.lower() for x in label_names]
    print("label namesssssss", label_names)
    return label_names


def store_json_elastic_search(json_object):
    region = 'us-east-1' 
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    
    # host = 'https://search-elasticdomain-3jxnwsonmydt2n7ct3njgaz7mi.us-east-1.es.amazonaws.com/'
    host = "https://search-photos-new-awbo5syy5gjznskb3oflfggoa4.us-east-1.es.amazonaws.com/"
    index = 'photos'
    url = host + index + "/_doc"
    headers = {"Content-Type": "application/json"}
    
    print("json.dumps(json_object)",json.dumps(json_object))
    try:
        resp = requests.post(url,auth=awsauth, json = json_object,headers = headers)
    except Exception as e:
        print("Exception occurred in opensearch:", e)
    print("Request posted")
    print("resp",resp.text)


    
def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.client('s3')
    record = event['Records'][0]
    print("abc")
    print("event : ", event)
    s3Object = record['s3']
    print("s3Object", s3Object)
    bucket = s3Object['bucket']['name']
    file_name = s3Object['object']['key']
    print("file_name", file_name)
    key = s3Object['object']['key']
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
    except Exception as e:
        print("Exception occurred in s3.head_object",e)
    print("head_object : " , response)
    if response["Metadata"]:
        customlabels = response["Metadata"]["customlabels"]
        print("customlabels : ", customlabels)
        customlabels = customlabels.split(',')
        customlabels = list(map(lambda x: x.lower(), customlabels))
    time_stamp = record['eventTime']
    print("Timestamp is :")
    print(time_stamp)
    label_names = rekognition_function(bucket, file_name)
    if response["Metadata"]:
        for cl in customlabels:
            print(cl)
            cl = cl.lower().strip()
            if cl not in label_names:
                label_names.append(cl)
    print("label_names",label_names)
    # print(label_names)
    json_object = {
        'objectKey': s3Object['object']['key'],
        'bucket': bucket,
        'createdTimestamp': time_stamp,
        'labels': label_names
    }
    print("json_object",json_object)
    store_json_elastic_search(json_object)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
        
