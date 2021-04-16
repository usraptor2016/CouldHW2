import json
import logging
import boto3
import time
#######sfasfsda
# from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection
import urllib.parse

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
ENDPOINT='search-photos-toice5g2dpgjyr4bejqtm5gns4.us-east-1.es.amazonaws.com'

def lambda_handler(event, context):
    credentials = boto3.Session().get_credentials()
    logger.debug(credentials)
    # auth = AWS4Auth(credentials.access_key, 
    #                 credentials.secret_key, 
    #                 'us-east-1', 'es', 
    #                 session_token=credentials.token)
    es = Elasticsearch(
        hosts = [{'host': ENDPOINT, 'port': 443}],
        http_auth = ('yl4736','QQhao-403459819'),
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )
    record = event['Records'][0]
    s3_object= record['s3']
    bucket = s3_object['bucket']['name']
    object_key = urllib.parse.unquote_plus(s3_object['object']['key'])
    image = {
        'S3Object': {
            'Bucket': bucket,
            'Name': object_key
        }
    }
    print(image)
    rekognition = boto3.client('rekognition')
    response = rekognition.detect_labels(Image=image)
    labels = []
    for i in range(len(response['Labels'])):
        labels.append(response['Labels'][i]['Name'])
    timestamp =time.time()
    
    es_object = json.dumps({
            'objectKey' : object_key,
            'bucket' : bucket,
            'createdTimestamp' : timestamp,
            'labels' : labels
    })
    es.index(index="photos", doc_type="Photo", id=object_key, body=es_object, refresh=True)
    return {
        'statusCode': 200,
        'body': json.dumps('Uploaded Photo successfully indexed')
    }
