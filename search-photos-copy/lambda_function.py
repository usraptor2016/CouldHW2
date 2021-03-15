import json
import boto3
import logging
from elasticsearch import Elasticsearch, RequestsHttpConnection

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

ENDPOINT='search-photos-toice5g2dpgjyr4bejqtm5gns4.us-east-1.es.amazonaws.com'
def search_intent(labels):
    es = Elasticsearch(
        hosts = [{'host': ENDPOINT, 'port': 443}],
        http_auth = ('yl4736','QQhao-403459819'),
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )
    responses = []
    for label in labels:
        if label is not None and label!='':
            data = es.search(index='photos',
                body={
                    'query':{
                        'match':{
                            'labels':label
                        }
                    }
                }
            )
            responses.append(data)
    object_keys=[]
    for resp in responses:
        if 'hits' in resp:
            for hits in resp['hits']['hits']:
                object_key = hits['_source']['objectKey']
                if object_key not in object_keys:
                    object_keys.append(object_key)
    
    return object_keys

def lambda_handler(event, context):
    lex =boto3.client('lex-runtime')
    response = lex.post_text(
        botName='queryAlbum',
        botAlias='querybot',
        userId='user',
        inputText= event['queryStringParameters']['q']
    )
    print('repsonse from lex:')
    print(response)
    if 'slots' in response:
        labels = [response['slots']['A'],response['slots']['B'],response['slots']['C']]
        print (labels)
        object_keys = search_intent(labels)
    
    if object_keys:
        return {
            'statusCode': 200,
            'headers': {"Access-Control-Allow-Origin":"*","Access-Control-Allow-Credentials":True,"Content-Type":"application/json"},
            'body': json.dumps(object_keys)
        }
    else:
        return {
            'statusCode':404,
            "headers": {"Access-Control-Allow-Origin":"*","Access-Control-Allow-Credentials":True,"Content-Type":"application/json"},
            'body':json.dumps('Not Found')
        }
