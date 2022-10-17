import logging
import boto3
import json
from opensearchpy import OpenSearch, RequestsHttpConnection
from decimal import *


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        # 👇️ if passed in object is instance of Decimal
        # convert it to a string
        if isinstance(obj, Decimal):
            return str(obj)
        # 👇️ otherwise use the default behavior
        return json.JSONEncoder.default(self, obj)


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

sqs = boto3.client('sqs')
queue_url = 'https://sqs.us-east-1.amazonaws.com/814655805539/DiningSuggestion.fifo'

openSearchHost = "search-assignment2-m3x5c4zswkalnoqpcpjqv47apm.us-east-1.es.amazonaws.com" 
openSearch = OpenSearch(
    hosts=[{"host": openSearchHost, "port": 443}],
    http_auth=("maxee998", "Admin1234!"),
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
)

dynamoDB = boto3.resource('dynamodb')
restaurant_table = dynamoDB.Table('yelp-restaurants')

ses = boto3.client('ses')

def print2(m):
    logger.debug(m)

def search(cuisine):
    q = cuisine
    query = {
    'size': 5,
    'query': {
        'multi_match': {
        'query': q,
        'fields': ['cuisine']
        }
    }
    }

    response = openSearch.search(
        body = query,
        index = "restaurants"
    )
    ids = []
    try:
        for hit in response["hits"]["hits"]:
            ids.append(hit["_id"])
    finally:
        return ids


def process_request(email, cuisine):
    ids = search(cuisine)
    batch_keys = {
        restaurant_table.name: {
            'Keys': [{'id': id} for id in ids]
        },
    }
    response = dynamoDB.batch_get_item(RequestItems=batch_keys)

    email_body = json.dumps(response, cls=DecimalEncoder, indent=4)
    response = ses.send_email(
            Source='maxeehungngai@gmail.com',
            Destination={
                    'ToAddresses': [
                        email,
                    ],
                },
                Message={
                    'Subject': {
                        'Data': 'Your dining recommendation',
                    },
                    'Body': {
                        'Text': {
                            'Data': email_body,
                        },
                    }
                },
        )
    print2(response)

def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        MessageAttributeNames=['All'],
    )

    if "Messages" in response:
        for msg in response["Messages"]:
            attrbutes = msg["MessageAttributes"]
            process_request(attrbutes["email"]["StringValue"], attrbutes["cuisine"]["StringValue"])
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
        return {"processed": len(response["Messages"])}
    return {"processed": 0}