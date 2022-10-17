import logging
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

sqs = boto3.client('sqs')
queue_url = 'https://sqs.us-east-1.amazonaws.com/814655805539/DiningSuggestionRequests'

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

def compose_email(attributes, recommendations):
    template = "Hello! Here are my %s restaurants suggestions for %s people, for today at %s:\n %s. \n\nEnjoy your meal!"
    restaurant_template = "%d. %s at \n%s"
    restaurant_strings = []
    for i, each in enumerate(recommendations):
        restaurant_strings.append(restaurant_template % (i+1, each["name"], "\n".join(each["address"]["display_address"])))
    return template % (
        attributes["cuisine"],
        attributes["numberOfPeople"],
        attributes["diningTime"],
        "\n\n".join(restaurant_strings)
    )

def process_request(attributes):
    ids = search(attributes["cuisine"])
    batch_keys = {
        restaurant_table.name: {
            'Keys': [{'id': id} for id in ids]
        },
    }
    response = dynamoDB.batch_get_item(RequestItems=batch_keys)

    email_body = compose_email(attributes, response["Responses"]["yelp-restaurants"])
    response = ses.send_email(
            Source='maxeehungngai@gmail.com',
            Destination={
                    'ToAddresses': [
                        attributes["email"],
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

def process_attributes(raw):
    return {
        "email": raw["email"]["StringValue"],
        "cuisine": raw["cuisine"]["StringValue"],
        "numberOfPeople": raw["numberOfPeople"]["StringValue"],
        "location": raw["location"]["StringValue"],
        "diningTime": raw["diningTime"]["StringValue"],
    }

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
            attrbutes = process_attributes(msg["MessageAttributes"])
            process_request(attrbutes)
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
        return {"processed": len(response["Messages"])}
    return {"processed": 0}