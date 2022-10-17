import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

sqs = boto3.client('sqs')
queue_url = 'https://sqs.us-east-1.amazonaws.com/814655805539/DiningSuggestionRequests'

def print(m):
    logger.debug(m)

def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    sessionState = {
        'dialogAction': {
            'type': 'Delegate'
        },
        'intent': {
            'name': event["sessionState"]["intent"]["name"],
            'state': 'Fulfilled',
        },
    }
    
    slots = event["sessionState"]["intent"]["slots"]
    # Send message to SQS queue
    responseSQS = sqs.send_message(
        QueueUrl=queue_url,
        MessageAttributes={
            'email': {
                'DataType': 'String',
                'StringValue': slots["email"]["value"]["interpretedValue"]
            },
            'cuisine': {
                'DataType': 'String',
                'StringValue': slots["cuisine"]["value"]["originalValue"]
            },
            'numberOfPeople': {
                'DataType': 'Number',
                'StringValue': slots["numberOfPeople"]["value"]["interpretedValue"]
            },
            'location': {
                'DataType': 'String',
                'StringValue': slots["location"]["value"]["originalValue"]
            },
            'diningTime': {
                'DataType': 'String',
                'StringValue': slots["diningTime"]["value"]["interpretedValue"]
            },
        },
        MessageBody=(
            'Dining Suggestion Request'
        ),
        #MessageGroupId="default"
    )

    print(responseSQS['MessageId'])
    

    response = {
        "sessionState": sessionState,
    }
    return response
