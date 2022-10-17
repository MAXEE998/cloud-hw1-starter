from email import message
import json
import time
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
client = boto3.client('lexv2-runtime')
botId='ADAEZAVMZP'
botAliasId='TSTALIASID'
localeId='en_US'
sessionId='assignment2'

def print(m):
    logger.debug(m)

def lambda_handler(event, context):
    # TODO implement
    response = client.recognize_text(
        botId=botId,
        botAliasId=botAliasId,
        localeId=localeId,
        sessionId=sessionId,
        text=event["messages"][0]["unstructured"]["text"],
    )

    boilerplate = {"messages": [{
        "type": "unstructured",
        "unstructured": {
            "id": str(i).zfill(2),
            "text": message["content"],
            "timestamp": str(time.time())
        }
    } for i, message in enumerate(response["messages"])]}
    return boilerplate