import json
import time


def lambda_handler(event, context):
    boilerplate = {"messages": [{
        "type": "unstructured",
        "unstructured": {
            "id": "01",
            "text": "Application under development. Search functionality will be implemented in Assignment 2",
            "timestamp": str(time.time())
        }
    }]}
    return boilerplate
