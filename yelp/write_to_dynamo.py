import boto3 
from botocore.exceptions import ClientError
import json
import time

cuisine = ["Chinese", "Indian", "Italian", "Japanese", "Mexico", "Thai"]
dynamodb = boto3.resource('dynamodb',  region_name="us-east-1")
table = dynamodb.Table('yelp-restaurants')
table_data = []

def process_data(data):
    processed = {}
    processed["id"] = data["id"]
    processed["name"] = data["name"]
    processed["address"] = data["location"]
    processed_coords = {
        "latitude": str(data["coordinates"]["latitude"]),
        "longitude": str(data["coordinates"]["longitude"])
    }
    processed["coordinates"] = processed_coords
    processed["review_count"] = data["review_count"]
    processed["rating"] = str(data["rating"])
    processed["zip_code"] = data["location"]["zip_code"]
    processed["categories"] = data["categories"]
    processed["inserted_at"] = int(time.time())
    return processed


def load_data():
    for each in cuisine:
        with open(each+".json", "r") as f:
            data = json.load(f)
            for business in data['businesses']:
                table_data.append(process_data(business))

def check_duplicate_id():
    ids = set()
    for each in table_data:
        ids.add(each["id"])
    print("Distinct id: %d" % len(ids))


# snippet-start:[python.example_code.dynamodb.PutItem_BatchWriter]
def fill_table(table, table_data):
    """
    Fills an Amazon DynamoDB table with the specified data, using the Boto3
    Table.batch_writer() function to put the items in the table.
    Inside the context manager, Table.batch_writer builds a list of
    requests. On exiting the context manager, Table.batch_writer starts sending
    batches of write requests to Amazon DynamoDB and automatically
    handles chunking, buffering, and retrying.
    :param table: The table to fill.
    :param table_data: The data to put in the table. Each item must contain at least
                       the keys required by the schema that was specified when the
                       table was created.
    """
    try:
        with table.batch_writer() as writer:
            for item in table_data:
                writer.put_item(Item=item)
        print("Loaded data into table %s." % table.name)
    except ClientError:
        print("Couldn't load data into table %s." % table.name)
        raise
# snippet-end:[python.example_code.dynamodb.PutItem_BatchWriter]

if __name__ == "__main__":
    load_data()
    check_duplicate_id()
    fill_table(table, table_data)