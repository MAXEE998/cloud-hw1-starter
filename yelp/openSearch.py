from urllib import response
from opensearchpy import OpenSearch, RequestsHttpConnection
import json

host = "search-assignment2-m3x5c4zswkalnoqpcpjqv47apm.us-east-1.es.amazonaws.com"  # cluster endpoint, for example: my-test-domain.us-east-1.es.amazonaws.com


index_name = "restaurants"

client = OpenSearch(
    hosts=[{"host": host, "port": 443}],
    http_auth=("maxee998", "Admin1234!"),
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
)


def create_index():
    # Create an index with non-default settings.
    index_body = {"settings": {"index": {"number_of_shards": 4}}}

    response = client.indices.create(index_name, body=index_body)
    print("\nCreating index:")
    print(response)

def add_documents():
    create_template = '{ "create" : { "_index" : "restaurants", "_id" : "%s" } }\n{"cuisine": "%s"}'
    docs = []

    with open("elasticSearchDocs.json", "r") as f:
        data = json.load(f)
        for each in data:
            docs.append(create_template % (each["id"], each["cuisine"]))

    print("Number of docs to write: %d" % len(docs))    
    response = client.bulk(body="\n".join(docs))
    print(response)

def search():
    q = 'thai'
    query = {
    'size': 5,
    'query': {
        'multi_match': {
        'query': q,
        'fields': ['cuisine']
        }
    }
    }

    response = client.search(
        body = query,
        index = index_name
    )
    print('\nSearch results:')
    print(json.dumps(response, indent=4))

if __name__ == "__main__":
    search()
