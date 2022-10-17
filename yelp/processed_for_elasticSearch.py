import json

cuisine = ["Chinese", "Indian", "Italian", "Japanese", "Mexico", "Thai"]
dictionary = {}

def load_data():
    for each in cuisine:
        with open(each+".json", "r") as f:
            data = json.load(f)
            for business in data['businesses']:
                if business["id"] not in dictionary:
                    dictionary[business["id"]] = each

def write_to_file():
    documents = []
    for each in dictionary:
        documents.append(
            {
                'id': each,
                'cuisine': dictionary[each]
            }
        )
    with open("elasticSearchDocs.json", "w") as f:
        json.dump(documents, f)

if __name__ == "__main__":
    load_data()
    write_to_file()
