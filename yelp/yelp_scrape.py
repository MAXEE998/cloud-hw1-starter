import requests
import json

cuisine = "Thai"

url = "https://api.yelp.com/v3/businesses/search?term=%s&location=NYC&limit=50&radius=40000&offset=%d"
payload = {}
headers = {
    "Authorization": "Bearer XeBI4aXMvzwhzj9UwlF48iqa-VizYvzBwFJAz-us9Ao0ltVvgFW6C0avwBj4VyLbE-3IyX7mxpLJyeDv4oo9oqRnIPss7O5zolkYKDiOkbLZVBtO-ZckXeiI3n1bXHYx"
}

store = {"businesses": []}

try:
    while len(store["businesses"]) < 1000:
        response = requests.request("GET", url % (cuisine, len(store["businesses"])), headers=headers, data=payload)
        parsed = json.loads(response.text)
        store["businesses"].extend(parsed["businesses"])
        print(len(store["businesses"]), "/ 1000")
finally:
    with open(cuisine+".json", "w") as f:
        f.write(json.dumps(store, indent=4))
