import json

def transform_data():
    with open("/tmp/raw_reviews.json") as f:
        data = json.load(f)

    transformed = [
        {
            "rating": d["rating"],
            "review": d["review"].lower(),
            "city": d["city"].upper()
        }
        for d in data
    ]

    with open("/tmp/clean_reviews.json", "w") as f:
        json.dump(transformed, f)
