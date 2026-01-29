import json
from pymongo import MongoClient

def load_data():
    client = MongoClient("mongodb://mongo:27017/")
    db = client.reviews
    collection = db.google_reviews

    with open("/tmp/clean_reviews.json") as f:
        data = json.load(f)

    collection.insert_many(data)
