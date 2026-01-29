import json

def extract_data():
    data = [
        {"rating": 5, "review": "Great service", "city": "Phoenix"},
        {"rating": 3, "review": "Average experience", "city": "Tempe"}
    ]
    with open("/tmp/raw_reviews.json", "w") as f:
        json.dump(data, f)
