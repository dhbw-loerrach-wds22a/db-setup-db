from pymongo import MongoClient
import json

# Connect to MongoDB
client = MongoClient('localhost', 27017)
db = client.yelp_data
reviews_collection = db.reviews
tips_collection = db.tips

data_path = "./data/"
review_file = "yelp_academic_dataset_review.json"
tip_file = "yelp_academic_dataset_tip.json"


# Function to bulk insert reviews
def bulk_insert_reviews(file_path, collection, batch_size=1000):
    with open(file_path, 'r') as file:
        batch = []
        for line in file:
            review = json.loads(line)
            batch.append(review)
            if len(batch) >= batch_size:
                collection.insert_many(batch)
                batch = []
        if batch:  # Insert any remaining documents in the last batch
            collection.insert_many(batch)

# Function to bulk insert tips
def bulk_insert_tips(file_path, collection, batch_size=1000):
    with open(file_path, 'r') as file:
        batch = []
        for line in file:
            tip = json.loads(line)
            batch.append(tip)
            if len(batch) >= batch_size:
                collection.insert_many(batch)
                batch = []
        if batch:  # Insert any remaining documents in the last batch
            collection.insert_many(batch)

# Connect to MongoDB
client = MongoClient('localhost', 27017)
db = client.yelp_data
reviews_collection = db.reviews
tips_collection = db.tips

data_path = "./data/"
review_file = "yelp_academic_dataset_review.json"
tip_file = "yelp_academic_dataset_tip.json"

try:
    # Bulk insert reviews
    print("Inserting reviews...")
    bulk_insert_reviews(data_path + review_file, reviews_collection, 5000)

    # Bulk insert tips
    print("Inserting tips...")
    bulk_insert_tips(data_path + tip_file, tips_collection, 5000)

    print("All data has been inserted into MongoDB.")

except Exception as e:
    print(f"Error: {e}")

finally:
    client.close()