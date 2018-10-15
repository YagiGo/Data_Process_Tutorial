from pymongo import MongoClient
client = MongoClient("localhost", 27017)

db = client['user-data']

raw_data_collection = db["raw_data"]

all_document = raw_data_collection.find()
for single_document in (list(all_document)):
    if single_document["url"].startswith("www.google.co.jp") and single_document["title"].endswith("Google 検索"):
        print(single_document["url"])
        print(single_document["title"])