from pymongo import MongoClient

client = MongoClient("localhost", 27017)
db = client["all-web-data"]
original_data = db["raw_data"]

google_data = db["google_data"]
yahoo_data = db["yahoo_data"]

for single_document in original_data.find():
    if single_document["url"].startswith("www.google.co.jp") and single_document["title"].endswith("Google 検索"):
        print("匹配到谷歌搜索！")
        google_data.insert_one(single_document)
    elif single_document["url"].startswith("www.yahoo.co.jp") and single_document["title"].endswith("検索"):
        print("匹配到雅虎搜索！")
        yahoo_data.insert_one(single_document)