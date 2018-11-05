from pymongo import MongoClient
import progressbar

client = MongoClient("localhost", 27017)
collection_names = client["test-100-data"].collection_names()
test_100_web_data = client["test-100-web-data"]
web_data_db = client["all-web-data"]
original_divided_web_data = web_data_db["divided_data"]
bar = progressbar.ProgressBar(max_value=len(collection_names))
index = 0
for collection_name in collection_names:
    print("匹配household number{}".format(collection_name))
    bar.update(index)
    original_divided_web_data.aggregate([
        {"$match": {"household_num": collection_name}},
        {"$sout": "web_data_tmp"}
    ])
    test_100_web_data[collection_name].insert_many(list(web_data_db["web_data_tmp"]))
    index += 1






