from pymongo import MongoClient
import time
client = MongoClient("localhost", 27017)

db = client['all-web-data']

raw_data_collection = db["raw_data"]
raw_cm_data_collection = client["all-cm-data"]["raw_data"]


print("HELLO")
"""
all_document = raw_data_collection.find()
index = 0
for single_document in all_document:
    if "trivago" in single_document["title"] and ("Google" in single_document["title"]
     or "Yahoo" in single_document["title"] or "楽天" in single_document["title"]):
        print(single_document["title"])
        print(single_document)
        index += 1
print("共找到{}条搜索结果".format(index))
"""

index = 0
for single_cm_data in raw_cm_data_collection.find():
    index += 1
    # print(single_cm_data["advertiser_code"])
    if(single_cm_data["advertiser_code"] == "154LS"):
        # print("FOUND!")
        print(single_cm_data)
        start_time = time.localtime(single_cm_data["timestamp"])
        print("广告名：{}\n放送开始时间：{}\n广告长度：{}s"
              .format(single_cm_data["advertiser_name(jap)"], start_time, single_cm_data["last_time"]))
print("共找到{}个结果".format(index))