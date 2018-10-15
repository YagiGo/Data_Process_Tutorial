from pymongo import MongoClient
import uuid

client = MongoClient("localhost", 27017);
db = client['user-data']

raw_data_collection = db["raw_data"]

cursor = raw_data_collection.aggregate([
    { "$group" : {"_id" : "$useragent", "num" : {"$sum":1}}}
])

user_collection = db["user-collection"]
user_collection.remove() # 插入前先删除
user_collection.insert_many(list(cursor))

all_users = user_collection.find()
index = 1

print("开始按用户对数据进行分别")
for user in all_users:
    user_history = list(raw_data_collection.find({"useragent": user["_id"]}))

    print("正在插入第{}个用户的数据".format(index))

    user_identification = str(uuid.uuid5(uuid.NAMESPACE_DNS, user["_id"]))
    user_history_collection = db[user_identification]
    user_history_collection.remove() # # 插入前先删除
    # print(list(user_history))
    user_history_collection.insert_many(user_history)
    index += 1

print("按用户分别数据处理完成")
