from pymongo import MongoClient

client = MongoClient("localhost", 27017)
web_data = client["all-web-data"]["divided_data"]

def remove_zero(string):
    index = 0
    for index in range(len(string)):
        if(string[index] != "0"):
            break
    return string[index:]

index = 0
size = web_data.estimated_document_count()
for single_document in web_data.find():
    index += 1
    print("正在处理{}个，共有{}个".format(index, size))
    web_data.update(
        {"_id": single_document["_id"]},
        {"$set": {"household_num": remove_zero(single_document["household_num"])}}
    )