from pymongo import MongoClient
import csv
import json
import sys
csv.field_size_limit(sys.maxsize)
import pandas as pd
client = MongoClient("localhost", 27017)
db = client['user-data']

# 创建一个文件放初始数据
raw_data_collection = db["raw_data"]
result = []

# 用CSV读csv
with open("data.csv",  encoding="utf-8") as csv_file:
    for row in csv.DictReader(csv_file):
        result.append(row)

json_data = json.loads(json.dumps(result))
# print(json_data)


raw_data_collection.remove()  # 先删除所有数据
print("开始载入数据。。。")
raw_data_collection.insert(json_data)  # 加载数据进数据库
print("数据载入完成")



