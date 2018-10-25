from pymongo import MongoClient
import time
import datetime
client = MongoClient("localhost", 27017)
db = client["all-web-data"]
original_data = db["raw_data"]
divided_data = db["divided_data"]

start_time = "2017-10-1 05:00:00"
end_time = "2018-4-1 04:59:59"

start_timestamp = time.mktime(datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").timetuple())
end_timestamp = time.mktime(datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").timetuple())
overall_data_size = original_data.estimated_document_count()
index = 1
# print(start_timestamp, end_timestamp)
for single_document in original_data.find():
    if start_timestamp <= single_document["timestamp"] <= end_timestamp:
        print("找到第{}条数据符合时间区分，共有{}条数据".format(index, overall_data_size))
        divided_data.insert_one(single_document)
        index += 1