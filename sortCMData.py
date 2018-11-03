from pymongo import MongoClient
import time
import datetime

client = MongoClient("localhost", 27017)
cm_data_db = client["all-cm-data"]
cm_raw_data = cm_data_db["raw_data"]
cm_data_test_db = client["cm-data-test-db"]

def sortDataIntoCollection():
    # 先把广告数据按天分到collection中
    index = 0
    cm_data_size = cm_raw_data.estimated_document_count()
    for single_document in cm_raw_data.find():
        index += 1
        print("正在处理{}条数据，共有{}条数据".format(index, cm_data_size))
        cm_date_time = single_document["date"] + " " + "00:00:00" # 看这广告是哪天播放的
        timestamp = time.mktime(datetime.datetime.strptime(cm_date_time, "%Y-%m-%d %H:%M:%S").timetuple())
        if 1506801600 <= int(timestamp) <= 1522526399:
            cm_data_test_db[str(timestamp)].insert(single_document)
        else:
            print(timestamp)

def sortDataWithinCollection():
    # 然后把广告数据按时间戳排序
    # TEST HERE!
    collection_names = cm_data_test_db.collection_names()
    print(collection_names)
    current_number = 0
    for collection_name in collection_names:
        current_number += 1
        print("正在处理第{}个，共有{}个".format(current_number, len(collection_names)))
        cm_data_collection = cm_data_test_db[collection_name]
        for single_data in cm_data_collection.find():
            index = int((single_data["timestamp"] - float(collection_name)) / 15)
            cm_data_collection.update(
                {"_id": single_data["_id"]},
                {"$set": {"broadcast_index": index}})
    """
    sorted_documents = list(cm_data_test_db["1506870000.0"].find().sort("timestamp", 1))
    cm_data_test_db["1506870000.0"].drop()
    cm_data_test_db["1506870000.0"].insert_many(sorted_documents)
    """


if __name__ == "__main__":
    # sortDataIntoCollection()
    sortDataWithinCollection()