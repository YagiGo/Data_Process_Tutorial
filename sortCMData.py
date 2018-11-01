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
    collection_names = cm_data_test_db.collection_names()
    collection_size = len(collection_names)
    for collection_name in collection_names:
        cm_data_test_db[collection_name].create_index({"timestamp"})


    return


if __name__ == "__main__":
    sortDataIntoCollection()