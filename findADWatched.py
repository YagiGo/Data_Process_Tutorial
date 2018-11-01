#  查出有哪些广告被用户看见了
from pymongo import MongoClient
import multiprocessing # 用多进程优化速度
import time
import datetime
import traceback

def matchingCMAndUser(collectionName):
    print("在{}中进行匹配".format(collectionName))
    print("第{}进程正在工作".format(multiprocessing.current_process()))
    client = MongoClient("localhost", 27017)
    cm_data_collection = client["all-cm-data"]["raw_data"]
    # tv_watch_data_collection = client["all-tv-orgn-data"]["raw_data"]
    tv_watch_data_collection = client["all-tv-orgn-data"][collectionName]
    cm_data_db = client["cm-data-test-db"]
    cm_user_match_db = client["cm_user_match"]
    cm_user_match_collection = cm_user_match_db["raw_data"]

    # cm_user_match_collection.remove()

    tv_watch_data_collection_size = tv_watch_data_collection.count()
    index = 0
    # print(tv_watch_data_collection)
    tv_watch_data_cursor = tv_watch_data_collection.find().batch_size(20)
    for single_tv_watch_data in tv_watch_data_cursor:
        try:
            index += 1
            print("{}进程正在对第{}条电视观看数据进行匹配，共有{}条数据".format(multiprocessing.current_process(), index, tv_watch_data_collection_size))
            start_timestamp = single_tv_watch_data["start_timestamp"]
            end_timestamp = single_tv_watch_data["end_timestamp"]
            watched_date = single_tv_watch_data["date"] + " " + "00:00:00"
            timestamp = time.mktime(datetime.datetime.strptime(watched_date, "%Y-%m-%d %H:%M:%S").timetuple())
            print(timestamp)
            cm_matching_collection = cm_data_db[str(timestamp)]
            cm_matching_collection.aggregate([
                {"$match": {"timestamp": {"$gt": start_timestamp, "$lt": end_timestamp}}},
                {"$addFields": {
                    "user_watch_date": single_tv_watch_data["date"],
                    "user_watch_data_SEQ": single_tv_watch_data["data_SEQ"],
                    "user_watch_day_of_week": single_tv_watch_data["day of week"],
                    "user_watch_personal_num": single_tv_watch_data["personal_num"],
                    "user_watch_household_num": single_tv_watch_data["household_num"],
                    "user_watch_TVNo": single_tv_watch_data["TVNo"],
                    "user_watch_TV_station_code": single_tv_watch_data["TV_station_code"],
                    "user_watch_data_category": single_tv_watch_data["data_category"],
                    "user_watch_end_timestamp": single_tv_watch_data["end_timestamp"],
                    "user_watch_start_timestamp": single_tv_watch_data["start_timestamp"],
                    "user_watch_last_time": single_tv_watch_data["last_time"]
                }},
                {"$out": "watchedCM_tmp"}
            ])
            cm_user_match_collection.insert_many(list(cm_data_db["watchedCM_tmp"]))


            """
            cm_cursor = cm_data_collection.find(no_cursor_timeout=True)
            for single_cm_data in (cm_cursor):
                if start_timestamp <= single_cm_data["timestamp"] <= end_timestamp and single_tv_watch_data["TV_station_code"] == single_cm_data["TV_station_code"]:
                    # print("Process No.{} FIND MATCH".format(multiprocessing.current_process()))
                    cm_user_watch_document = {
                        "user_watch_date": single_tv_watch_data["date"],
                        "user_watch_data_SEQ": single_tv_watch_data["data_SEQ"],
                        "user_watch_day_of_week": single_tv_watch_data["day of week"],
                        "user_watch_personal_num": single_tv_watch_data["personal_num"],
                        "user_watch_household_num": single_tv_watch_data["household_num"],
                        "user_watch_TVNo": single_tv_watch_data["TVNo"],
                        "user_watch_TV_station_code": single_tv_watch_data["TV_station_code"],
                        "user_watch_data_category": single_tv_watch_data["data_category"],
                        "user_watch_end_timestamp": single_tv_watch_data["end_timestamp"],
                        "user_watch_start_timestamp": single_tv_watch_data["start_timestamp"],
                        "user_watch_last_time": single_tv_watch_data["last_time"],
                        "cm_brand_code": single_cm_data["brand_code"],
                        "advertiser_code": single_cm_data["advertiser_code"],
                        "cm_type": single_cm_data["CM_type"],
                        "cm_last_time": single_cm_data["last_time"],
                        "cm_tv_station_code": single_cm_data["TV_station_code"],
                        "cm_timestamp": single_cm_data["timestamp"]
                    }
                    print(cm_user_watch_document)
                    cm_user_match_collection.insert_one(cm_user_watch_document)
                    # print("插入成功")
            cm_cursor.close()
            """
        except Exception as e:
            traceback.print_exc()
            print("插入出错，无视此条记录")

#  把一个任务分成八部分，分给八个核
if __name__ == "__main__":
    pool = multiprocessing.Pool(processes=4)
    sub_collections = ["sub_collection_00", "sub_collection_01", "sub_collection_02", "sub_collection_03"]
    """
    for collection_name in sub_collections:
        pool.apply_async(matchingCMAndUser, args=(collection_name,))
    """
    # pool.close()
    # pool.join()
    matchingCMAndUser(sub_collections[3])



