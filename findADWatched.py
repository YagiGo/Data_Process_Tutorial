#  查出有哪些广告被用户看见了
from pymongo import MongoClient
from progressbar import *
import multiprocessing # 用多进程优化速度


def matchingCMAndUser(start_point, end_point):
    print("第{}进程正在工作".format(multiprocessing.current_process()))
    print("从{}出发到{}条数据".format(start_point, end_point))
    client = MongoClient("localhost", 27017)
    cm_data_collection = client["all-cm-data"]["raw_data"]
    tv_watch_data_collection = client["all-tv-orgn-data"]["raw_data"]

    cm_user_match_db = client["cm_user_match"]
    cm_user_match_collection = cm_user_match_db["raw_data"]

    # cm_user_match_collection.remove()

    tv_watch_data_collection_size = tv_watch_data_collection.count()
    index = start_point
    # print(tv_watch_data_collection)
    for single_tv_watch_data in tv_watch_data_collection.find():
        if start_point <= index <= end_point:
            print("正在对第{}条电视观看数据进行匹配，共有{}条数据".format(index, tv_watch_data_collection_size))
            index += 1
            start_timestamp = single_tv_watch_data["start_timestamp"]
            end_timestamp = single_tv_watch_data["end_timestamp"]
            for single_cm_data in cm_data_collection.find():
                if start_timestamp <= single_cm_data["timestamp"] <= end_timestamp and single_tv_watch_data["TV_station_code"] == single_cm_data["TV_station_code"]:
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
                    cm_user_match_collection.insert_one(cm_user_watch_document)
        else:
            break

#  把一个任务分成八部分，分给八个核
pool = multiprocessing.Pool(processes=8)
client = MongoClient("localhost", 27017)
tv_watch_data_collection = client["all-tv-orgn-data"]["raw_data"]
tv_watch_data_collection_size = tv_watch_data_collection.count()
chunck_size = int(tv_watch_data_collection_size / 30)
if __name__ == "__main__":
    pool = multiprocessing.Pool(processes=30)
    for i in range(30):
        pool.apply_async(matchingCMAndUser, args=(i*chunck_size, (i+1)*chunck_size))
    pool.close()
    pool.join()




