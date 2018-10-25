from pymongo import MongoClient
import uuid # 用来生成用户ID
import multiprocessing as mp

# 用来生成时间戳
import time
import datetime
# 思路：根据用户ID找到各自的浏览数据，把浏览数据中的时间转换为时间戳
# 更新：先把傻逼的30小时时间制转成正常的24小时时间制，写两个函数可以相互转换时间
def convert30Hto24H(time):
    # 输入时间格式 2018-10-13 27:13:14
    record_date = time[:10].split("-")  # 1999-12-31 -> [1999,12,31]转换成数组
    record_time = time[11:].split(":")  # 27:13:14 -> [27,13,14]
    date_of_month = int(record_date[2]) # 转换成int用来做数据操作
    hour_of_time = int(record_time[0])
    if(hour_of_time > 23):
        # 如果跳到了第二天但是在用傻逼的30小时时间制
        hour_of_time -= 24
        record_time[0] = str(hour_of_time).zfill(2)  # zfill可以把1，2，3这种变为01，02，03
        # 把改了时间的时间再合并
        time_modified = "-".join(record_date) + " " + ":".join(record_time)
        # 现在变成了 1999-12-31 03:13:14,然后把时间从13变成14
        # 大月 31->1, 小月 30->1, 平年 02-28->03-01, 闰年 02-28->02-29 etc
        new_time = datetime.datetime.strptime(time_modified, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=1)
        return new_time


def convert24HTo30H(time):
    # 24小时制转成30小时制
    # 输入时间格式 2000-01-01 03：13：14
    # 把日期减一天 2000-01-01->1999-12-31
    # 大月 1->31, 小月 1->30, 平年 03-01->02-28, 闰年 03-01->02-29 etc
    new_date = str(datetime.datetime.strptime(time, "%Y-%m-%d %H:%M:%S") - datetime.timedelta(days=1))[:10]
    # new_date == 1999-12-31
    record_time = time[11:].split(":")  # 03:13:14 -> [03,13,14]
    hour_of_time = int(record_time[0])

    if (0 <= hour_of_time < 4):
        # 0点到4点的时间转换为30小时制
        hour_of_time += 24
        record_time[0] = str(hour_of_time).zfill(2)
        # 把改了时间的时间再合并
        new_time = ":".join(record_time) # [27,13,14]->27:13:14
        return new_date + " " + new_time # 输出1999-12-31 27：13：14
client = MongoClient("192.168.96.208", 27017)
"""
db = client['all-web-data']

raw_data_collection = db["raw_data"]  # 往初始数据中加入时间戳
for raw_data in raw_data_collection.find():
    # print(raw_data)
    # 把时间格式从121314转成12:13:14
    raw_data_time = raw_data["time"]
    raw_data_time = (":").join([raw_data_time[0:2], raw_data_time[2:4], raw_data_time[4:6]])
    # 把时间和日期合并，把格式转换成2018-10-13 12:13:14
    formatted_time = raw_data["date"] + " " + raw_data_time
    hour_of_day = int(formatted_time[11:13])
    if(int(formatted_time[11:13]) <= 23):
            # 没有在24点-28点之间，可以直接转成时间戳
            timestamp = time.mktime(datetime.datetime.strptime(formatted_time, "%Y-%m-%d %H:%M:%S").timetuple())
            # print("变换后的时间戳：" + str(timestamp))
    else:
        # 24点-28点之间，先转成24点再转成时间戳
        print(formatted_time," 用了傻逼的30小时时间制，需要转换")
        converted_time = convert30Hto24H(formatted_time)
        print("转换后时间: ", converted_time)
        timestamp = time.mktime(converted_time.timetuple())
        print("\n")
    raw_data_collection.update(
        {"_id": raw_data["_id"]},
        {"$set": {"timestamp": timestamp}})
"""
"""
# CM部分
db = client["all-cm-data"]
raw_data_collection = db["raw_data"]
pbar = ProgressBar().start()
for raw_data in raw_data_collection.find():
    # print(raw_data)
    # 把时间格式从121314转成12:13:14
    raw_data_start_time = raw_data["start_time"].zfill(6)
    # 把时间和日期合并，把格式转换成2018-10-13 12:13:14
    formatted_date = "20" + raw_data["date"]
    formatted_time = formatted_date + " " + (":").join([raw_data_start_time[0:2], raw_data_start_time[2:4], raw_data_start_time[4:6]])
    hour_of_day = int(formatted_time[11:13])
    if(int(formatted_time[11:13]) <= 23):
            # 没有在24点-28点之间，可以直接转成时间戳
            timestamp = time.mktime(datetime.datetime.strptime(formatted_time, "%Y-%m-%d %H:%M:%S").timetuple())
            # print("变换后的时间戳：" + str(timestamp))
    else:
        # 24点-28点之间，先转成24点再转成时间戳
        print(formatted_time," 用了傻逼的30小时时间制，需要转换")
        converted_time = convert30Hto24H(formatted_time)
        print("转换后时间: ", converted_time)
        timestamp = time.mktime(converted_time.timetuple())
        print("\n")
    raw_data_collection.update(
        {"_id": raw_data["_id"]},
        {"$set": {"timestamp": timestamp,
                  "start_time": raw_data_start_time,
                  "date": formatted_date}})
pbar.stop()
"""
# TV接触数据
def add_timestamp_to_tv_orgn_data(start_point, end_point):
    print("第{}进程正在工作".format(mp.current_process()))
    print("从{}出发到{}条数据".format(start_point, end_point))
    client = MongoClient("192.168.96.208", 27017)
    db = client["all-tv-orgn-data"]
    raw_data_collection = db["raw_data"]

    document_count = raw_data_collection.count()
    index = 1
    for raw_data in raw_data_collection.find():
        if start_point <= index < end_point:
            print("Process No.{}正在处理第{}个，共有{}个".format(mp.current_process, index, document_count))
            index += 1
            # 把时间格式从1834转换为183400
            raw_data_start_time = raw_data["start_time"].zfill(4) + "00"
            raw_data_end_time = raw_data["end_time"].zfill(4) + "00"
            # 把时间和日期合并
            formatted_start_time = raw_data["date"] + " " + (":").join([raw_data_start_time[0:2], raw_data_start_time[2:4], raw_data_start_time[4:6]])
            formatted_end_time = raw_data["date"] + " " + (":").join([raw_data_end_time[0:2], raw_data_end_time[2:4], raw_data_end_time[4:6]])
            if(int(formatted_start_time[11:13]) <= 23 and int(formatted_end_time[11:13]) <=23):
                # 开始时间和结束时间都是0-23点
                start_timestamp = time.mktime(datetime.datetime.strptime(formatted_start_time, "%Y-%m-%d %H:%M:%S").timetuple())
                end_timestamp = time.mktime(datetime.datetime.strptime(formatted_end_time, "%Y-%m-%d %H:%M:%S").timetuple())
            elif(int(formatted_start_time[11:13]) > 23 and int(formatted_end_time[11:13]) <= 23):
                # 开始时间是24-27点，结束时间是0-23点
                formatted_start_time = str(convert30Hto24H(formatted_start_time))
                start_timestamp = time.mktime(datetime.datetime.strptime(formatted_start_time, "%Y-%m-%d %H:%M:%S").timetuple())
                end_timestamp = time.mktime(datetime.datetime.strptime(formatted_end_time, "%Y-%m-%d %H:%M:%S").timetuple())
            elif(int(formatted_start_time[11:13]) <=23 and int(formatted_end_time[11:13]) > 23):
                formatted_end_time = str(convert30Hto24H((formatted_end_time)))
                start_timestamp = time.mktime(datetime.datetime.strptime(formatted_start_time, "%Y-%m-%d %H:%M:%S").timetuple())
                end_timestamp = time.mktime(datetime.datetime.strptime(formatted_end_time, "%Y-%m-%d %H:%M:%S").timetuple())
            else:
                #开始时间和结束时间都是24-27点
                formatted_start_time = str(convert30Hto24H(formatted_start_time))
                formatted_end_time = str(convert30Hto24H(formatted_end_time))
                start_timestamp = time.mktime(datetime.datetime.strptime(formatted_start_time, "%Y-%m-%d %H:%M:%S").timetuple())
                end_timestamp = time.mktime(datetime.datetime.strptime(formatted_end_time, "%Y-%m-%d %H:%M:%S").timetuple())
            last_time = end_timestamp - start_timestamp
            raw_data_collection.update(
                {"_id": raw_data["_id"]},
                {"$set": {"start_timestamp": start_timestamp,
                          "end_timestamp": end_timestamp,
                          "last_time": last_time}})


db = client["all-tv-orgn-data"]
raw_data_collection = db["raw_data"]

document_count = raw_data_collection.count()
chunck_size = int(document_count / 8)
if __name__ == "__main__":
    pool = mp.Pool(processes=8)
    for i in range(8):
        pool.apply_async(add_timestamp_to_tv_orgn_data, args=(i*chunck_size, (i+1)*chunck_size))
    pool.close()
    pool.join()
    # matchingCMAndUser(0, chunck_size)
    # add_timestamp_to_tv_orgn_data(0, document_count)