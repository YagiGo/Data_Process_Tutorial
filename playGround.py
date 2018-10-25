import datetime
import time
""""
timeExample_1 = "1999-12-31 27:49:23"
timeExample_2 = "2000-01-01 03:49:23"

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


print(convert30Hto24H(timeExample_1))
print(convert24HTo30H(timeExample_2))
"""
# timestamp_example = 1530937943
# print(time.localtime(timestamp_example))
import matplotlib.pyplot as plt
test_graph = [[0,1],[1,2]]
fig, ax = plt.subplots()  # 图表分成坐标轴和图两部分
im = ax.imshow(test_graph, interpolation="nearest", vmin=0)  # 用imshow画热点图
plt.colorbar(im)
plt.show()