from pymongo import MongoClient
import numpy as np
import time
import matplotlib
import matplotlib.pyplot as plt

# sphinx_gallery_thumbnail_number = 2
client = MongoClient("localhost", 27017)
db = client['all-web-data']
raw_data_collection = db["raw_data"]


def count_access_time(dbCollection):
    # 创建一个24*31的矩阵，计算十月份的每一天的每一个小时的访问总量，用来画热点图
    access_time_counter = np.zeros((24, 31))
    for access_history in dbCollection.find():
        formatted_time = time.localtime(access_history["timestamp"])
        access_time_counter[formatted_time.tm_hour - 1, formatted_time.tm_mday - 1] += 1
        min_value, max_value = access_time_counter.min(), access_time_counter.max()
    return (access_time_counter - min_value) / (max_value - min_value)  # 矩阵归一化


# print(count_access_time(raw_data_collection))

def generate_heatmap(access_date, access_time, db_collection):
    access_time_counter = count_access_time(db_collection)
    print(access_time_counter)
    fig, ax = plt.subplots()  # 图表分成坐标轴和图两部分
    ax.imshow(access_time_counter, interpolation="nearest", vmin=0)  # 用imshow画热点图
    ax.set_xticks(np.arange(len(access_date)))  # 设置x轴的范围，这里是日期长度，31天
    ax.set_yticks(np.arange(len(access_time)))  # 设置y轴的范围，这里是一天的长度，24小时
    ax.set_xticklabels(access_date)  # x轴上加标签
    ax.set_yticklabels(access_time)  # y轴上加标签
    # Rotate the tick labels and set their alignment.
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
             rotation_mode="anchor")  # x轴标签旋转45度防止重叠
    ax.set_title("Internet Access Rush Hour Heatmap")  # 设置图标标题
    for edge, spine in ax.spines.items():
        spine.set_visible(False)  # 去除网格，改为True可以增加网格显示
    plt.show()


access_date = ["Day1", "Day2", "Day3", "Day4", "Day5", "Day6", "Day7", "Day8", "Day9", "Day10",
               "Day11", "Day12", "Day13", "Day14", "Day15", "Day16", "Day17", "Day18", "Day19", "Day20",
               "Day21", "Day22", "Day23", "Day24", "Day25", "Day26", "Day27", "Day28", "Day29", "Day30",
               "Day31"]
access_time = ["2400", "2500", "2600", "2700", "0400", "0500", "0600", "0700", "0800", "0900", "1000",
               "1100", "1200", "1300", "1400", "1500", "1600", "1700", "1800", "1900", "2000", "2100", "2200", "2300"]

generate_heatmap(access_date, access_time, raw_data_collection)

"""
date = ["Jan", "F", "lettuce", "asparagus",
              "potato", "wheat", "barley"]
time = ["Farmer Joe", "Upland Bros.", "Smith Gardening",
           "Agrifun", "Organiculture", "BioGoods Ltd.", "Cornylee Corp."]

harvest = np.array([[0.8, 2.4, 2.5, 3.9, 0.0, 4.0, 0.0],
                    [2.4, 0.0, 4.0, 1.0, 2.7, 0.0, 0.0],
                    [1.1, 2.4, 0.8, 4.3, 1.9, 4.4, 0.0],
                    [0.6, 0.0, 0.3, 0.0, 3.1, 0.0, 0.0],
                    [0.7, 1.7, 0.6, 2.6, 2.2, 6.2, 0.0],
                    [1.3, 1.2, 0.0, 0.0, 0.0, 3.2, 5.1],
                    [0.1, 2.0, 0.0, 1.4, 0.0, 1.9, 6.3]])


fig, ax = plt.subplots()
im = ax.imshow(harvest)

# We want to show all ticks...
ax.set_xticks(np.arange(len(farmers)))
ax.set_yticks(np.arange(len(vegetables)))
# ... and label them with the respective list entries
ax.set_xticklabels(farmers)
ax.set_yticklabels(vegetables)

# Rotate the tick labels and set their alignment.
plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
         rotation_mode="anchor")

# Loop over data dimensions and create text annotations.
for i in range(len(vegetables)):
    for j in range(len(farmers)):
        text = ax.text(j, i, harvest[i, j],
                       ha="center", va="center", color="w")

ax.set_title("Harvest of local farmers (in tons/year)")
fig.tight_layout()
plt.show()
"""
