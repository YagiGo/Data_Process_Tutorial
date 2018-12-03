from pymongo import MongoClient
import progressbar


client = MongoClient("localhost", 27017)
web_data_100 = client["test-100-web-data"]

cm_user_match_data_100 = client["cm_user_match"]
bar1 = progressbar.ProgressBar(max_value=len(web_data_100.collection_names()))

print("开始合并网络数据 {}人".format(len(web_data_100.collection_names())))
index = 0
for collection_name in web_data_100.collection_names():
    bar1.update(index)
    web_data_100["all_data"].insert_many(list(web_data_100[collection_name].find()))
    index += 1

print("开始合并CM数据 {}人".format(len(cm_user_match_data_100.collection_names())))
index = 0
bar2 = progressbar.ProgressBar(max_value=len(cm_user_match_data_100.collection_names()))
for collection_name in cm_user_match_data_100.collection_names():
    bar2.update(index)
    cm_user_match_data_100["all_data"].insert_many(list(cm_user_match_data_100[collection_name].find()))
    index += 1
