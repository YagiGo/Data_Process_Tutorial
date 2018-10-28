# 把一个大数据分成几个小数据用于后面分散处理
from pymongo import MongoClient
client = MongoClient("localhost", 27017)
# TV data
tv_orgn_data_db = client["all-tv-orgn-data"]
all_tv_orgn_data = tv_orgn_data_db["raw_data"] # 把这个collection分成4个小份
sub_collection_00 = tv_orgn_data_db["sub_collection_00"] # 第1份
sub_collection_01 = tv_orgn_data_db["sub_collection_01"] # 第2份
sub_collection_02 = tv_orgn_data_db["sub_collection_02"] # 第3份
sub_collection_03 = tv_orgn_data_db["sub_collection_03"] # 第4份
tv_orgn_data_size = all_tv_orgn_data.estimated_document_count() # 统计总共有多少条数据
chunk_size = tv_orgn_data_size / 4 # 每份有多少条数据

index = 0
for single_document in all_tv_orgn_data.find():
    if 0 <= index < chunk_size:
        sub_collection_00.insert_one(single_document)
    elif chunk_size <= index < 2*chunk_size:
        sub_collection_01.insert_one(single_document)
    elif 2*chunk_size <= index < 3*chunk_size:
        sub_collection_02.insert_one(single_document)
    elif 3*chunk_size <= index < 4*chunk_size:
        sub_collection_03.insert_one(single_document)