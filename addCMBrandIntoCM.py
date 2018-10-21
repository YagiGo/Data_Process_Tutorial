from pymongo import MongoClient
client = MongoClient("localhost", 27017)

raw_cm_brand_data_collection = client["all-cm-brand"]["raw_data"]
raw_cm_data_collection = client["all-cm-data"]["raw_data"]

index = 1
cm_brand_size = raw_cm_brand_data_collection.count()

for cm_brand_code in raw_cm_brand_data_collection.find():
    print("对第{}个商标进行匹配，共有{}个商标".format(index, cm_brand_size))
    index += 1
    advertiser_code = cm_brand_code["advertiser_code"]
    advertiser_name_jap = cm_brand_code["advertiser_name(jap)"]
    advertiser_name_kana = cm_brand_code["advertiser_name(kana)"]
    raw_cm_data_collection.update_many(
        {"advertiser_code": advertiser_code},
        {"$set": {"advertiser_name(jap)": advertiser_name_jap,
                  "advertiser_name(kana)": advertiser_name_kana}})


