# coding: utf-8
# 本模块按照需求寻找数据
from DataAnalysisNTT.DBInteraction.DumpDataIntoDB import DataInteractModule
import progressbar, uuid
import copy
class DataSearchModule(DataInteractModule):
    def __init__(self):
        super(DataSearchModule, self).__init__()
        self.motorcar_brand_dict = {
            "81101554": "BMW_3Series",
            "81101410": "Mercedes_CClass",
            "81101631": "Toyota_HARRIER",
            "81101697": "Toyota_VITZ",
            "81101921": "Toyota_SIENTA",
            "81101E11": "Toyota_CHR",
            "81101B85": "Honda_FIT",
            "81101848": "Toyota_VOXY",
            "81101526": "Mazda_DEMIO",
            "81101907": "Honda_STEPWGN",
            "81102169": "Honda_NBOX",
            "81101797": "Nissan_XTRAIL",
            "81101968": "Nissan_NOTE",
            "81102180": "Nissan_DAYZ",
            "81102188": "Nissan_DAYZ",
            "81101392": "Subaru_IMPREZA",
            "81102123": "Daihatsu_Tanto",
            "81102207": "Daihatsu_MOVE",
            "81102178": "Suzuki_SPACIA",
            "81102184": "Suzuki_SPACIA",
            "81101755": "Suzuki_SWIFT",
            "81102058":"Suzuki_WGANR"
            }
        self.motorcar_brand_name_list = self.findBrandName()
        self.motorcar_brand_code_list = self.findBrnadCode()
        self.household_number_list = []
        self.household_number_Processed_list = []
        self.brand_without_webdata = {}
        self.brand_with_webdata = {}
        self.function_explanation = {
            "findMotorcarWebData": "寻找确认汽车品牌关键字相关的网络浏览数据",
            "findMotorCarAdWatchedDataByBrand": "寻找匹配汽车品牌广告接触数据",
            "": ""
        }

    def findBrnadCode(self):
        result = []
        for key, value in self.motorcar_brand_dict.items():
            if key not in result:
                result.append(key)
        return result

    def findBrandName(self):
        result = []
        for key, value in self.motorcar_brand_dict.items():
            if value not in result:
                result.append(value)
        return result

    @staticmethod
    def reverseDict(original_dict):
        #  把字典的key和value反转
        return dict((v, k) for k, v in original_dict.items())

    @staticmethod
    def mergeCollectionsIntoOne(db, merged_collection_name):
        print("Merge")
        for collection_name in db.collection_names():
            for single_document in db[collection_name].find():
                single_document["_id"] = uuid.uuid1()
                db[merged_collection_name].insert_one(single_document)

    def findProcessedHousehold(self):
        result = []
        for collection_name in self.client[self.db_name].collection_names():
            if collection_name.split("_")[0] not in result:
                result.append(collection_name.split("_")[0])
        return result

    def functionExplainer(self, function_name):
        # 在执行函数之前解释是干啥用的
        print("即将执行{}，用于{}".format(function_name, self.function_explanation[function_name]))

    @staticmethod
    def removeZero(string):
        index = 0
        for index in range(len(string)):
            if (string[index] != "0"):
                break
        return string[index:]

    def findMotorcarWebData(self):
        #  按照关键词查找自动车相关网络数据
        self.functionExplainer("findMotorcarWebData")
        # 输入相关信息
        # self.userInteract(has_input_file=False)
        self.userInteract(description_of_db="输入要导出到的数据库名称(若空白则默认为all-web-data)",
                          description_of_collection="输入要导出到的collection名称(若空白则默认为divided_data)",
                          has_input_file=False)
        # self.db_name = str(input("输入要导出到的数据库名称(若空白则默认为all-web-data)："))
        # self.collection_name = str(input("输入要导出到的collection名称(若空白则默认为divided_data)："))
        if len(self.db_name) == 0:
            # 默认数据库名
            self.db_name = "all-web-data"

        if len(self.collection_name) == 0:
            self.collection_name = "divided_data"

        if self.dbNameCheckPassed() and self.collectionNameCheckPassed():
            original_data = self.client[self.db_name][self.collection_name]
            print(original_data)

            # 改变数据库名称
            # self.userInteract(has_input_file=False)
            self.userInteract(description_of_db="输入要导出到的数据库名称(若空白则默认为all_motorcar_web_data)",
                              description_of_collection="输入要导出到的collection名称(若空白则默认为all_data)",
                              has_input_file=False)
            # self.db_name = str(input("输入要导出到的数据库名称(若空白则默认为motorcar_all_web_data)："))
            # self.collection_name = str(input("输入要导出到的collection名称(若空白则默认为all_data)："))
            if len(self.db_name) == 0:
                # 默认数据库名
                self.db_name = "all_motorcar_web_data"

            if len(self.collection_name) == 0:
                self.collection_name = "all_data"

            if self.dbNameCheckPassed() and self.collectionNameCheckPassed():
                # 进度条
                # bar1 = progressbar.ProgressBar(max_value=len(list(original_data.find())))
                index = 0

                motorcar_web_data = self.client[self.db_name]
                print(motorcar_web_data)
                for single_document in original_data.find():
                    # bar1.update(index)
                    index += 1
                    if "BMW3" in single_document["title"] or "BMW3シリーズ" in single_document["title"] or "BMW3車" in \
                            single_document["title"] or "BMW3" == single_document["title"]:
                        print("匹配到搜索！")
                        motorcar_web_data["BMW_3Series"].insert_one(single_document)
                    if "ベンツC" in single_document["title"] or "Cクラス" in single_document["title"] or "ベンツCクラス" in \
                            single_document["title"]:
                        print("匹配到搜索！")
                        motorcar_web_data["Mercedes_CClass"].insert_one(single_document)
                    if "ハリアー車" in single_document["title"] or "ハリアートヨタ" in single_document["title"] or "トヨタハリアー" in \
                            single_document["title"]:
                        print("匹配到搜索！")
                        motorcar_web_data["Toyota_HARRIER"].insert_one(single_document)
                    if "トヨタヴィッツ" in single_document["title"] or "ヴィッツ" in single_document["title"] or "トヨタVITZ" in \
                            single_document["title"]:
                        print("匹配到搜索！")
                        motorcar_web_data["Toyota_VITZ"].insert_one(single_document)
                    if "トヨタシエンタ" in single_document["title"] or "シエンタ車" in single_document["title"] or "シエンタトヨタ" in \
                            single_document["title"]:
                        print("匹配到搜索！")
                        motorcar_web_data["Toyota_SIENTA"].insert_one(single_document)
                    if "トヨタCHR" in single_document["title"] or "トヨタC-HR" in single_document["title"] or "C-HRトヨタ" in \
                            single_document["title"] or "CHRトヨタ" in single_document["title"]:
                        print("匹配到搜索！")
                        motorcar_web_data["Toyota_CHR"].insert_one(single_document)
                    if "VOXY" == single_document["title"] or "ヴォクシー" == single_document["title"] or "ヴォクシートヨタ" in \
                            single_document["title"]:
                        print("匹配到搜索！")
                        motorcar_web_data["Toyota_VOXY"].insert_one(single_document)
                    if "ホンダフィット" in single_document["title"] or "ホンダFIT" in single_document["title"] or "フィットHonda" in \
                            single_document["title"]:
                        print("匹配到搜索！")
                        motorcar_web_data["Honda_FIT"].insert_one(single_document)
                    if "ホンダステップワゴン" in single_document["title"] or "ステップワゴン" in single_document[
                        "title"] or "ステップワゴンHonda" in single_document["title"]:
                        print("匹配到搜索！")
                        motorcar_web_data["Honda_STEPWGN"].insert_one(single_document)
                    if "ホンダNBOX" in single_document["title"] or "nbox車" in single_document["title"] or "N-BOXHonda" in \
                            single_document["title"] or "NBOXホンダ" in single_document["title"]:
                        print("匹配到搜索！")
                        motorcar_web_data["Honda_NBOX"].insert_one(single_document)
                    if "マツダデミオ" in single_document["title"] or "MAZDAデミオ" in single_document["title"] or "デミオマツダ" in \
                            single_document["title"]:
                        print("匹配到搜索！")
                        motorcar_web_data["Mazda_DEMIO"].insert_one(single_document)
                    if "日産エクストレイル" in single_document["title"] or "X-TRAIL" in single_document["title"] or "エクストレイル" in \
                            single_document["title"]:
                        print("匹配到搜索！")
                        motorcar_web_data["Nissan_XTRAIL"].insert_one(single_document)
                    if "日産DAYZ" in single_document["title"] or "日産デイズ" in single_document["title"] or "デイズ日産" in \
                            single_document["title"] or "DAYZ日産" in single_document["title"]:
                        print("匹配到搜索！")
                        motorcar_web_data["Nissan_DAYZ"].insert_one(single_document)
                    if "日産ノート" in single_document["title"] or "日産NOTE" in single_document["title"] or "日産note" in \
                            single_document["title"] or "ノート日産" in single_document["title"]:
                        print("匹配到搜索！")
                        motorcar_web_data["Nissan_NOTE"].insert_one(single_document)
                    if "スバルインプレッサ" in single_document["title"] or "インプレッサSUBARU" in single_document[
                        "title"] or "インプレッサスバル" in single_document["title"]:
                        print("匹配到搜索！")
                        motorcar_web_data["Subaru_IMPREZA"].insert_one(single_document)
                    if "ダイハツタント" in single_document["title"] or "タント車" in single_document["title"] or "タントダイハツ" in \
                            single_document["title"]:
                        print("匹配到搜索！")
                        motorcar_web_data["Daihatsu_Tanto"].insert_one(single_document)
                    if "ダイハツムーヴ" in single_document["title"] or " ムーヴ車" in single_document["title"] or "ダイハツムーヴ" in \
                            single_document["title"]:
                        print("匹配到搜索！")
                        motorcar_web_data["Daihatsu_MOVE"].insert_one(single_document)
                    if "スズキ" in single_document["title"] and "スペーシア" in single_document["title"]:
                        print("匹配到搜索！")
                        motorcar_web_data["Suzuki_SPACIA"].insert_one(single_document)
                    if "スズキ" in single_document["title"] and "スイフト" in single_document["title"]:
                        print("匹配到搜索！")
                        motorcar_web_data["Suzuki_SWIFT"].insert_one(single_document)
                    if "スズキ" in single_document["title"] and "ワゴン" in single_document["title"]:
                        print("匹配到搜索！")
                        motorcar_web_data["Suzuki_WGANR"].insert_one(single_document)

                # 把household number最前面的0去掉
                for brand_name in self.motorcar_brand_name_list:
                    for single_document in motorcar_web_data[brand_name].find():
                        motorcar_web_data[brand_name].update(
                            {"_id": single_document["_id"]},
                            {"$set": {"household_num": self.removeZero(single_document["household_num"])}}
                        )
                # MERGE COLLECTIONS
                self.mergeCollectionsIntoOne(motorcar_web_data,
                                             merged_collection_name="all_data")


    def findMotorCarAdWatchedDataByBrand(self):
        # 导入全部的电视接触数据
        self.userInteract(description_of_db="输入要导入的电视观看原始数据的数据库名称(若空白则默认为all-tv-orgn-data)：",
                          description_of_collection="输入要导入的collection名称(若空白则默认为divided_data)：",
                          has_input_file=False)
        # self.db_name = str(input("输入要导入的电视观看原始数据的数据库名称(若空白则默认为all-tv-orgn-data)："))
        # self.collection_name = str(input("输入要导入的collection名称(若空白则默认为divided_data)："))
        if len(self.db_name) == 0:
            # 默认数据库名
            self.db_name = "all-tv-orgn-data"

        if len(self.collection_name) == 0:
            self.collection_name = "divided_data"
        if self.dbNameCheckPassed() and self.collectionNameCheckPassed():
            all_tv_orgn_db = self.client[self.db_name]
            all_tv_orgn_data = self.client[self.db_name][self.collection_name]  # 半年的电视接触数据


        # 导入按汽车品牌分类的广告数据
        self.userInteract(description_of_db="输入要导入的按汽车品牌分类的数据库名称(若空白则默认为cm_by_brand_motorcar)：",
                          has_input_file=False,
                          has_collection=False)
        # self.db_name = str(input("输入要导入的按汽车品牌分类的数据库名称(若空白则默认为cm_by_brand_motorcar)："))
        if len(self.db_name) == 0:
            # 默认数据库名
            self.db_name = "cm_by_brand_motorcar"
        if self.dbNameCheckPassed():
            cm_by_brand = self.client[self.db_name]  # 汽车品牌别广告数据

        # 导入用户别的网络数据
        self.userInteract(description_of_db="输入要导入的按household number分类的网络数据的数据库名称(若空白则默认为all_motorcar_web_data_by_household)：",
                          has_input_file=False,
                          has_collection=False)
        # self.db_name = str(input("输入要导入的按household number分类的网络数据的数据库名称(若空白则默认为all_motorcar_web_data_by_household)："))
        if len(self.db_name) == 0:
            # 默认数据库名
            self.db_name = "all_motorcar_web_data_by_household"
        if self.dbNameCheckPassed():
            all_motorcar_web_data = self.client[self.db_name]

        # 导出到新的数据库
        self.userInteract(description_of_db="输入要导出的存在网络数据的广告接触数据的数据库名称(若空白则默认为motorcar_ad_match_by_household_num)：",
                          has_input_file=False,
                          has_collection=False)

        # self.db_name = str(input("输入要导入的按household number分类的网络数据的数据库名称(若空白则默认为motorcar_ad_match_by_household_num)："))
        if len(self.db_name) == 0:
            # 默认数据库
            self.db_name = "motorcar_ad_match_by_household_num"
        if self.dbNameCheckPassed():
            motorcar_ad_match_by_household_num = self.client[self.db_name]

        self.userInteract(description_of_db="输入要导出的不存在网络数据的广告接触数据的数据库名称(若空白则默认为motorcar_ad_match_by_household_num_without_webdata)：",
                          has_input_file=False,
                          has_collection=False)

        # self.db_name = str(input("输入要导入的按household number分类的网络数据的数据库名称(若空白则默认为motorcar_ad_match_by_household_num)："))
        if len(self.db_name) == 0:
            # 默认数据库
            self.db_name = "motorcar_ad_match_by_household_num_without_webdata"
        if self.dbNameCheckPassed():
            motorcar_ad_match_by_household_num_without_webdata = self.client[self.db_name]
        household_num_list = []
        for collection_name in all_motorcar_web_data.collection_names():
            if collection_name.split("_")[0] not in household_num_list:
                household_num_list.append(collection_name.split("_")[0])
        household_num_processed_list = []
        for collection_name in motorcar_ad_match_by_household_num.collection_names():
            # print(collection_name)
            # household_num = collection_name.split("_")
            if collection_name.split("_")[0] not in household_num_processed_list:
                household_num_processed_list.append(collection_name.split("_")[0])

        for collection_name in motorcar_ad_match_by_household_num_without_webdata.collection_names():
            # print(collection_name)
            # household_num = collection_name.split("_")
            if collection_name.split("_")[0] not in household_num_processed_list:
                household_num_processed_list.append(collection_name.split("_")[0])

        # 找到每个household number没有网络数据的品牌
        for collection_name in all_motorcar_web_data.collection_names():
            brand_name = "_".join(collection_name.split("_")[1:])
            try:
                self.brand_with_webdata[collection_name.split("_")[0]].append(brand_name)
            except:
                self.brand_with_webdata.update({collection_name.split("_")[0]:[brand_name]}) # fixed!
                # BUG! WILL LOST FIRST DATA: FIXED

        for household_num in household_num_list:
            brand_without_webdata = copy.deepcopy(self.motorcar_brand_name_list)  # Create a deep copy
            self.brand_without_webdata.update({household_num: brand_without_webdata})
            # print(household_num, self.brand_without_webdata[household_num])
            for brand_name in self.brand_with_webdata[household_num]:
                # print(brand_name, household_num)
                # print(self.brand_without_webdata[household_num])
                self.brand_without_webdata[household_num].remove(brand_name)

        # print(household_num_processed_list)
        # print(len(household_num_list), household_num_list)

        # 进度条
        bar2 = progressbar.ProgressBar(max_value=len(household_num_list))
        index = 0
        print(household_num_processed_list)
        for household_num in household_num_list:
            # print("Length of Brand with webdata{}\nLegth of Brand without webdata{}".format(len(self.brand_with_webdata[household_num]), len(self.brand_without_webdata[household_num])))
            # print(household_num.split("_")[0])
            bar2.update(index)
            index += 1
            global start_searching_timestamp #aki debug
            if household_num in household_num_processed_list:
                # print("该household number已被处理")
                continue
            else:
                for i in range(len(self.brand_with_webdata[household_num])):
                    print("Search With WebData")
                    # print(household_num)
                    print("With WebData ", household_num+"_"+self.brand_with_webdata[household_num][i])
                    start_searching_timestamp = all_motorcar_web_data[household_num+"_"+self.brand_with_webdata[household_num][i]].find_one()["timestamp"]

                    all_tv_orgn_data.aggregate([
                        {"$match": {"household_num": household_num,
                                    "end_timestamp": {"$lt": start_searching_timestamp}}},
                        {"$out": "matching_TV_data_tmp"}
                    ])
                    # print(type(start_searching_timestamp))
                    # print(household_num)
                    # print(all_tv_orgn_data.find())

                    # print(list(all_tv_orgn_db["matching_TV_data_tmp"].find()))
                    for single_tv_watch_data in all_tv_orgn_db["matching_TV_data_tmp"].find():
                        # print("_".join(household_num.split("_")[1:]))
                        # 用后面的品牌名去匹配
                        cm_by_brand[self.brand_with_webdata[household_num][i]].aggregate([
                            {"$match": {"timestamp": {"$gt": single_tv_watch_data["start_timestamp"],
                                                      "$lt": single_tv_watch_data["end_timestamp"]}}},
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
                            {"$out": "watchedCM_tmp_with_webdata"}
                        ])
                        if (len(list(cm_by_brand["watchedCM_tmp_with_webd664ata"].find())) != 0):
                            for single_document in cm_by_brand["watchedCM_tmp_with_webdata"].find():
                                tmp = single_document
                                tmp["_id"] = uuid.uuid1()
                                cm_by_brand["watchedCM_tmp_id_reset"].insert_one(tmp)
                            # 每个householdnumber 按照品牌的广告接触数据
                            motorcar_ad_match_by_household_num[household_num+"_"+self.brand_with_webdata[household_num][i]].insert_many(
                                list(cm_by_brand["watchedCM_tmp_id_reset"].find()))
                            cm_by_brand["watchedCM_tmp_id_reset"].drop()

                for i in range(len(self.brand_without_webdata[household_num])):
                    # print(household_num, self.brand_without_webdata[household_num])
                    # print("Search Without WebData")
                    print("No WebDta Search")
                    print("No WebData: ", household_num + "_" + self.brand_without_webdata[household_num][i])
                    all_tv_orgn_data.aggregate([
                        {"$match": {"household_num": household_num}},
                        {"$out": "matching_TV_data_tmp"}
                    ])
                    for single_tv_watch_data in all_tv_orgn_db["matching_TV_data_tmp"].find():
                        # print(single_tv_watch_data)
                        cm_by_brand[self.brand_without_webdata[household_num][i]].aggregate([
                            {"$match": {"timestamp": {"$gt": single_tv_watch_data["start_timestamp"],
                                                      "$lt": single_tv_watch_data["end_timestamp"]}}},
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
                            {"$out": "watchedCM_tmp_without_webdata"}
                        ])
                        # print(cm_by_brand["watchedCM_tmp"].find())
                        if (len(list(cm_by_brand["watchedCM_tmp_without_webdata"].find())) != 0):
                            for single_document in cm_by_brand["watchedCM_tmp_without_webdata"].find():
                                tmp = single_document
                                tmp["_id"] = uuid.uuid1()
                                cm_by_brand["watchedCM_tmp_id_reset"].insert_one(tmp)
                            motorcar_ad_match_by_household_num_without_webdata[household_num+"_"+self.brand_without_webdata[household_num][i]].insert_many(list(cm_by_brand["watchedCM_tmp_id_reset"].find()))
                            # print(cm_by_brand["watchedCM_tmp_id_reset"])
                            cm_by_brand["watchedCM_tmp_id_reset"].drop()

if __name__ == "__main__":
    data_search = DataSearchModule()
    # data_search.findMotorcarWebData()
    #print(DataSearchModule().findMotorcarWebData())
    data_search.findMotorCarAdWatchedDataByBrand()