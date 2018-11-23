# coding: utf-8
# 根据需求把数据分出来
import uuid
from DBInteraction.DumpDataIntoDB import DataInteractModule
from DataInteraction.SearchDataOnDemand import DataSearchModule
import progressbar

class DataDivisionModule(DataSearchModule):
    def __init__(self):
        super(DataDivisionModule, self).__init__()
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
        self.addFunctionExplaniation(function_name="divideMotorcarWebData",
                                     description="按汽车品牌网站阅览过的userid为基准划分并生成新的用户区分的汽车网络阅览数据")
        self.addFunctionExplaniation(function_name="divideMotorcarCMDataOnBrand",
                                     description="按照汽车品牌code分割广告数据")

    def addFunctionExplaniation(self, function_name, description):
        # 加入新的函数解释
        self.function_explanation[function_name] = description


    def divideMotorcarWebData(self):
        # 按汽车品牌网站阅览过的userid为基准划分并生成新的用户区分的汽车网络阅览数据
        # 输入被导入的数据库和collection名
        # 先从按关键字获取的网络数据的数据库中导入数据
        self.functionExplainer(function_name="divideMotorcarWebData")
        self.userInteract(has_input_file=False,
                          description_of_db="输入要导入的数据库名称(若空白则默认为all_motorcar_web_data)：",
                          description_of_collection="输入要导入的collection名称(若空白则默认为all_data)：")
        # self.db_name = str(input("输入要导入的数据库名称(若空白则默认为motorcar_all_web_data)："))
        # self.collection_name = str(input("输入要导入的collection名称(若空白则默认为all_data)："))
        if len(self.db_name) == 0:
            # 默认数据库名
            self.db_name = "all_motorcar_web_data"

        if len(self.collection_name) == 0:
            print("默认collection名")
            self.collection_name = "all_data"

        if self.dbNameCheckPassed() and self.collectionNameCheckPassed():

            all_motorcar_web_data = self.client[self.db_name]
            # 输入导出的数据库名
            self.userInteract(has_input_file=False,
                              has_collection=False,
                              description_of_db="输入要导入的数据库名称(若空白则默认为all_motorcar_web_data_by_household)：")
            # self.db_name = str(input("输入要导出的数据库名称(若空白则默认为all_motorcar_web_data_by_household)："))
            if len(self.db_name) == 0:
                self.db_name = "all_motorcar_web_data_by_household"
            if self.dbNameCheckPassed():
                # 导出的数据库
                all_motorcar_web_data_by_household = self.client[self.db_name]
                # 找到全部的household number
                print(self.collection_name)
                for single_document in all_motorcar_web_data[self.collection_name].find():
                    print(single_document)
                    if single_document["household_num"] not in self.household_number_list:
                        self.household_number_list.append(single_document["household_num"])
                # print(self.household_number_list)
                bar1 = progressbar.ProgressBar(max_value=len(self.household_number_list))
                index = 0
                for household_num in self.household_number_list:
                    bar1.update(index)
                    index += 1
                    for brand_name in self.motorcar_brand_name_list:
                        all_motorcar_web_data[brand_name].aggregate([
                            {"$match": {"household_num": household_num}},
                            {"$addFields": {"brand_name": brand_name}},
                            {"$sort": {"timestamp": 1}},
                            {"$out": "tmp"}
                        ])
                        collection_name_with_hn_brand = household_num + "_" + brand_name
                        if (len(list(all_motorcar_web_data["tmp"].find()))):
                            all_motorcar_web_data_by_household[collection_name_with_hn_brand].insert_many(
                                list(all_motorcar_web_data["tmp"].find()))

    def divideMotorcarCMDataOnBrand(self):
        self.functionExplainer(function_name="divideMotorcarCMDataOnBrand")
        # 导入全部的广告数据
        self.userInteract(has_input_file=False,
                          description_of_db="输入要导入的数据库名称(若空白则默认为all-cm-data)：",
                          description_of_collection="输入要导入的广告数据库的collection名(若空白默认为raw_data):")
        # self.db_name = str(input("输入要导入的广告数据库名称(若空白则默认为all-cm-data)："))
        # self.collection_name = str(input("输入要导入的广告数据库的collection名(若空白默认为divided_data):"))
        if len(self.db_name) == 0:
            self.db_name = "all-cm-data"
        if len(self.collection_name) == 0:
            self.collection_name = "raw_data"

        if self.dbNameCheckPassed() and self.collectionNameCheckPassed():
            cm_db = self.client[self.db_name]
            cm_all_data = self.client[self.db_name][self.collection_name]

        # 输出按汽车品牌分类的广告的数据库命名
        self.userInteract(has_input_file=False,
                          has_collection=False,
                          description_of_db="输入要导出的广告数据库名称(若空白则默认为cm_by_brand_motorcar)：")
        # self.db_name = str(input("输入要导出的广告数据库名称(若空白则默认为cm_by_brand_motorcar)："))
        if len(self.db_name) == 0:
            self.db_name = "cm_by_brand_motorcar"
        if self.dbNameCheckPassed():
            cm_by_brand = self.client[self.db_name]

        # 进度条
        bar2 = progressbar.ProgressBar(max_value=len(self.motorcar_brand_code_list))
        index = 1
        for key, value in self.motorcar_brand_dict.items():
            bar2.update(index)
            cm_all_data.aggregate([
                {"$match": {"brand_code": key}},
                {"$out": "tmp"}
            ])

            if (len(list(cm_db["tmp"].find())) != 0):
                # print("INSERT CM OF ", value)
                print(value)
                cm_by_brand[value].insert_many(list(cm_db["tmp"].find()))
            index += 1



if __name__ == "__main__":
    test = DataDivisionModule()
    # test.divideMotorcarCMDataOnBrand()
    test.divideMotorcarWebData()
