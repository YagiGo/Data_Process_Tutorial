# coding: utf-8
# 把csv文件导入数据库
# 默认数据库位置localhost 默认端口27017
import pandas as pd
from pymongo import MongoClient
from DBInteraction.DBInteractModule import DBInteractModule  # DB交互模块
import os, sys, csv, json

class CSVData:
    def __init__(self, csv_file_path = None, column_name_added = False):
        self.csv_file_path = csv_file_path
        self.column_name_added = column_name_added



class DataInteractModule:

    def __init__(self):

        # self.DB_ADDR = DB_ADDRESS
        # self.DB_PORT = DB_PORT
        self.client = DBInteractModule().client
        self.input_file_name = None
        self.db_name = None
        self.collection_name = None
        self.csv_file_info = CSVData() # 收纳csv文件相关信息，有没有加名字

    def userInteract(self, description_of_db=None,description_of_collection=None, has_input_file = True, has_db_name = True, has_collection = True,):
        #T重写用户交互部分
        # 输入变量（文件名，数据库名称，collection名称）
        if has_input_file:
            # 需要输入文件名的时候
            self.input_file_name = str(input("输入要导入数据库的csv文件（example.csv）："))
        if has_db_name:
            # 需要输入数据库名的时候
            self.db_name = str(input(description_of_db))
        if has_collection:
            # 需要输入collection名的时候
            self.collection_name = str(input(description_of_collection))
    def pathCheckPassed(self):
        # 检查输入的文件是否存在
        dir_path = os.path.dirname(os.path.realpath(__file__))  # 定位到当前文件的根目录
        input_file_path = os.path.join(dir_path, "..", "InputCSVFiles") # InputCSVFiles下放csv文件
        # print(dir_path)
        # print(input_file_path)
        while not self.input_file_name.endswith(".csv"):
            print("必须输入csv格式的文件，请重试！")
            self.input_file_name = str(input("输入要导入数据库的csv文件（example.csv）："))
        while not os.path.exists(os.path.join(input_file_path, self.input_file_name)):
            print("指定的文件{}不存在，请重新输入！".format(self.input_file_name))
            self.input_file_name = str(input("输入要导入数据库的csv文件（example.csv）："))
        self.input_file_name = os.path.join(input_file_path, self.input_file_name)
        return True
    def dbNameCheckPassed(self):
        # 检查输入的数据库名是否正确
        while self.db_name not in self.client.list_database_names():
            create = str(input(("指定的数据库{}不存在，是否创建？（y/n）:").format(self.db_name)))
            if(create == "y"):
                print("CREATED")
                # 创建该collection
                return True
            else:
                self.db_name = str(input(("请检查数据库名称后重新输入：")))
        return True
    def collectionNameCheckPassed(self):
        # 检查输入的collection_name是否正确
        while self.collection_name not in self.client[self.db_name].collection_names():
            create = str(input(("指定的collection不存在，是否创建？（y/n）:").format(self.collection_name)))
            if(create == "y"):
                print("CREATED")
                # 创建该collection
                return True
            else:
                self.collection_name = str(input(("请检查collection名称后重新输入：")))
        return True

    def addingColoumNames(self):
        # 在csv文件中加入列名称
        print(self.input_file_name)
        df = pd.read_csv(self.input_file_name, header=None, encoding="utf-8")

        # 改这里的coloum名！！！！
        """
        Example:
        df.rename(columns={0: 'advertiser_code', 1: 'advertiser_name(kana)', 2: 'advertiser_name(jap)',
                           3: 'first_verision_date', 4: 'final_version_date'}, inplace=True)
        """
        df.rename(columns={0: 'advertiser_code', 1: 'advertiser_name(kana)', 2: 'advertiser_name(jap)',
                           3: 'first_verision_date', 4: 'final_version_date'}, inplace=True)
        df.to_csv(self.input_file_name, index=False)  # save to new csv file
        # self.csv_file_info.csv_file_path = self.csv_file_info
        # self.csv_file_info.column_name_added = True

    def confirm(self):
        # 最终确认
        print("\n此操作将会将{}存入{}数据库的{} collection中".format(self.input_file_name, self.db_name, self.collection_name))
        confirm = str(input("是否确认(y/n）"))
        if confirm == "y":
            return True
        else:
            return False
    def importData(self):
        # 导入数据库
        self.userInteract(description_of_db="输入要导入的数据库",
                          description_of_collection="输入要导入的collection名称")
        if self.pathCheckPassed() and self.dbNameCheckPassed() and self.collectionNameCheckPassed():
            column_name_added = str(input("csv文件中是否已经加入列名称（y/n)"))
            if column_name_added == "n":
                self.addingColoumNames()  # 加入column名
            # 然后开始导入数据库
            csv.field_size_limit(sys.maxsize)
            # client = MongoClient("localhost", 27017)
            # db = client['all-cm-data']
            # 创建一个文件放初始数据
            raw_data_collection = self.client[self.db_name][self.collection_name]
            result = []
            # 用CSV读csv
            with open(self.input_file_name, encoding="utf-8") as csv_file:
                for row in csv.DictReader(csv_file):
                    result.append(row)
            json_data = json.loads(json.dumps(result))
            # print(json_data)
            if self.confirm():
                delete_existing_collection = str(input("是否删除已经存在的数据？(y/n):"))
                if(delete_existing_collection == "y"):
                    raw_data_collection.remove()  # 先删除所有数据
                print("开始载入数据。。。")
                raw_data_collection.insert(json_data)  # 加载数据进数据库q
                print("数据载入完成")
            else:
                print("终止插入数据，退出")
                return


if __name__ == "__main__":
    DataDump = DataInteractModule()
    # DataDump.userInteract()
    # DataDump.collectionNameCheckPassed()
    DataDump.importData()
