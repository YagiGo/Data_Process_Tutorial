# coding: utf-8
# 测试用
import os, sys

def pathCheckTest():
    dir_path = os.path.dirname(os.path.realpath(__file__)) # 定位到当前文件的根目录
    input_file_path = os.path.join(dir_path, "..", "InputCSVFiles")
    print(dir_path)
    print(input_file_path)
    print(os.path.exists(os.path.join(input_file_path, "test.csv")))

def inputCorrect():
    input_file_name = str(input("CSV File Name:"))
    while not input_file_name.endswith(".csv"):
        print("Must Input CSV format of File!")
        input_file_name = str(input("CSV File Name:"))

def splitTest():
    print("12313".split("_"))
    print("1231_w342_234".split("_"))

splitTest()