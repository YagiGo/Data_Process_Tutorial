from pymongo import MongoClient
client = MongoClient("localhost", 27017)

db = client['all-web-data']

raw_data_collection = db["raw_data"]
index = 0
index_google_search = index_yahoo_search = index_rakuten_search = 0
for single_document in raw_data_collection.find():
    index += 1

    if single_document["url"].startswith("www.google.co.jp") and single_document["title"].endswith("Google 検索"):
        index_google_search += 1
        print("匹配到谷歌搜索！")
        print("搜索标题：{}".format(single_document["title"]))
    elif single_document["url"].startswith("www.rakuten.co.jp") and single_document["title"].endswith("検索"):
        index_rakuten_search += 1
        print("匹配到乐天搜索！")
        print("搜索标题：{}".format(single_document["title"]))
    elif single_document["url"].startswith("www.yahoo.co.jp") and single_document["title"].endswith("検索"):
        index_yahoo_search += 1
        print("匹配到雅虎搜索！")
        print("搜索标题：{}".format(single_document["title"]))

print("在{}条数据中共有{}条谷歌搜索结果\n{}条乐天搜索结果\n{}条雅虎搜索结果".format(raw_data_collection.count(),
                                                        index_google_search,index_rakuten_search,index_yahoo_search))