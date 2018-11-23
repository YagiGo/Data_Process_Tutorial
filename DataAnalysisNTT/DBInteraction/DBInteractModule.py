from pymongo import MongoClient

class DBInteractModule:
    def __init__(self, DB_ADDRESS="192.168.1.4", DB_PORT=27017):
        self.DB_ADDR = DB_ADDRESS
        self.DB_PORT = DB_PORT
        self.client = MongoClient(self.DB_ADDR, self.DB_PORT)