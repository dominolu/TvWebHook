# -*- coding:utf-8 -*-
"""
@Time : 2020/5/3 12:14 上午
@Author : Domionlu
@Site :
@File : MongoDataSerer.py
"""
from  pymongo import MongoClient
from config import *
import pandas as pd

class Database(object):
    def __init__(self, address, port, database):
        self.conn = MongoClient(host=address, port=port)
        self.db = self.conn[database]
        # self.db.authenticate("admin","123456")

    def get_state(self):
        return self.conn is not None and self.db is not None

    def insert_one(self, collection, data):
        if self.get_state():
            ret = self.db[collection].insert_one(data)
            return ret.inserted_id
        else:
            return ""

    def insert_many(self, collection, data):
        if self.get_state():
            ret = self.db[collection].insert_many(data)
            return ret.inserted_id
        else:
            return ""

    def update(self, collection, data):
        # data format:
        # {key:[old_data,new_data]}
        data_filter = {}
        data_revised = {}
        for key in data.keys():
            data_filter[key] = data[key][0]
            data_revised[key] = data[key][1]
        if self.get_state():
            return self.db[collection].update_many(data_filter, {"$set": data_revised}).modified_count
        return 0

    def find(self, col, condition, column=None):
        if self.get_state():
            if column is None:
                return self.db[col].find(condition)
            else:
                return self.db[col].find(condition, column)
        else:
            return None

    def delete(self, col, condition):
        if self.get_state():
            return self.db[col].delete_many(filter=condition).deleted_count
        return 0

db = Database(config.mongo.host, config.mongo.port, config.mongo.database)

def get_spreed():
    data=db.db["spreed"].find().sort([("_id",-1)]).limit(10000)
    data = pd.DataFrame(list(data), columns=["timestamp", "makerprice", "takerprice"])
    data['time'] = pd.to_datetime(data['timestamp'], unit='ms', origin=pd.Timestamp('1970-01-01 08:00:00'))
    data = data.set_index('time')
    data = data.sort_index()
    return data

def get_foreprice():
    data = db.db["forceOrder"].find().sort([("_id", -1)]).limit(10000)
    data=pd.DataFrame(list(data))
    data.to_csv("forceorder.csv")

def dbtest():
    for i in range(10):
        msg= {"symbol": "BTCUSDT","timestamp": i}
        db.insert_one("spreed",msg)
if __name__ == '__main__':
    dbtest()