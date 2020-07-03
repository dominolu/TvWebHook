# -*- coding:utf-8 -*-
"""
@Time : 2020/4/8 9:21 下午
@Author : Domionlu
@Site : 
@File : config.py
"""
import json



class Config(dict):
    def __init__(self, argument):
        # 如果输入的是字符串，把它当做文件路径，加载文件内容
        if isinstance(argument, str):
            with open(file = argument, mode = 'r') as file:
                argument = json.loads(file.read())

        # 遍历字典，给自身赋值
        for key, value in argument.items():
            # 如果某个值是 dict 类型，递归调用自身构造函数
            function = Config if isinstance(value, dict) else lambda x: x
            setattr(self, key, function(value))
            self[key] = value


config = Config("config.json")

if __name__ == "__main__":
    print(f"host:{config.rabbitmq.host}")