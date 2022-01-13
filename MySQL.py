# -*- coding: utf-8 -*-
"""
Created on Fri Jan 10 18:39:37 2020

@author: beibe
"""
import pymysql
import pandas as pd

class MySQL:   
    def __init__(self,server,user,password,database):
        # 类的构造函数，初始化DBC连接信息
        self.server = server
        self.user = user
        self.password = password
        self.database = database
        self.__GetConnect()

    def __GetConnect(self):
        # 得到数据库连接信息，返回conn.cursor()
        if not self.database:
            raise(NameError,"没有设置数据库信息")
        self.conn = pymysql.connect(host =self.server, user =self.user, password=self.password, db=self.database,charset = 'utf8')
        self.cur = self.conn.cursor()
        if not self.cur:
            raise(NameError,"连接数据库失败")  # 将DBC信息赋值给cur
        print("连接数据库成功")
    
    def ExecQuery(self,db_code,returnif=True):
        #执行查询语句,返回一个包含tuple的list，list是元素的记录行，tuple记录每行的字段数值
        self.cur.execute(db_code) # 执行查询语句
        if returnif==True:
            result=self.cur.fetchall()# fetchall()获取查询结果
            result=[list(x) for x in result]
            columnDes = self.cur.description  # 获取连接对象的描述信息
            columnNames = [x[0] for x in columnDes]
            result=pd.DataFrame(result,columns=columnNames)
            return result
        else:
            self.conn.commit()

    def closesql(self):
        self.conn.close()
        print("关闭数据库成功")