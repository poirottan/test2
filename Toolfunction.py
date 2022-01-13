# -*- coding: utf-8 -*-
"""
Created on Wed Nov 18 13:45:39 2020

@author: Admin
"""
#常用
import pandas as pd
import numpy as np

#step 0 准备函数
def to_csv_self(data,pathx,savecol=True,big=False,chunksize = 100000,date_format='%Y-%m-%d'):
    if big==False:
        data.to_csv(pathx+'.csv',index=savecol,encoding='utf-8_sig',date_format=date_format)
    else:
        try:
            data.to_csv(pathx+'.csv',index=savecol,chunksize=chunksize,encoding='utf-8_sig',date_format=date_format)
        except Exception as e:
            pass
    return

#pd.read_csv()方法中header参数，默认为0，标签为0（即第1行）的行为表头。若设置为-1，则无表头
#dtype:强制把str格式的存为str，否则会自动存为int,dtype={'ticker_symbol':'str'}
def read_csv_self(pathx,index=False,index_col=[],dtcolname=[],parse_dates=False,dtformat='%Y-%m-%d',big=False,chunksize=10000,header=0,dtype={}):
    # index_col和dtcolname都转成list,如果index_col和dtcolname有值，则设置index=True,parse_dates=True
    index_col=index_col if isinstance(index_col,list) else [index_col]
    dtcolname=dtcolname if isinstance(dtcolname,list) else [dtcolname]
    if (len(index_col)>0)&(index==False): index=True
    if (len(dtcolname)>0)&(parse_dates==False): parse_dates=True
    
    if big==False:
        data=pd.read_csv(pathx+'.csv',parse_dates=parse_dates,header=header,dtype=dtype)
    else:
        df=pd.read_csv(pathx+'.csv',parse_dates=parse_dates,iterator=True,header=header,dtype=dtype)
        chunks=[]
        while True:
            try:
                chunk=df.get_chunk(chunksize)
                chunks.append(chunk)
            except Exception as e:
                break
        data=pd.concat(chunks)
    if parse_dates==True:
        if (len(dtcolname)==1)&(dtcolname[0]=='allcolumn'):
            dtcolname=data.columns if index==False else [x for x in data.columns if (x not in index_col)]
            for i in dtcolname:
                data[i]=pd.to_datetime(data[i],format=dtformat)
        elif isinstance(dtcolname,list):
            for i in dtcolname:
                data[i]=pd.to_datetime(data[i],format=dtformat)

    if index==True:
        data.set_index(index_col,inplace=True)
    return data

def cut_group(data,num,cutrange=[],drop0=False):
    if drop0==True:
        data_na=data[data==0]
        data_nona=data[data!=0]
    else:   
        data_nona=data
    if len(cutrange)>0:
        result=pd.cut(data_nona,cutrange,right=True,labels=False) #默认左开右闭
    else:
        result=pd.Series(np.nan,index=data_nona.index)
        piece=(1/num)
        data_rank=data_nona.rank(method='dense',pct=True)
        for i in range(num):
            if i==0:
                result[(data_rank>=(piece*i))&(data_rank<=(piece*(i+1)))]=int(i)
            else:
                result[(data_rank>(piece*i))&(data_rank<=(piece*(i+1)))]=int(i)
    if drop0==True:
        result_final=data_na.append(result+1).reindex(data.index)
        return result_final
    else:
        result=result+1
        return result

def judgenotdo(data):
    if all(data.isnull()):
        return True
    elif len(np.unique(data.dropna()))==1:
        return True
    elif (data.notnull().sum()<5):
        return True
    else:
        return False
    
def rank_data(data,drop0=False,direction=True,reverse0=True):
    data_rank=data.copy()
    if (((drop0==False)&judgenotdo(data_rank))|((drop0==True)&judgenotdo(data_rank[data_rank!=0]))):
        data_rank=pd.Series(np.nan,index=data.index)
        return data_rank
    
    if drop0==True:
        data0index=data_rank[data_rank==0].index
        datano0index=data_rank[data_rank!=0].index
        data_rank.loc[datano0index]=data_rank.loc[datano0index].rank(method='dense',ascending=direction,pct=True)
        if (direction==False)&(reverse0==True):#如果正排序，则0是最小的；如果负排序，0是最大的
            data_rank.loc[data0index]=1
        else:
            pass
    else:
        data_value=np.unique(data_rank.dropna())
        if ((len(data_value)==2) &(0 in data_value)&(1 in data_value)): #如果是dummy variable
            if direction==False:#如果正排序，则0是最小的；如果负排序，0是最大的,1是最小的
                data_rank=1-data_rank
            else:
                pass
        else:
            data_rank=data_rank.rank(method='dense',ascending=direction,pct=True)
    return data_rank

def powerself(x,n):
    if isinstance(x,pd.Series):
        return pd.Series([y**(n) if y>=0 else -(-y)**(n) for y in x],index=x.index)
    else:
        return x**(n) if x>=0 else -(-x)**(n)

def isnumber(aString):
    try:
        float(aString)
        return True
    except:
        return False  

def standardreturn(period):
    switcher = {'Y': 1,'Q': 4, 'M': 12,'W':52,'D':252}
    return switcher.get(period, "nothing")

def periodtofreq(period):
    switcher = {'Y': 'A-DEC','Q': 'Q-DEC', 'M': 'M','W':'W','D':'D'}
    return switcher.get(period, "nothing")

def get_benchSecuCode(benchname):
    switcher = {'沪深300':'000300.SH','中证500':'000905.SH','中证800':'000906.SH','中证1000':'000852.SH','中证全指':'000985.CSI','华夏50ETF':'510050.SH','中证红利指数':'000922.CSI','上证50':'000016.SH','创业板指':'399006.SZ','中小板指':'399005.SZ','科创50':'0000688.SH'}
    return switcher.get(benchname, "nothing")

def get_benchnamefs(benchcode):
    switcher = {'000300':'沪深300','000905':'中证500','000906':'中证800','000852':'中证1000','000985':'中证全指','510050':'华夏50ETF','000922':'中证红利指数','000016':'上证50','399006':'创业板指','399005':'中小板指','0000688':'科创50'}
    return switcher.get(benchcode, "nothing")

# 010301-证监会行业V2012；010303-申万行业分类；010306-恒生行业；010314-中证行业分类（2016版）；010317-中信行业分类；010319-战略性新兴产业(2018)
def get_indu_type_id(induname):
    switcher = {'申万':'010303','中证':'010314','证监会':'010301','恒生':'010306','中信':'010317'}
    return switcher.get(induname, "nothing")