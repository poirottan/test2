# -*- coding: UTF-8 -*-

import pandas as pd
import numpy as np
import os
import re
import matplotlib.pyplot as plt #画图
import seaborn as sns
import math
plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
plt.rcParams['axes.unicode_minus']=False #用来正常显示负号
from numpy import nan as NaN
import datetime
from datetime import datetime,timedelta,date
from collections import OrderedDict #保持Key的顺序
from collections import Iterable #判断是否可以循环

#tqdm显示进程
from tqdm import tqdm 
tqdm.pandas(desc="my bar!") #保证tqdm在pandas的apply和groupby apply中也可以用
#tqdm(, ncols=1)保证进度条显示在一行

#warning
import warnings
warnings.filterwarnings('ignore')

#特殊的模块
from WindPy import *

#自己的模块
import sys #添加路径，导入模块
#sys.path
sys.path.append(r'D:\anacoda\selfku')
from SQLServer import SQLServer


#step 0 准备函数
def save_csv_self(data,pathx,savecol=True,big=False,chunksize = 100000):
    if big==False:
        data.to_csv(pathx+'.csv',index=savecol)
    else:
        try:
            data.to_csv(pathx+'.csv',index=savecol,chunksize=chunksize)
        except Exception as e:
            pass
    return

#pd.read_csv()方法中header参数，默认为0，标签为0（即第1行）的行为表头。若设置为-1，则无表头
def read_csv_self(pathx,index=False,index_col=['Unnamed: 0'],dtcolname='dt',parse_dates=False,dtformat='%Y-%m-%d',big=False,chunksize=10000,header=0):
    if big==False:
        data=pd.read_csv(pathx+'.csv',parse_dates=parse_dates,header=header)
    else:
        df=pd.read_csv(pathx+'.csv',parse_dates=parse_dates,iterator=True,header=header)
        chunks=[]
        while True:
            try:
                chunk=df.get_chunk(chunksize)
                chunks.append(chunk)
            except Exception as e:
                break
        data=pd.concat(chunks)
    if parse_dates==True:
        if isinstance(dtcolname,list):
            for i in dtcolname:
                data[i]=pd.to_datetime(data[i],format=dtformat)
        else:
            data[dtcolname]=pd.to_datetime(data[dtcolname],format=dtformat)
    if index==True:
        data.set_index(index_col,inplace=True)
    return data

def powerself(x,n):
    if isinstance(x,pd.Series):
        return pd.Series([y**(n) if y>=0 else -(-y)**(n) for y in x],index=x.index)
    else:
        return x**(n) if x>=0 else -(-x)**(n)
            
def get_mode(modenum):
    switcher = {0:'simple',1:'LR',2:'TTM',3:'tongbi',4:'tongbizengsu',5:'guoqu5year',6:'guoqu3year',7:'huanbi'}
    return switcher.get(modenum, "nothing")

#获取时间的函数,periodstart=True是区间开始时间，istradeday取交易日
#st=SD;et=ED;freq='D';periodstart=False;istradeday=True
def get_date(st,et,freq,periodstart=False,istradeday=True): 
    if istradeday==True: 
        if freq not in ['Y','Q','M','W']:#'D'
            condition=''
        else:
            if freq=='Y':
                condition='YEAR'
            elif freq=='Q':  
                condition='QUARTER'
            elif freq=='M':
                condition='MONTH'   
            elif freq=='W':
                condition='WEEK' 
                
            if periodstart==True:
                condition=' and calendar_date='+condition+'_START_DATE'
            else:
                condition=' and calendar_date='+condition+'_END_DATE'
        
        db_code = "select distinct(calendar_date) from md_trade_cal " \
                  "where IS_OPEN=1 and EXCHANGE_CD='XSHG' "\
                  "and calendar_date>=\'" +st.strftime("%Y-%m-%d") +"\' and calendar_date<=\'"+et.strftime("%Y-%m-%d")+"\' " \
                  +condition+" order by calendar_date"
        result=sql.ExecQuery(db_code)
        result = pd.DatetimeIndex([list(x)[0] for x in result])
    else:
        if (periodstart==True)&(freq!='D'): 
            if freq=='W':
                result=pd.date_range(st,et,freq='W-MON')  
            elif freq=='M':
                result=pd.date_range(st,et,freq='MS')  
            elif freq=='Q':
                result=pd.date_range(st,et,freq='QS-JAN')  #1.1,4.1,7.1,10.1
            else:
                result=pd.date_range(st,et,freq='AS-JAN')  #1.1
        else:
            result=pd.date_range(st,et,freq=freq)   
    return result

#获取所有股票的列表
def get_stocklist(field='全部A股'):
    if field=='科创板':
        db_code = "SELECT distinct(a.security_id), a.sec_short_name, a.ticker_symbol, a.type_name, a.into_date, a.out_date, b.party_id " \
            "FROM md_sec_type a left join md_security b on a.security_id = b.security_id " \
                "where a.type_name like '%科创板%' order by a.into_date,a.security_id"
    else:
        db_code = "SELECT distinct(a.security_id), a.sec_short_name, a.ticker_symbol, a.type_name, a.into_date, a.out_date, b.party_id " \
            "FROM md_sec_type a left join md_security b on a.security_id = b.security_id " \
                "where a.type_name = '全部A股' order by a.ticker_symbol"
    result = sql.ExecQuery(db_code)
    result = pd.DataFrame(result, columns = ['security_id', 'sec_short_name', 'ticker_symbol', 'type_name', 'into_date', 'out_date', 'party_id'])
    result['into_date']=pd.to_datetime(result['into_date'],format='%Y-%m-%d') 
    result['out_date']=pd.to_datetime(result['out_date'],format='%Y-%m-%d')
    return result

def get_mergedata(result,keycol,keydt='publish_date',usesecurity=False,fillmethod='na',singletime=False):    
    result=result.rename(columns={keydt:'trddt'})
    result=result.set_index('trddt')
    if singletime==True:
        itime=result.index[-1]
        if usesecurity==True:
            result=pd.merge(stocklist_df.loc[itime],result,on = ['security_id'], how = "left")    
        else:
            result=pd.merge(stocklist_df.loc[itime],result,on = ['party_id'], how = "left")    
        result['trddt']=itime
    else:
        if usesecurity==True:
            if fillmethod=='ffill':
                result=result.groupby('security_id')[keycol].apply(lambda x:x.reindex(Dt_all,method='ffill'))  
            elif fillmethod=='fill0':
                result=result.groupby('security_id')[keycol].apply(lambda x:x.reindex(Dt_all,fill_value=0)) 
            else:
                result=result.groupby('security_id')[keycol].apply(lambda x:x.reindex(Dt_all))
            result.index.names= ['security_id', 'trddt']
            result=pd.merge(stocklist_df,result, on = ['trddt','security_id'], how = "left")
        else:
            if fillmethod=='ffill':
                result=result.groupby('party_id')[keycol].apply(lambda x:x.reindex(Dt_all,method='ffill'))
            elif fillmethod=='fill0':
                result=result.groupby('party_id')[keycol].apply(lambda x:x.reindex(Dt_all,fill_value=0))
            else:
                result=result.groupby('party_id')[keycol].apply(lambda x:x.reindex(Dt_all))
            result.index.names= ['party_id', 'trddt']
            result=pd.merge(stocklist_df,result, on = ['trddt','party_id'], how = "left")
    
    result=result.reset_index().set_index(['trddt','security_id'])[keycol].sort_index()     
    # result.name=keycol
    if fillmethod=='fill0':
        result=result.fillna(0)
    
    return result

# tablename='vw_fdmt_bs';keycol='T_EQUITY_ATTR_P';givenstocklist=[];st='';et='';keydt='publish_date';singletime=False
# tablename='vw_fdmt_bs';keycol='T_EQUITY_ATTR_P';givenstocklist=stocklist_df.loc[Dt_all[0]];st='';et=Dt_all[0];keydt='publish_date';singletime=True
# tablename='vw_fdmt_bs';keycol='T_EQUITY_ATTR_P'
def get_latestdata_ttm(tablename,keycol,givenstocklist=[],st='',et='',keydt='publish_date',singletime=False):
    if ('_q_' in tablename)|('_ttm_' in tablename):#如果不含有act_pubtime和end_date_rep 
        if singletime==False:
            db_code = "select b.* from (select a.PARTY_ID,a.publish_date,a.END_DATE,a."+keycol+",row_number() "\
                        "over(partition by a.party_id,a.publish_date order by a.end_date desc) as n from "+tablename+" as a "
        else:
            db_code = "select b.* from (select a.PARTY_ID,a.publish_date,a.END_DATE,a."+keycol+",row_number() "\
                        "over(partition by a.party_id order by a.end_date desc,a.publish_date desc) as n from "+tablename+" as a "
    else:
        if singletime==False:
            db_code = "select b.* from (select a.PARTY_ID,a.publish_date,a.END_DATE,a."+keycol+",row_number() "\
                        "over(partition by a.party_id,a.publish_date order by a.end_date desc,a.act_pubtime desc,a.end_date_rep desc) as n from "+tablename+" as a "
        else:
            db_code = "select b.* from (select a.PARTY_ID,a.publish_date,a.END_DATE,a."+keycol+",row_number() "\
                        "over(partition by a.party_id order by a.end_date desc,a.publish_date desc,a.act_pubtime desc,a.end_date_rep desc) as n from "+tablename+" as a "

    if len(givenstocklist)==0:#如果没有给定stock
        db_code = db_code+ "where a.MERGED_FLAG=1 and a.PARTY_ID in (" + ','.join(stocklist['party_id'].astype(str).tolist()) + ") "
    else:
        db_code = db_code+ "where a.MERGED_FLAG=1 and a.PARTY_ID in (" + ','.join(givenstocklist['party_id'].astype(str).tolist()) + ") "
           
     #时间筛选
    if (st!='')&(et==''):
        db_code = db_code+ "and "+keydt+">='"+st.strftime("%Y-%m-%d")+"' "
    elif (st=='')&(et!=''):
        db_code = db_code+ "and "+keydt+"<='"+et.strftime("%Y-%m-%d")+"' "
    elif (st!='')&(et!=''):
        db_code = db_code+ "and "+keydt+">='"+st.strftime("%Y-%m-%d")+ "' and "+keydt+"<='"+et.strftime("%Y-%m-%d")+"' "

    db_code=db_code+") as b where b.n=1 order by b.PARTY_ID,b.PUBLISH_DATE,b.END_DATE"
    
    result = sql.ExecQuery(db_code)
    result = pd.DataFrame(result, columns = ['party_id', 'publish_date', 'end_date', keycol,'n'])
    result['publish_date']=pd.to_datetime(result['publish_date'],format='%Y-%m-%d') 
    result['end_date']=pd.to_datetime(result['end_date'],format='%Y-%m-%d') 
    result['max_end_date']=result.groupby('party_id')['end_date'].apply(lambda x:x.cummax())
    result=result[result['end_date']==result['max_end_date']]
    
    result[keycol]=result[keycol].astype('float')
    result=result.rename(columns={keycol:'value'})
    if singletime==False:
        result['trddt']=result[keydt]
    else:
        result['trddt']=et
    result=get_mergedata(result,keycol=['value','end_date'],keydt='trddt',usesecurity=False,fillmethod='ffill',singletime=singletime)

    return result

# tablename='vw_fdmt_bs';indicator=['party_id', 'publish_date', 'end_date','T_EQUITY_ATTR_P'];orderindicator="party_id,publish_date,end_date";givenstocklist=[];st='';et='';keydt='publish_date';dtcol=[];keycol2='T_EQUITY_ATTR_P';fillmethod='na';has_merged_flag=True;wherecondition='';singletime=False;usesecurity=False
# tablename='mkt_equd';indicator=['security_id', 'TRADE_DATE', 'market_value'];orderindicator=['security_id','TRADE_DATE'];givenstocklist=stocklist_df.loc[Dt_all[0]];st=Dt_all[0];et=Dt_all[0];keycol='market_value';keepkeycol=True;usesecurity=True;keydt='TRADE_DATE';singletime=True;has_merged_flag=False;wherecondition='';dtcol=[]
# tablename='equ_div';indicator=['security_id', 'ticker_symbol', 'end_date', 'publish_date','shc_publish_date','im_publish_date','ex_div_date','event_process_cd','per_cash_div','base_shares'];orderindicator="security_id,end_date";st=Dt_all_PD1[0];et=Dt_all_PD1[-1];usesecurity=True
def get_simpledatafromsql(tablename,indicator,orderindicator,givenstocklist=[],st='',et='',keycol='',keepkeycol=False,usesecurity=False,keydt='publish_date',dtcol=[],fillmethod='na',has_merged_flag=False,wherecondition='',singletime=False):
    indicator=indicator if isinstance(indicator,list) else indicator.split(',')
    orderindicator=orderindicator if isinstance(orderindicator,list) else orderindicator.split(',')
    
    #用party_id还是security_id筛选
    if usesecurity==True:
        if len(givenstocklist)==0:#如果没有给定stock
            db_code = "select "+','.join(indicator)+" from "+tablename+" "\
                       "where SECURITY_ID in (" + ','.join(stocklist['security_id'].astype(str).tolist()) + ") "
        else:
            db_code = "select "+','.join(indicator)+" from "+tablename+" "\
                       "where SECURITY_ID in (" + ','.join(givenstocklist['security_id'].astype(str).tolist()) + ") "
    else:
        if len(givenstocklist)==0:#如果没有给定stock
            db_code = "select "+','.join(indicator)+" from "+tablename+" "\
                       "where party_id in (" + ','.join(stocklist['party_id'].astype(str).tolist()) + ") "
        else:
            db_code = "select "+','.join(indicator)+" from "+tablename+" "\
                       "where party_id in (" + ','.join(givenstocklist['party_id'].astype(str).tolist()) + ") "

    #时间筛选
    if (st!='')&(et==''):
        db_code = db_code+ "and "+keydt+">='"+st.strftime("%Y-%m-%d")+"' "
    elif (st=='')&(et!=''):
        db_code = db_code+ "and "+keydt+"<='"+et.strftime("%Y-%m-%d")+"' "
    elif (st!='')&(et!=''):
        db_code = db_code+ "and "+keydt+">='"+st.strftime("%Y-%m-%d")+ "' and "+keydt+"<='"+et.strftime("%Y-%m-%d")+"' "
    
    #其他条件
    if has_merged_flag==True:
        db_code = db_code+ "and merged_flag=1 "
    if len(wherecondition)>0:
        db_code = db_code+ wherecondition
    db_code = db_code+ " order by "+','.join(orderindicator)
    result = sql.ExecQuery(db_code)      
    result = pd.DataFrame(result, columns = indicator)
    
    #调整时间格式和数字格式
    dtcol=dtcol if len(dtcol)>0 else [x for x in indicator if ('date' in x.lower())|('time' in x.lower())]
    if len(dtcol)>0:
        for i in dtcol:
            result[i]=pd.to_datetime(result[i],format='%Y-%m-%d')    
    if keycol!='':
        if isinstance(keycol,list):
            for i in keycol:
                result[i]=result[i].astype('float')
        else:       
            result[keycol]=result[keycol].astype('float')
       
    #是否只返回一列，并mergedata
    if keepkeycol==True:
        result=get_mergedata(result,keycol=keycol,keydt=keydt,usesecurity=usesecurity,fillmethod=fillmethod,singletime=singletime)
    return result

# givenstocklist=stocklist_df.loc[itime];st='';et=itime;singletime=True
# lr:tablename='vw_fdmt_bs';indicator=['party_id', 'publish_date','act_pubtime', 'end_date','end_date_rep','T_EQUITY_ATTR_P'];orderindicator="party_id,publish_date,act_pubtime,end_date,end_date_rep";givenstocklist=[];st='';et='';keydt='publish_date';dtcol=[];keycol='T_EQUITY_ATTR_P';fillmethod='ffill';has_merged_flag=True;modenum=0;singletime=False
#tongbi:tablename='vw_fdmt_is';indicator=['party_id', 'publish_date', 'end_date','REVENUE'];orderindicator="party_id,publish_date,end_date";givenstocklist=stocklist_df.loc[Dt_all[0]];et=Dt_all[0];keycol='REVENUE';fillmethod='ffill';has_merged_flag=True;wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ";modenum=3;singletime=True
# ttm:tablename='fdmt_main_data_pit';indicator=['party_id', 'publish_date', 'end_date','NI_ATTR_P_CUT'];orderindicator="party_id,publish_date,end_date";givenstocklist=[];st='';et='';keydt='publish_date';dtcol=[];keycol='NI_ATTR_P_CUT';fillmethod='ffill';has_merged_flag=True;wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ";modenum=2;singletime=False
# ttm_s:tablename='fdmt_main_data_pit';indicator=['party_id', 'publish_date', 'end_date','NI_ATTR_P_CUT'];orderindicator="party_id,publish_date,end_date";givenstocklist=stocklist_df.loc[Dt_all[0]];st='';et=Dt_all[0];keydt='publish_date';dtcol=[];keycol='NI_ATTR_P_CUT';fillmethod='ffill';has_merged_flag=True;wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ";modenum=2;singletime=True
# zengsu:tablename='fdmt_main_data_pit';indicator=['party_id', 'publish_date', 'end_date','NI_ATTR_P_CUT'];orderindicator="party_id,publish_date,end_date";givenstocklist=[];st='';et='';keydt='publish_date';dtcol=[];keycol='NI_ATTR_P_CUT';fillmethod='ffill';has_merged_flag=True;wherecondition=" and report_type = 'A' ";modenum=5;singletime=False
# zengsu_s:tablename='fdmt_main_data_pit';indicator=['party_id', 'publish_date', 'end_date','NI_ATTR_P_CUT'];orderindicator="party_id,publish_date,end_date";givenstocklist=stocklist_df.loc[Dt_all[0]];st='';et=Dt_all[0];keydt='publish_date';dtcol=[];keycol='NI_ATTR_P_CUT';fillmethod='ffill';has_merged_flag=True;wherecondition=" and report_type = 'A' ";modenum=5;singletime=True
# modedict= {0:'simple',1:'LR',2:'TTM',3:'tongbi',4:'tongbizengsu',5:'guoqu5year',6:'guoqu3year',7:'huanbi',8:'singleqtable_tongbizengsu',9:'average'}
def get_calculatefactor_fr(tablename,indicator,orderindicator="party_id,publish_date,act_pubtime,end_date,end_date_rep",givenstocklist=[],st='',et='',keycol='',usesecurity=False,keydt='publish_date',dtcol=[],fillmethod='na',has_merged_flag=False,wherecondition='',modenum=0,singletime=False): 
    grouped_data=get_simpledatafromsql(tablename,indicator,orderindicator,givenstocklist=givenstocklist,st=st,et=et,keycol=keycol,keepkeycol=False,usesecurity=False,keydt=keydt,dtcol=dtcol,fillmethod=fillmethod,has_merged_flag=has_merged_flag,wherecondition=wherecondition)
    grouped_data=grouped_data.rename(columns={keydt:'trddt',keycol:'value'})
    # grouped_data=grouped_data[grouped_data['value'].notnull()]
    grouped_data=grouped_data.groupby('party_id')
    
    result=OrderedDict()
    for groupname,idata in tqdm(grouped_data):
        idata=idata.sort_values(by=['end_date','trddt','act_pubtime','end_date_rep'])
        resulti=pd.DataFrame()
        
        if singletime==True: #只循环1次,当前的trddt
            datelist=[et]
        else:#循环多次,每次的pub date
            datelist=sorted(np.unique(idata[idata['trddt']>=PD1]['trddt']))
    
        if len(datelist)==0:
            continue
        
        for itime in datelist:
            temp=idata[(idata['trddt']<=itime)]
            if len(temp)==0:
                continue
            ilatestdata=temp[temp['end_date']==temp['end_date'].iloc[-1]]['value']
              
            # #如果最新财报太早，小于PD1，则整个时间都不要                
            # if temp['end_date'].iloc[-1]<PD1:
            #     continue
            
            if ilatestdata.isnull().all():
                #如果最新一期end_date是空值
                resulti.at[itime,'value']=np.nan
                resulti.at[itime,'end_date']=temp['end_date'].iloc[-1]
            elif modenum==0:
                #如果最新一期end_date不是空值，且modenum=0（简单取最后一期不需要resample）
                resulti.at[itime,'value']=ilatestdata.dropna().iloc[-1]
                resulti.at[itime,'end_date']=temp['end_date'].iloc[-1]
            else: #如果最新一期end_date不是空值   
                temp=temp[temp['value'].notnull()] #dropna  (因为已经判断过最后一期是否为na)       
                temp=temp.drop_duplicates(subset='end_date',keep='last', inplace=False).set_index('end_date')
                
                if modenum in (5,6):
                    temp=temp.sort_index().resample('A-DEC').last()
                else:
                    temp=temp.sort_index().resample('Q-DEC').last()
                
                if modenum==1:#LR
                    temp['final']=np.where(temp.index.month==3,temp['value']*4,np.where(temp.index.month==6,temp['value']*2,np.where(temp.index.month==9,temp['value']*4/3,temp['value'])))
                elif modenum==2:#ttm
                    temp['diff']=temp['value']-temp['value'].shift(1)
                    temp['value_final']=np.where(temp.index.month==3,temp['value'],temp['diff'])
                    temp['final1']=np.where(temp.index.month==12,temp['value'],temp['value_final']+temp['value_final'].shift(1)+temp['value_final'].shift(2)+temp['value_final'].shift(3))
                    temp['final2']=np.where(temp.index.month==3,temp['value']+temp['value'].shift(1)-temp['value'].shift(4),np.where(temp.index.month==6,temp['value']+temp['value'].shift(2)-temp['value'].shift(4),np.where(temp.index.month==9,temp['value']+temp['value'].shift(3)-temp['value'].shift(4),temp['value'])))
                    temp['final']=np.where(temp['final1'].isnull(),temp['final2'],temp['final1'])
                elif modenum==3:#同比：LR
                    temp['final']=np.where(temp['value'].shift(4)==0,np.nan,(temp['value']-temp['value'].shift(4))/abs(temp['value'].shift(4)))
                elif modenum==4:#同比增速：单季度
                    temp['diff']=temp['value']-temp['value'].shift(1)
                    temp['value_final']=np.where(temp.index.month==3,temp['value'],temp['diff'])
                    temp['final']=np.where((temp['value_final'].shift(4)==0)|(temp['value_final'].shift(5)==0),np.nan,(temp['value_final']-temp['value_final'].shift(4))/abs(temp['value_final'].shift(4))-(temp['value_final'].shift(1)-temp['value_final'].shift(5))/abs(temp['value_final'].shift(5)))
                elif modenum==5:#5年复合增速
                    temp['final']=np.where(temp['value'].shift(5)==0,np.nan,np.where(temp['value'].shift(5)>0,powerself(temp['value']/temp['value'].shift(5),1/5)-1,powerself(1-(temp['value']/temp['value'].shift(5)),1/5)))
                elif modenum==6:#3年复合增速
                    temp['final']=np.where(temp['value'].shift(3)==0,np.nan,np.where(temp['value'].shift(3)>0,powerself(temp['value']/temp['value'].shift(3),1/3)-1,powerself(1-(temp['value']/temp['value'].shift(3)),1/3)))
                elif modenum==7:#环比：单季度
                    temp['diff']=temp['value']-temp['value'].shift(1)
                    temp['value_final']=np.where(temp.index.month==3,temp['value'],temp['diff'])
                    temp['final']=np.where(temp['value_final'].shift(1)==0,np.nan,(temp['value_final']-temp['value_final'].shift(1))/abs(temp['value_final'].shift(1)))
                elif modenum==8:#同比增速：单季度 #读单季度表的同比增速
                    temp['final']=np.where((temp['value'].shift(4)==0)|(temp['value'].shift(5)==0),np.nan,(temp['value']-temp['value'].shift(4))/abs(temp['value'].shift(4))-(temp['value'].shift(1)-temp['value'].shift(5))/abs(temp['value'].shift(5)))
                elif modenum==9:#当年和上年同期平均
                    temp['final']=(temp['value']+temp['value'].shift(4))/2
                elif modenum==10:#同比：单季度
                    temp['diff']=temp['value']-temp['value'].shift(1)
                    temp['value_final']=np.where(temp.index.month==3,temp['value'],temp['diff'])
                    temp['final']=np.where(temp['value_final'].shift(4)==0,np.nan,(temp['value_final']-temp['value_final'].shift(4))/abs(temp['value_final'].shift(4)))

                temp=temp.sort_index()
                resulti.at[itime,'value']=temp['final'].iloc[-1]
                resulti.at[itime,'end_date']=temp.index[-1]

            
        result[groupname]=resulti
     
    result=pd.concat(result.values(), keys=result.keys())
    result.index.names=['party_id','trddt']
    # result.name=keycol
    result=result.reset_index()
    result=get_mergedata(result,keycol=['value','end_date'],keydt='trddt',usesecurity=False,fillmethod=fillmethod,singletime=singletime)
    
    return result

# tablename2='vw_fdmt_bs';indicator2=['party_id', 'publish_date','act_pubtime', 'end_date','end_date_rep','T_EQUITY_ATTR_P'];orderindicator="party_id,publish_date,act_pubtime,end_date,end_date_rep";givenstocklist=[];st='';et='';keydt='publish_date';dtcol=[];keycol2='T_EQUITY_ATTR_P';fillmethod='ffill';has_merged_flag2=True;modenum2=1;wherecondition2='';singletime=False;keeppos=True
# tablename1='fdmt_main_data_pit';indicator1=['party_id', 'publish_date','act_pubtime','end_date','end_date_rep','NI_ATTR_P_CUT'];orderindicator="party_id,publish_date,act_pubtime,end_date,end_date_rep";givenstocklist=[];st='';et='';keydt='publish_date';dtcol=[];keycol1='NI_ATTR_P_CUT';fillmethod='ffill';has_merged_flag1=True;wherecondition1=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ";modenum1=1;singletime=False
def get_calculatefactor_fr2(tablename1,indicator1,tablename2,indicator2,orderindicator="party_id,publish_date,act_pubtime,end_date,end_date_rep",givenstocklist=[],st='',et='',keycol1='',keycol2='',usesecurity=False,keydt='publish_date',dtcol=[],fillmethod='na',has_merged_flag1=False,has_merged_flag2=False,wherecondition1='',wherecondition2='',modenum1=0,modenum2=0,singletime=False,keeppos=True,selectlastnonull=False): 
    grouped_data1=get_simpledatafromsql(tablename1,indicator1,orderindicator,keycol=keycol1,usesecurity=False,has_merged_flag=has_merged_flag1,wherecondition=wherecondition1)
    grouped_data2=get_simpledatafromsql(tablename2,indicator2,orderindicator,keycol=keycol2,usesecurity=False,has_merged_flag=has_merged_flag2,wherecondition=wherecondition2)
    
    grouped_data1=grouped_data1.rename(columns={keydt:'trddt'})
    grouped_data2=grouped_data2.rename(columns={keydt:'trddt'})
        
    grouped_data=pd.merge(grouped_data1,grouped_data2,on=['party_id','trddt','act_pubtime','end_date','end_date_rep'],how='outer').sort_values(by=['party_id','trddt','act_pubtime','end_date','end_date_rep'])
    grouped_data=grouped_data.groupby('party_id')
    
    result=OrderedDict()
    for groupname,idata in tqdm(grouped_data):
        idata=idata.sort_values(by=['end_date','trddt','act_pubtime','end_date_rep'])
            
        resulti=pd.DataFrame()
        if singletime==True: #只循环1次,当前的trddt
            datelist=[et]
        else:#循环多次,每次的pub date
            datelist=sorted(np.unique(idata[idata['trddt']>=PD1]['trddt']))
        
        if len(datelist)==0:
            continue
        
        for itime in datelist:
            tempall=idata[(idata['trddt']<=itime)]
            if len(tempall)==0:
                continue
            ilatestdata=tempall[tempall['end_date']==tempall['end_date'].iloc[-1]][[keycol1,keycol2]]
            
            # #如果最新财报太早，小于PD1，则整个时间都不要   
            # if tempall['end_date'].iloc[-1]<PD1:
            #     continue
            
            #如果最新一期end_date,keycol1或者keycol2是空值                     
            if ((ilatestdata[keycol1].isnull().all())|(ilatestdata[keycol2].isnull().all())):
                resulti.at[itime,'value']=np.nan
                resulti.at[itime,'end_date']=tempall['end_date'].iloc[-1]
                continue

            #分子
            temp=tempall[['trddt','end_date',keycol1]].copy()
            temp=temp.rename(columns={keycol1:'value'})
            temp=temp[temp['value'].notnull()]        
            temp=temp.drop_duplicates(subset='end_date',keep='last', inplace=False).set_index('end_date')
            temp=temp.sort_index().resample('Q-DEC').last()     
            
            if modenum1==2:#ttm
                temp['diff']=temp['value']-temp['value'].shift(1)
                temp['value_final']=np.where(temp.index.month==3,temp['value'],temp['diff'])
                temp['final1']=np.where(temp.index.month==12,temp['value'],temp['value_final']+temp['value_final'].shift(1)+temp['value_final'].shift(2)+temp['value_final'].shift(3))
                temp['final2']=np.where(temp.index.month==3,temp['value']+temp['value'].shift(1)-temp['value'].shift(4),np.where(temp.index.month==6,temp['value']+temp['value'].shift(2)-temp['value'].shift(4),np.where(temp.index.month==9,temp['value']+temp['value'].shift(3)-temp['value'].shift(4),temp['value'])))
                temp['final']=np.where(temp['final1'].isnull(),temp['final2'],temp['final1'])
            else: #lr
                temp['final']=np.where(temp.index.month==3,temp['value']*4,np.where(temp.index.month==6,temp['value']*2,np.where(temp.index.month==9,temp['value']*4/3,temp['value'])))
    
            resulti_up=temp['final']
            
            #分母
            temp=tempall[['trddt','end_date',keycol2]].copy()
            temp=temp.rename(columns={keycol2:'value'})
            temp=temp[temp['value'].notnull()]   
            temp=temp.drop_duplicates(subset='end_date',keep='last', inplace=False).set_index('end_date')
            temp=temp.sort_index().resample('Q-DEC').last()   
            
            if modenum2==2: #平均
                temp['final']=(temp['value']+temp['value'].shift(4))/2
            else:#lr
                temp['final']=temp['value']
            
            resulti_down=temp['final']
            
            resulti_merge=pd.merge(resulti_up,resulti_down,left_index=True,right_index=True,how='outer',suffixes=['up','down'])
            if keeppos==True:
                resulti_merge['finalresult']=np.where(resulti_merge['finaldown']>0,resulti_merge['finalup']/resulti_merge['finaldown'],np.nan)
            else:
                resulti_merge['finalresult']=np.where(resulti_merge['finaldown']!=0,resulti_merge['finalup']/resulti_merge['finaldown'],np.nan)

            resulti_merge=resulti_merge.sort_index()            
            
            if selectlastnonull==True:
                resulti.at[itime,'value']=resulti_merge['finalresult'].dropna().iloc[-1]
                resulti.at[itime,'end_date']=resulti_merge['finalresult'].dropna().index[-1]  
            else:
                resulti.at[itime,'value']=resulti_merge['finalresult'].iloc[-1]
                resulti.at[itime,'end_date']=resulti_merge.index[-1]   
            
            # if resulti_up.index[-1]==resulti_down.index[-1]:
            #     if ((keeppos==True)&(resulti_down.iloc[-1]>0))|((keeppos==False)&(resulti_down.iloc[-1]!=0)):
            #         resulti.at[itime,'value']=resulti_up.iloc[-1]/resulti_down.iloc[-1]
            #     else:
            #         resulti.at[itime,'value']=np.nan
            # else:
            #     resulti.at[itime,'value']=np.nan
            # resulti.at[itime,'end_date']=max(resulti_up.index[-1],resulti_down.index[-1])
        
                      
        result[groupname]=resulti
        
    result=pd.concat(result.values(), keys=result.keys())
    result.index.names=['party_id','trddt']
    result=result.reset_index()
    result=get_mergedata(result,keycol=['value','end_date'],keydt='trddt',usesecurity=False,fillmethod=fillmethod,singletime=singletime)
       
    return result

def merge2data(x,y,keeppos=True,cmpenddate=False,fillmethod='na'):
    if isinstance(x,pd.Series):
        x.name='value'
    if isinstance(x,pd.Series):
        y.name='value'
    result=pd.merge(x,y,left_index=True,right_index=True)
    if cmpenddate==True:
        if keeppos==True:
            result=pd.Series(np.where(result['end_date_x']==result['end_date_y'],np.where(result['value_y']>0,result['value_x']/result['value_y'],np.nan),np.nan),index=result.index)
        else:
            result=pd.Series(np.where(result['end_date_x']==result['end_date_y'],np.where(result['value_y']!=0,result['value_x']/result['value_y'],np.nan),np.nan),index=result.index)
    else:
        if keeppos==True:
            result=pd.Series(np.where(result['value_y']>0,result['value_x']/result['value_y'],np.nan),index=result.index)
        else:
            result=pd.Series(np.where(result['value_y']!=0,result['value_x']/result['value_y'],np.nan),index=result.index)
    result.name='value'
    if fillmethod=='ffill':
        result=result.groupby(level=1).ffill()
    elif fillmethod=='fill0':
        result=result.fillna(0)
    return result

#------------------------------------------------------------------------
#####################################################################################################################
#   正文
#####################################################################################################################
##设置回测区间
path=r'E:\盈科\因子库'
os.chdir(path)
sql = SQLServer("192.168.99.46","poirot","Aliyun123456","TLSJ")

readif=False
saveif=True
readif=True
saveif=False

SD=datetime.strptime("2017-01-01",'%Y-%m-%d')
ED=date.today()
PD=datetime.strptime("2000-01-01",'%Y-%m-%d') #数据多提取10年，用于计算过去5年
PD1=datetime.strptime("2016-01-01",'%Y-%m-%d') #数据多提取1年，用于计算过去5年
# =============================================================================
# #：取时间和stocklist
# =============================================================================
#取时间
Dt_all= get_date(SD,ED,'D',istradeday=False)
Dt_trade = get_date(SD,ED,'D')
Dt_all_PD = get_date(PD,ED,'D',istradeday=False)
Dt_trade_PD = get_date(PD,ED,'D')
Dt_all_PD1 = get_date(PD1,ED,'D',istradeday=False)
Dt_trade_PD1 = get_date(PD1,ED,'D')

#取全部A股（包括科创板）
stocklist=get_stocklist(field='全部A股')
stocklist_df = OrderedDict()
for idate in Dt_all:
    stocklist_df[idate] = stocklist[ (stocklist['into_date'].notnull()) & (stocklist['into_date'] <= idate)\
                            & ((stocklist['out_date'].isnull()) | (stocklist['out_date']>idate))][['security_id','party_id','ticker_symbol']].reset_index(drop=True)
stocklist_df=pd.concat(stocklist_df.values(), keys=stocklist_df.keys())
stocklist_df.index.names = ['trddt', 'num']

# =============================================================================
# 常用因子
# =============================================================================
#总市值
market_value_s=get_simpledatafromsql('mkt_equd',['security_id', 'TRADE_DATE', 'market_value'],['security_id','TRADE_DATE'],givenstocklist=stocklist_df.loc[Dt_all[0]],st=Dt_all[0],et=Dt_all[0],keycol='market_value',keepkeycol=True,usesecurity=True,keydt='TRADE_DATE',singletime=True)
market_value=get_simpledatafromsql('mkt_equd',['security_id', 'TRADE_DATE', 'market_value'],['security_id','TRADE_DATE'],st=Dt_all_PD1[360],et=Dt_all[-1],keycol='market_value',keepkeycol=True,usesecurity=True,keydt='TRADE_DATE',fillmethod='ffill')
# xx=market_value[market_value.isnull()] #只有se_id=77481 #na

#总股本
TOTAL_SHARES=get_simpledatafromsql('equ_share_change',['party_id', 'CHANGE_DATE', 'TOTAL_SHARES'],['party_id','CHANGE_DATE'],st=Dt_all_PD1[0],et=Dt_all[-1],keycol='TOTAL_SHARES',keepkeycol=True,usesecurity=False,keydt='CHANGE_DATE',fillmethod='ffill')

# 净利润(不含少数股东损益)_TTM
# N_INCOME_ATTR_P_TTM=get_latestdata_ttm('fdmt_is_ttm_pit','N_INCOME_ATTR_P')
N_INCOME_ATTR_P_TTM_s=get_calculatefactor_fr('vw_fdmt_is',['party_id','publish_date','act_pubtime','end_date','end_date_rep','N_INCOME_ATTR_P'],keycol='N_INCOME_ATTR_P',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=2,givenstocklist=stocklist_df.loc[Dt_all[0]],et=Dt_all[0],singletime=True)
N_INCOME_ATTR_P_TTM=get_calculatefactor_fr('vw_fdmt_is',['party_id','publish_date','act_pubtime','end_date','end_date_rep','N_INCOME_ATTR_P'],keycol='N_INCOME_ATTR_P',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=2)

# 净利润(不含少数股东损益)_LR
N_INCOME_ATTR_P_LR=get_calculatefactor_fr('vw_fdmt_is',['party_id','publish_date','act_pubtime','end_date','end_date_rep','N_INCOME_ATTR_P'],keycol='N_INCOME_ATTR_P',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=1)

#扣除非经常性损益后归母净利润_TTM
# NI_ATTR_P_CUT_TTM=get_latestdata_ttm('fdmt_der_ttm_pit','NI_ATTR_P_CUT')
NI_ATTR_P_CUT_TTM_s=get_calculatefactor_fr('fdmt_main_data_pit',['party_id','publish_date','act_pubtime','end_date','end_date_rep','NI_ATTR_P_CUT'],givenstocklist=stocklist_df.loc[Dt_all[0]],et=Dt_all[0],keycol='NI_ATTR_P_CUT',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=2,singletime=True)
NI_ATTR_P_CUT_TTM=get_calculatefactor_fr('fdmt_main_data_pit',['party_id','publish_date','act_pubtime','end_date','end_date_rep','NI_ATTR_P_CUT'],keycol='NI_ATTR_P_CUT',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=2)
# NI_ATTR_P_CUT_TTM.to_csv('NI_ATTR_P_CUT_TTM_'+ED.strftime("%Y-%m-%d")+'.csv')
# NI_ATTR_P_CUT_TTM=read_csv_self('NI_ATTR_P_CUT_TTM_20200723',index=True,index_col=['trddt','security_id'],dtcolname=['trddt','end_date'],parse_dates=True)

#扣除非经常性损益后归母净利润_LR
NI_ATTR_P_CUT_LR_s=get_calculatefactor_fr('fdmt_main_data_pit',['party_id','publish_date','act_pubtime','end_date','end_date_rep','NI_ATTR_P_CUT'],givenstocklist=stocklist_df.loc[Dt_all[0]],et=Dt_all[0],keycol='NI_ATTR_P_CUT',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=1,singletime=True)
NI_ATTR_P_CUT_LR=get_calculatefactor_fr('fdmt_main_data_pit',['party_id','publish_date','act_pubtime','end_date','end_date_rep','NI_ATTR_P_CUT'],keycol='NI_ATTR_P_CUT',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=1)

#股东权益_平均 (股东权益合计_最新财报 + 股东权益合计_去年同期)/2 (不含少数股东权益)
Equity_mean=get_calculatefactor_fr('vw_fdmt_bs',['party_id','publish_date','act_pubtime','end_date','end_date_rep','T_EQUITY_ATTR_P'],keycol='T_EQUITY_ATTR_P',fillmethod='ffill',has_merged_flag=True,modenum=9)
# Equity_mean.to_csv('Equity_mean_20200723.csv')
# Equity_mean=read_csv_self('Equity_mean_20200723',index=True,index_col=['trddt','security_id'],dtcolname=['trddt','end_date'],parse_dates=True)

#股东权益_LR (不含少数股东权益)
Equity_LR=get_latestdata_ttm('vw_fdmt_bs','T_EQUITY_ATTR_P')
# Equity_LR=get_calculatefactor_fr('vw_fdmt_bs',['party_id','publish_date','act_pubtime','end_date','end_date_rep','T_EQUITY_ATTR_P'],keycol='T_EQUITY_ATTR_P',fillmethod='ffill',has_merged_flag=True,modenum=0)

# 经营活动产生的现金流量净额_TTM
OCF_TTM=get_calculatefactor_fr('vw_fdmt_cf',['party_id','publish_date','act_pubtime','end_date','end_date_rep','N_CF_OPERATE_A'],keycol='N_CF_OPERATE_A',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=2)
# 经营活动产生的现金流量净额_LR
OCF_LR=get_calculatefactor_fr('vw_fdmt_cf',['party_id','publish_date','act_pubtime','end_date','end_date_rep','N_CF_OPERATE_A'],keycol='N_CF_OPERATE_A',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=1)

# 营业利润_TTM
OP_TTM=get_calculatefactor_fr('vw_fdmt_is',['party_id','publish_date','act_pubtime','end_date','end_date_rep','OPERATE_PROFIT'],keycol='OPERATE_PROFIT',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=2)

# 非流动负债_LR
T_NCL=get_latestdata_ttm('vw_fdmt_bs','T_NCL')

# =============================================================================
# #因子1DP，DP_LTM 个股回溯过去一年的现金分红总额/当前总市值
# =============================================================================
#----------------------方法一：自己计算------------------------------------------
# 个股回溯过去一年的现金分红总额，以公告日期为准
usepub=False
result=get_simpledatafromsql('equ_div',['security_id', 'ticker_symbol', 'end_date', 'publish_date','shc_publish_date','im_publish_date','ex_div_date','event_process_cd','per_cash_div','base_shares'],"security_id,end_date",st=Dt_all_PD1[0],et=Dt_all_PD1[-1],usesecurity=True)
result=result.rename(columns={'per_cash_div':'div'})

#对pub null的处理
# 如果pub日期没有，但是同一上股东大会日有另外一个提案，则用另一提案的pub date
for i in result[result['publish_date'].isnull()].index:
    conditon=(result['security_id']==result.at[i,'security_id'])&(result['end_date']==result.at[i,'end_date'])&(result['shc_publish_date']==result.at[i,'shc_publish_date'])&(result['publish_date'].notnull())
    if conditon.any():
        result.at[i,'publish_date']=result.at[result[conditon].index[0],'publish_date']
#如果填充完还是pub日期没有，就用SHC_PUBLISH_DATE股东大会公告日填充
result.loc[result['publish_date'].isnull(),'publish_date']=result.loc[result['publish_date'].isnull(),'shc_publish_date']
#如果填充完还是pub日期没有，就用IM_PUBLISH_DATE分红实施公告日填充
result.loc[result['publish_date'].isnull(),'publish_date']=result.loc[result['publish_date'].isnull(),'im_publish_date']

result=result[result['div'].notnull()] #筛去不是现金分红的
result=result[np.logical_not(result['event_process_cd'].isin([3,7,95,97]))] #筛去未成功实施或者否决的
result['sum_div_ttm']=result['div']*result['base_shares']

if usepub==True:
    result=result.rename(columns={'publish_date':'trddt'})
else:
    result=result.rename(columns={'ex_div_date':'trddt'})
result=result[(result['trddt'].notnull())&(result['trddt']>=Dt_all_PD1[0])]

#把每个publish_date加总起来
# temp=result.groupby('security_id').apply(lambda x:x.groupby('trddt')['div'].sum().rolling('365D').sum())

#个股回溯过去一年的现金分红总额，以公告日期/除权除息日为准
div=result[['security_id','trddt','sum_div_ttm']]
div=div.groupby('security_id').apply(lambda x:x.groupby('trddt')['sum_div_ttm'].sum().reindex(Dt_all_PD1).rolling('365D').sum())
div=div.stack()
div.index.names=['security_id','trddt']
div.name='sum_div_ttm'
div=get_mergedata(div.reset_index(),keycol='sum_div_ttm',keydt='trddt',usesecurity=True,fillmethod='fill0')

#DP_LTM：股息率
div_rate=merge2data(div,market_value,fillmethod='fill0')

#每股股利，以公告日期/除权除息日为准
DP=result[['security_id','trddt','div']].rename(columns={'publish_date':'trddt'})
DP=DP.groupby('security_id').apply(lambda x:x.groupby('trddt')['div'].sum())
DP=pd.merge(stocklist_df,DP, on = ['security_id','trddt'], how = "left")
DP=DP.fillna(0)
DP=DP.rename(columns={'div':'DP'})

'''
#----------------------方法二：取沪深股息率排名(mkt_rank_div_yield)------------------------------------------
#过去一年现金分红总额
div1=get_simpledatafromsql('mkt_rank_div_yield',['security_id', 'TRADE_DATE', 'market_value', 'sum_div_ttm','div_rate'],"SECURITY_ID,TRADE_DATE",st=Dt_all[0],et=Dt_all[-1],keycol='sum_div_ttm',keepkeycol=True,usesecurity=True,keydt='TRADE_DATE',fillmethod='fill0')
#过去一年股息率
div_rate1=get_simpledatafromsql('mkt_rank_div_yield',['security_id', 'TRADE_DATE', 'market_value', 'sum_div_ttm','div_rate'],"SECURITY_ID,TRADE_DATE",st=Dt_all[0],et=Dt_all[-1],keycol='div_rate',keepkeycol=True,usesecurity=True,keydt='TRADE_DATE',fillmethod='fill0')

核对过程
方法二的当天的因子值有问题
cmp=pd.merge(div_rate,div_rate1, on = ['trddt','security_id'],suffixes=('','1'))
xxx=(cmp['div_rate']//100==cmp['div_rate1']//100)|((cmp['div_rate']==0)&(cmp['div_rate1']==0))
xy=cmp[np.logical_not(xxx)]
'''


# =============================================================================
# #因子2 EP，市盈率倒数
# =============================================================================
# EP_TTM_Deducted 扣除非经常性损益后净利润_TTM/总市值
EP_TTM_Deducted=merge2data(NI_ATTR_P_CUT_TTM['value'],market_value,fillmethod='ffill')

# EP_LYR_Deducted 净利润（归母扣非）最新年报/总市值
EP_LR_Deducted=merge2data(NI_ATTR_P_CUT_LR['value'],market_value,fillmethod='ffill')

# =============================================================================
# 因子5 BP 市净率倒数
# =============================================================================
##BP_LR 股东权益合计(不含少数股东权益)_最新财报/总市值
BP_LR=merge2data(Equity_LR,market_value,fillmethod='ffill')

# =============================================================================
# 因子7 营收增速：成长因子
# =============================================================================
# Gr_5Y_Sale
Gr_5Y_Sale_s=get_calculatefactor_fr('vw_fdmt_is',['party_id','publish_date','act_pubtime','end_date','end_date_rep','REVENUE'],givenstocklist=stocklist_df.loc[Dt_all[0]],et=Dt_all[0],keycol='REVENUE',fillmethod='ffill',has_merged_flag=True,wherecondition="and report_type = 'A' ",modenum=5,singletime=True)

Gr_5Y_Sale=get_calculatefactor_fr('vw_fdmt_is',['party_id','publish_date','act_pubtime','end_date','end_date_rep','REVENUE'],keycol='REVENUE',fillmethod='ffill',has_merged_flag=True,wherecondition="and report_type = 'A' ",modenum=5)

# Gr_3Y_Sale
Gr_3Y_Sale_s=get_calculatefactor_fr('vw_fdmt_is',['party_id','publish_date','act_pubtime','end_date','end_date_rep','REVENUE'],givenstocklist=stocklist_df.loc[Dt_all[0]],et=Dt_all[0],keycol='REVENUE',fillmethod='ffill',has_merged_flag=True,wherecondition="and report_type = 'A' ",modenum=6,singletime=True)

Gr_3Y_Sale=get_calculatefactor_fr('vw_fdmt_is',['party_id','publish_date','act_pubtime','end_date','end_date_rep','REVENUE'],keycol='REVENUE',fillmethod='ffill',has_merged_flag=True,wherecondition="and report_type = 'A' ",modenum=6)

# Gr_1Y_Sale
Gr_1Y_Sale_s=get_calculatefactor_fr('vw_fdmt_is',['party_id','publish_date','act_pubtime','end_date','end_date_rep','REVENUE'],givenstocklist=stocklist_df.loc[Dt_all[0]],et=Dt_all[0],keycol='REVENUE',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=3,singletime=True)

Gr_1Y_Sale=get_calculatefactor_fr('vw_fdmt_is',['party_id','publish_date','act_pubtime','end_date','end_date_rep','REVENUE'],keycol='REVENUE',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=3)

# =============================================================================
# 因子8：净利增速 
# =============================================================================
# Gr_5Y_Earning
Gr_5Y_Earning_s=get_calculatefactor_fr('fdmt_main_data_pit',['party_id','publish_date','act_pubtime','end_date','end_date_rep','NI_ATTR_P_CUT'],givenstocklist=stocklist_df.loc[Dt_all[0]],et=Dt_all[0],keycol='NI_ATTR_P_CUT',fillmethod='ffill',has_merged_flag=True,wherecondition="and report_type = 'A' ",modenum=5,singletime=True)

Gr_5Y_Earning=get_calculatefactor_fr('fdmt_main_data_pit',['party_id','publish_date','act_pubtime','end_date','end_date_rep','NI_ATTR_P_CUT'],keycol='NI_ATTR_P_CUT',fillmethod='ffill',has_merged_flag=True,wherecondition="and report_type = 'A' ",modenum=5)

# Gr_3Y_Earning
Gr_3Y_Earning_s=get_calculatefactor_fr('fdmt_main_data_pit',['party_id','publish_date','act_pubtime','end_date','end_date_rep','NI_ATTR_P_CUT'],givenstocklist=stocklist_df.loc[Dt_all[0]],et=Dt_all[0],keycol='NI_ATTR_P_CUT',fillmethod='ffill',has_merged_flag=True,wherecondition="and report_type = 'A' ",modenum=6,singletime=True)

Gr_3Y_Earning=get_calculatefactor_fr('fdmt_main_data_pit',['party_id','publish_date','act_pubtime','end_date','end_date_rep','NI_ATTR_P_CUT'],keycol='NI_ATTR_P_CUT',fillmethod='ffill',has_merged_flag=True,wherecondition="and report_type = 'A' ",modenum=6)

# Gr_1Y_Earning
Gr_1Y_Earning_s=get_calculatefactor_fr('fdmt_main_data_pit',['party_id','publish_date','act_pubtime','end_date','end_date_rep','NI_ATTR_P_CUT'],givenstocklist=stocklist_df.loc[Dt_all[0]],et=Dt_all[0],keycol='NI_ATTR_P_CUT',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=3,singletime=True)

Gr_1Y_Earning=get_calculatefactor_fr('fdmt_main_data_pit',['party_id','publish_date','act_pubtime','end_date','end_date_rep','NI_ATTR_P_CUT'],keycol='NI_ATTR_P_CUT',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=3)

# =============================================================================
# 因子9 增速边际变化率 #方法一 用原始表
# =============================================================================
# Ac_SQ_Sale
Ac_SQ_Sale_s=get_calculatefactor_fr('vw_fdmt_is',['party_id','publish_date','act_pubtime','end_date','end_date_rep','REVENUE'],keycol='REVENUE',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=4,givenstocklist=stocklist_df.loc[Dt_all[0]],et=Dt_all[0],singletime=True)

Ac_SQ_Sale=get_calculatefactor_fr('vw_fdmt_is',['party_id','publish_date','act_pubtime','end_date','end_date_rep','REVENUE'],keycol='REVENUE',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=4)

# Ac_SQ_Earning
Ac_SQ_Earning_s=get_calculatefactor_fr('fdmt_main_data_pit',['party_id','publish_date','act_pubtime','end_date','end_date_rep','NI_ATTR_P_CUT'],keycol='NI_ATTR_P_CUT',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=4,givenstocklist=stocklist_df.loc[Dt_all[0]],et=Dt_all[0],singletime=True)

Ac_SQ_Earning=get_calculatefactor_fr('fdmt_main_data_pit',['party_id','publish_date','act_pubtime','end_date','end_date_rep','NI_ATTR_P_CUT'],keycol='NI_ATTR_P_CUT',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=4)

# Ac_SQ_OpEarning 营业利润
Ac_SQ_OpEarning_s=get_calculatefactor_fr('vw_fdmt_is',['party_id','publish_date','act_pubtime','end_date','end_date_rep','OPERATE_PROFIT'],keycol='OPERATE_PROFIT',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=4,givenstocklist=stocklist_df.loc[Dt_all[0]],et=Dt_all[0],singletime=True)

Ac_SQ_OpEarning=get_calculatefactor_fr('vw_fdmt_is',['party_id','publish_date','act_pubtime','end_date','end_date_rep','OPERATE_PROFIT'],keycol='OPERATE_PROFIT',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=4)

# Ac_SQ_OCF 经营活动产生的现金流量净额
Ac_SQ_OCF_s=get_calculatefactor_fr('vw_fdmt_cf',['party_id','publish_date','act_pubtime','end_date','end_date_rep','N_CF_OPERATE_A'],keycol='N_CF_OPERATE_A',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=4,givenstocklist=stocklist_df.loc[Dt_all[0]],et=Dt_all[0],singletime=True)

Ac_SQ_OCF=get_calculatefactor_fr('vw_fdmt_cf',['party_id','publish_date','act_pubtime','end_date','end_date_rep','N_CF_OPERATE_A'],keycol='N_CF_OPERATE_A',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=4)

'''
# =============================================================================
# 因子9 增速边际变化率 #方法二 用单季度表（有错）
# =============================================================================
# Ac_SQ_Sale 
Ac_SQ_Sale=get_calculatefactor_fr('fdmt_is_q_pit',['party_id','publish_date','act_pubtime','end_date','end_date_rep','REVENUE'],keycol='REVENUE',fillmethod='ffill',has_merged_flag=True,modenum=8)

# Ac_SQ_Earning
Ac_SQ_Earning=get_calculatefactor_fr('fdmt_main_data_q_pit',['party_id','publish_date','act_pubtime','end_date','end_date_rep','NI_ATTR_P_CUT'],keycol='NI_ATTR_P_CUT',fillmethod='ffill',has_merged_flag=True,wherecondition=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum=8)

# Ac_SQ_OpEarning
Ac_SQ_OpEarning=get_calculatefactor_fr('fdmt_is_q_pit',['party_id','publish_date','act_pubtime','end_date','end_date_rep','OPERATE_PROFIT'],keycol='OPERATE_PROFIT',fillmethod='ffill',has_merged_flag=True,modenum=8)

# Ac_SQ_OCF 经营活动产生的现金流量净额
Ac_SQ_OCF=get_calculatefactor_fr('fdmt_cf_q_pit',['party_id','publish_date','act_pubtime','end_date','end_date_rep','N_CF_OPERATE_A'],keycol='N_CF_OPERATE_A',fillmethod='ffill',has_merged_flag=True,modenum=8)
'''

# =============================================================================
# 因子13 ROE
# =============================================================================
## ROE_TTM
#方法1:分开做
ROE_TTM1=merge2data(NI_ATTR_P_CUT_TTM,Equity_mean,cmpenddate=True)

#方法2：合在一起做
ROE_TTM_s=get_calculatefactor_fr2('fdmt_main_data_pit',['party_id','publish_date','act_pubtime','end_date','end_date_rep','NI_ATTR_P_CUT'],'vw_fdmt_bs',['party_id','publish_date','act_pubtime','end_date','end_date_rep','T_EQUITY_ATTR_P'],keycol1='NI_ATTR_P_CUT',keycol2='T_EQUITY_ATTR_P',fillmethod='ffill',has_merged_flag1=True,has_merged_flag2=True,wherecondition2=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum1=2,modenum2=2,givenstocklist=stocklist_df.loc[Dt_all[0]],et=Dt_all[0],singletime=True)
ROE_TTM2=get_calculatefactor_fr2('fdmt_main_data_pit',['party_id','publish_date','act_pubtime','end_date','end_date_rep','NI_ATTR_P_CUT'],'vw_fdmt_bs',['party_id','publish_date','act_pubtime','end_date','end_date_rep','T_EQUITY_ATTR_P'],keycol1='NI_ATTR_P_CUT',keycol2='T_EQUITY_ATTR_P',fillmethod='ffill',has_merged_flag1=True,has_merged_flag2=True,wherecondition2=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum1=2,modenum2=2)
# ROE_TTM2.to_csv('ROE_TTM_20200723_method2.csv')
# ROE_TTM2=read_csv_self('ROE_TTM_20200723_method2',index=True,index_col=['trddt','security_id'],dtcolname=['trddt','end_date'],parse_dates=True)

# 核对method1和2的结果是否一致
# xx=pd.merge(ROE_TTM1,ROE_TTM2,left_index=True,right_index=True)
# xx=xx[(xx['value_x'].notnull()&xx['value_y'].notnull()&(round(xx['value_x'],4)!=round(xx['value_y'],4)))]

##ROE_LR
#方法1:分开做
ROE_LR=merge2data(NI_ATTR_P_CUT_LR,Equity_LR,cmpenddate=True)

#核对单期数据
# Equity_LR1_s=get_latestdata_ttm('vw_fdmt_bs','T_EQUITY_ATTR_P',givenstocklist=stocklist_df.loc[Dt_all[0]],et=Dt_all[0],singletime=True)
# Equity_LR2_s=get_calculatefactor_fr('vw_fdmt_bs',['party_id','publish_date','act_pubtime','end_date','end_date_rep','T_EQUITY_ATTR_P'],keycol='T_EQUITY_ATTR_P',fillmethod='ffill',has_merged_flag=True,modenum=0,givenstocklist=stocklist_df.loc[Dt_all[0]],et=Dt_all[0],singletime=True)
# Equity_LR3_s=Equity_LR[Equity_LR.index.get_level_values(0)==Dt_all[0]]

#方法2：合在一起做
ROE_LR=get_calculatefactor_fr2('fdmt_main_data_pit',['party_id','publish_date','act_pubtime','end_date','end_date_rep','NI_ATTR_P_CUT'],'vw_fdmt_bs',['party_id', 'publish_date', 'end_date','T_EQUITY_ATTR_P'],keycol1='NI_ATTR_P_CUT',keycol2='T_EQUITY_ATTR_P',fillmethod='ffill',has_merged_flag1=True,has_merged_flag2=True,wherecondition2=" and ((REPORT_TYPE !='Q3') or (REPORT_TYPE ='Q3' and FISCAL_PERIOD=9)) ",modenum1=1,modenum2=1)

# =============================================================================
# 因子17 OCF 现金流质量
# =============================================================================
# OCF2OP_TTM 经营活动产生的现金流量净额_TTM / 营业利润_TTM
OCF2OP_TTM=merge2data(OCF_TTM,OP_TTM,cmpenddate=True)

# OCF2NetProfit_LR 经营活动产生的现金流量净额_最新年报 / 净利润_最新年报
OCF2NetProfit_LR=merge2data(OCF_LR,NI_ATTR_P_CUT_LR,cmpenddate=True)

# OCFPS_LR 经营活动产生的现金流量净额 / 总股本
OCFPS_LR=merge2data(OCF_LR,TOTAL_SHARES)

# =============================================================================
# 因子19 资产负债率
# =============================================================================
# LTDebt2Equity_LR 非流动负债合计_最新财报 / 股东权益合计_最新财报
LTDebt2Equity_LR=merge2data(T_NCL,Equity_LR,cmpenddate=True)