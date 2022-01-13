# -*- coding: UTF-8 -*-

#------------------------------------------------------------------------
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
from scipy.optimize import minimize #动态优化

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
sys.path.append(r'C:\Users\admin\Desktop\work\tools\python_fn')
from SQLServer import SQLServer


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
    
def lagTradeDate(dt,num,method='back'):
#1.如果num=0，method='back'，找到的是比dt小的最大工作日；
#2.如果num=0，method='forward'，找到的是比dt大的最小工作日；
#3.如果num=1,method='back'，等同2
    tempdt=pd.Series(Dt_trade_long)
    if method=='back':
        try:
            resultdt=tempdt[tempdt[(tempdt<=dt)].index.max()+num]
        except:
            try:#如果存在比第一个工作日还要早的日期
                resultdt=tempdt[tempdt[(tempdt>dt)].index.min()+num-1]
            except:
                resultdt=np.nan
    else:
        try:
            resultdt=tempdt[tempdt[(tempdt>=dt)].index.min()+num]
        except:
            #如果存在比最后一个工作日还要晚的日期,就忽略掉
            resultdt=np.nan
    return(resultdt)

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

def get_tickerchange():
    db_code = "select SECURITY_ID,value,BEGIN_DATE,END_DATE from md_sec_chg where SEC_INFO_TYPE=0111 and (value like '60%' or value like '68%'  or value like '00%' or value like '30%') order by security_id,BEGIN_DATE"
    result = sql.ExecQuery(db_code)
    result = pd.DataFrame(result, columns = ['security_id','ticker_symbol','begin_date','end_date'])
    result['begin_date']=pd.to_datetime(result['begin_date'],format='%Y-%m-%d') 
    result['end_date']=pd.to_datetime(result['end_date'],format='%Y-%m-%d')
    return result

#获取所有股票的列表
def get_stocklist(field='全部A股',st='',et=''):
    if field in ['科创板','全部A股']:
        typename="like '%科创板%'" if field=='科创板' else "= '全部A股'"        
        db_code = "SELECT distinct(a.security_id), a.sec_short_name, a.ticker_symbol, a.type_name, a.into_date, a.out_date, b.party_id,a.EXCHANGE_CD,b.LIST_DATE " \
            "FROM md_sec_type a left join md_security b on a.security_id = b.security_id " \
                "where a.type_name "+typename+" order by a.ticker_symbol"
        result = sql.ExecQuery(db_code)
        result = pd.DataFrame(result, columns = ['security_id', 'sec_short_name', 'ticker_symbol', 'type_name', 'into_date', 'out_date', 'party_id','exchange','ipo_date'])
    else: #取全部A股但是不包含科创板
        db_code = "SELECT distinct(a.security_id), a.sec_short_name, a.ticker_symbol, a.type_name, a.into_date, a.out_date, b.party_id,a.EXCHANGE_CD,b.LIST_DATE " \
            "FROM md_sec_type a left join md_security b on a.security_id = b.security_id " \
                "where a.type_name = '全部A股' order by a.ticker_symbol"
        result1 = sql.ExecQuery(db_code)
        result1 = pd.DataFrame(result1, columns = ['security_id', 'sec_short_name', 'ticker_symbol', 'type_name', 'into_date', 'out_date', 'party_id','exchange','ipo_date'])

        db_code = "SELECT distinct(a.security_id), a.sec_short_name, a.ticker_symbol, a.type_name, a.into_date, a.out_date, b.party_id,a.EXCHANGE_CD,b.LIST_DATE " \
            "FROM md_sec_type a left join md_security b on a.security_id = b.security_id " \
                "where a.type_name like '%科创板%' order by a.ticker_symbol"
        result2 = sql.ExecQuery(db_code)
        result2 = pd.DataFrame(result2, columns = ['security_id', 'sec_short_name', 'ticker_symbol', 'type_name', 'into_date', 'out_date', 'party_id','exchange','ipo_date'])
        result=result1[np.logical_not(result1.security_id.isin(result2.security_id.tolist()))].reset_index(drop=True)
    
    result['into_date']=pd.to_datetime(result['into_date'],format='%Y-%m-%d') 
    result['out_date']=pd.to_datetime(result['out_date'],format='%Y-%m-%d')
    result['ipo_date']=pd.to_datetime(result['ipo_date'],format='%Y-%m-%d')
    if st!='':
        result=result[((result['out_date'].isnull())|(result['out_date']>=st))]
    if et!='':
        result=result[((result['into_date'].notnull())&(result['into_date']<=et))]
                       
    temp=pd.Series(np.where(result['exchange']=='XSHG','SH',np.where(result['exchange']=='XSHE','SZ',np.nan)),index=result.index)   
    result['Windcode']=result['ticker_symbol']+'.'+temp
    result['TLcode']=result['ticker_symbol']+"."+result['exchange']
    result=result.reset_index(drop=True)
    return result

# datelist=Dt_trade_PD1;givenstocklist=[];freq=period;read=readif;save=saveif
def get_stockprice(datelist,givenstocklist=[],freq='D',read=False,save=False):
    if read==False:
        #获取stocklist的收益
        givenstocklist=stocklist if len(givenstocklist)==0 else givenstocklist
        if freq=='W':
            tablename='mkt_equw_adj'
        elif freq=='M':
            tablename='mkt_equm_adj'
        elif freq=='Q':
            tablename='mkt_equq_adj'
        elif freq=='Y': 
            tablename='mkt_equa_adj'
        else:
            tablename='mkt_equd_adj'
        if freq=='D':
            db_code = "select a.SECURITY_ID,a.TRADE_DATE,a.PRE_CLOSE_PRICE_1,a.OPEN_PRICE_1,a.HIGHEST_PRICE_1,a.LOWEST_PRICE_1,a.CLOSE_PRICE_1,a.TURNOVER_VOL " \
                "from mkt_equd_adj a join mkt_equd_ind b on a.SECURITY_ID=b.SECURITY_ID and a.TRADE_DATE=b.TRADE_DATE " \
                "where b.CHG_STATUS!=-1 and b.EXCHANGE_CD in ('XSHG','XSHE') and " \
                "a.TRADE_DATE>='"+datelist[0].strftime("%Y-%m-%d")+"' and a.TRADE_DATE<='"+datelist[-1].strftime("%Y-%m-%d")+"' " \
                "and a.SECURITY_ID in (" + ','.join(givenstocklist['security_id'].astype(str).tolist()) + ")" \
                "  order by a.TRADE_DATE,a.SECURITY_ID"
        else:
            db_code = "select a.SECURITY_ID,a.END_DATE,a.PRE_CLOSE_PRICE,a.OPEN_PRICE,a.HIGHEST_PRICE,a.LOWEST_PRICE,a.CLOSE_PRICE,a.TURNOVER_VOL " \
                "from "+tablename+" a join mkt_equd_ind b on a.SECURITY_ID=b.SECURITY_ID and a.END_DATE=b.TRADE_DATE " \
                "where b.CHG_STATUS!=-1 and b.EXCHANGE_CD in ('XSHG','XSHE') and " \
                "a.END_DATE>='"+datelist[0].strftime("%Y-%m-%d")+"' and a.END_DATE<='"+datelist[-1].strftime("%Y-%m-%d")+"' " \
                "and a.SECURITY_ID in (" + ','.join(givenstocklist['security_id'].astype(str).tolist()) + ")" \
                "  order by a.END_DATE,a.SECURITY_ID"                
        result = sql.ExecQuery(db_code)
        result = pd.DataFrame(result, columns = ['security_id','trddt','pre_close','open','high','low','close','volume'])
        result['trddt']=pd.to_datetime(result['trddt'],format='%Y-%m-%d') 
        result[['pre_close','open','high','low','close','volume']]=result[['pre_close','open','high','low','close','volume']].apply(lambda x:x.astype('float'))
        result['rt']=result['close']/result['pre_close']-1
        result['ticker_symbol']=stocklist.set_index('security_id').loc[result.security_id,'ticker_symbol'].values
        result=result.sort_values(['trddt','ticker_symbol'])
        result.set_index(['trddt','ticker_symbol'],inplace=True)
    else:
        result=read_csv_self(r'originaldata/stockprice_'+freq+'_'+ED.strftime("%Y%m%d"),index=True,parse_dates=True,dtcolname='trddt',index_col=['trddt','ticker_symbol'],big=True,dtype={'ticker_symbol':'str'})
        result.index.names=['trddt','ticker_symbol']
    if save==True:
        to_csv_self(result,r'originaldata/stockprice_'+freq+'_'+ED.strftime("%Y%m%d")) #默认存表头
    return result

# data=price_all;keycol='close';st=-1;et=0
def get_stockrt(data,keycol,st=-1,et=0):
    data=data.sort_index()
    result=data.groupby(level=1,group_keys=False)[keycol].apply(lambda x:pd.Series(np.where(x.shift(-st)==0,np.nan,x.shift(-et)/x.shift(-st)-1),index=x.index) )
    result=result.unstack()
    return result

def get_stockprice_unadj(datelist,givenstocklist=[],read=False,save=False):
    if read==False:
        #获取stocklist的收益
        givenstocklist=stocklist if len(givenstocklist)==0 else givenstocklist
        db_code = "select a.SECURITY_ID,a.TRADE_DATE,a.ACT_PRE_CLOSE_PRICE,a.OPEN_PRICE,a.HIGHEST_PRICE,a.LOWEST_PRICE,a.CLOSE_PRICE,a.TURNOVER_VOL " \
            "from mkt_equd a join mkt_equd_ind b on a.SECURITY_ID=b.SECURITY_ID and a.TRADE_DATE=b.TRADE_DATE " \
            "where b.CHG_STATUS!=-1 and b.EXCHANGE_CD in ('XSHG','XSHE') and " \
            "a.TRADE_DATE>='"+datelist[0].strftime("%Y-%m-%d")+"' and a.TRADE_DATE<='"+datelist[-1].strftime("%Y-%m-%d")+"' " \
            "and a.SECURITY_ID in (" + ','.join(givenstocklist['security_id'].astype(str).tolist()) + ")" \
            "  order by a.TRADE_DATE,a.SECURITY_ID"
        result = sql.ExecQuery(db_code)
        result = pd.DataFrame(result, columns = ['security_id','trddt','pre_close','open','high','low','close','volume'])
        result['trddt']=pd.to_datetime(result['trddt'],format='%Y-%m-%d') 
        result[['pre_close','open','high','low','close','volume']]=result[['pre_close','open','high','low','close','volume']].apply(lambda x:x.astype('float'))
        result['rt']=result['close']/result['pre_close']-1
        result['ticker_symbol']=stocklist.set_index('security_id').loc[result.security_id,'ticker_symbol'].values
        result=result.sort_values(['trddt','ticker_symbol'])
        result.set_index(['trddt','ticker_symbol'],inplace=True)
    else:
        result=read_csv_self(r'originaldata/stockprice_unadj_'+'_'+ED.strftime("%Y%m%d"),index=True,parse_dates=True,dtcolname='trddt',index_col=['trddt','ticker_symbol'],big=True,dtype={'ticker_symbol':'str'})
        result.index.names=['trddt','ticker_symbol']
    if save==True:
        to_csv_self(result,r'originaldata/stockprice_unadj_'+'_'+ED.strftime("%Y%m%d")) #默认存表头
    return result

def get_pricelimit(datelist,givenstocklist=[],read=False,save=False):    
    if read==False:
        givenstocklist=stocklist if len(givenstocklist)==0 else givenstocklist
        price_limit=get_simpledatafromsql('mkt_limit',['security_id','trade_date','limit_up_price','limit_down_price'],['security_id','trade_date'],givenstocklist=givenstocklist,st=datelist[0],et=datelist[-1],keycol=['limit_up_price','limit_down_price'],usecode='security_id',keydt='trade_date')
        price_limit=price_limit.rename(columns={'trade_date':'trddt'})
        price_limit['ticker_symbol']=stocklist.set_index('security_id').loc[price_limit.security_id,'ticker_symbol'].values
        price_limit=price_limit.sort_values(['trddt','ticker_symbol'])
        price_limit.drop_duplicates(subset=['trddt','ticker_symbol'],keep='last', inplace=True)
        price_limit=price_limit.set_index(['trddt','ticker_symbol'])
    else:
        price_limit=read_csv_self('originaldata/pricelimit'+'_'+ED.strftime("%Y%m%d"),index=True,parse_dates=True,dtcolname='trddt',index_col=['trddt','ticker_symbol'],big=True,dtype={'ticker_symbol':'str'})
        price_limit.index.names=['trddt','ticker_symbol']
    if save==True:
        to_csv_self(price_limit,r'originaldata/pricelimit'+'_'+ED.strftime("%Y%m%d")) #默认存表头
    return price_limit

def get_ST_stock(st='',et=''):
    db_code = "select a.SECURITY_ID,a.VALUE,b.TICKER_SYMBOL,a.BEGIN_DATE,a.END_DATE "\
        "from md_sec_chg a join md_security b on a.SECURITY_ID=b.SECURITY_ID "\
        "where a.SEC_INFO_TYPE='0101' and b.ASSET_CLASS='E' and b.EXCHANGE_CD in ('XSHE','XSHG') and (a.VALUE like '%S%' or a.VALUE like '%退'  or a.VALUE like '退市%') order by ticker_symbol,begin_date"
    result = sql.ExecQuery(db_code) 
    result = pd.DataFrame(result, columns = ['security_id', 'sec_short_name', 'ticker_symbol', 'begin_date', 'end_date'])
    result['begin_date']=pd.to_datetime(result['begin_date'],format='%Y-%m-%d') 
    result['end_date']=pd.to_datetime(result['end_date'],format='%Y-%m-%d')
    return result

#函数1：获取停牌股票的列表
def get_halt_stock(givenstocklist=[],st='',et=''):
    givenstocklist=stocklist if len(givenstocklist)==0 else givenstocklist  
    db_code = "select SECURITY_ID,TICKER_SYMBOL,HALT_BEGIN_TIME,RESUMP_BEGIN_TIME, HALT_PERIOD_DESC "\
        "from md_sec_halt where SECURITY_ID in ("+','.join(givenstocklist['security_id'].astype(str).tolist())+") order by ticker_symbol,HALT_BEGIN_TIME"
    result = sql.ExecQuery(db_code) 
    result = pd.DataFrame(result, columns = ['security_id', 'ticker_symbol', 'begin_time', 'resump_time','period'])
    result['begin_time']=pd.to_datetime(result['begin_time'],format='%Y-%m-%d') 
    result['resump_time']=pd.to_datetime(result['resump_time'],format='%Y-%m-%d')
    return result

#函数2：获取股票交易的状态，收盘涨跌状态:-1-停牌(含暂停上市)，0-平盘，1-上涨(不含涨停)，2-涨停(不含一字涨停)，3-一字涨停，4-下跌(不含跌停)，5-跌停(不含一字跌停)，6-一字跌停
def get_tradestatus(datelist,givenstocklist=[]):
    givenstocklist=stocklist if len(givenstocklist)==0 else givenstocklist
    db_code = "select TRADE_DATE,SECURITY_ID,TICKER_SYMBOL,CHG_STATUS FROM mkt_equd_ind where EXCHANGE_CD in ('XSHG','XSHE') "\
        "and TRADE_DATE in ("+",".join(['\''+x.strftime("%Y-%m-%d")+'\'' for x in datelist])+ ") and SECURITY_ID in ("+','.join(givenstocklist['security_id'].astype(str).tolist())+") order by TRADE_DATE,TICKER_SYMBOL"
    result=sql.ExecQuery(db_code)
    result = pd.DataFrame(result, columns = ['trddt','security_id', 'ticker_symbol', 'status'])
    result['trddt']=pd.to_datetime(result['trddt'],format='%Y-%m-%d') 
    result['ticker_symbol']=stocklist.set_index('security_id').loc[result.security_id,'ticker_symbol'].values
    result=result.set_index(['trddt','ticker_symbol'])
    result=result['status']
    return result

#获取行业的type_id,type_name
def get_indu_table(st,induname='申万',industry_level=1):
    indu_id=get_indu_type_id(induname)        
    db_code = "select TYPE_ID,TYPE_NAME,BEGIN_DATE,END_DATE,INDUSTRY "\
        "from md_type where TYPE_ID like '"+indu_id+"%' and INDUSTRY_LEVEL="+str(industry_level)+" and "\
        "(end_date IS NULL or end_date>='"+st.strftime("%Y-%m-%d")+"') order by type_id,BEGIN_DATE"    
    result=sql.ExecQuery(db_code)
    result=pd.DataFrame(result,columns=['type_id','type_name','begin_date','end_date','industry'])
    result['begin_date']=pd.to_datetime(result['begin_date'],format='%Y-%m-%d') 
    result['end_date']=pd.to_datetime(result['end_date'],format='%Y-%m-%d')
    return result

#获取所有股票的行业
def get_stockindu(st,induname='申万',industry_level=1):
    indu_id=get_indu_type_id(induname)  
    if industry_level==1:
        type_level="left(a.TYPE_ID,8)"
    elif industry_level==2:
        type_level="left(a.TYPE_ID,10)"
    else:#3级行业分类
        type_level="a.TYPE_ID"
    db_code = "select a.PARTY_ID,"+type_level+",b.type_name,a.INTO_DATE,a.OUT_DATE "\
        "from md_inst_type a join md_type b on "+type_level+"=b.TYPE_ID "\
        "where a.TYPE_ID like '"+indu_id+"%' and "\
        "(a.out_date IS NULL or a.out_date>='"+st.strftime("%Y-%m-%d")+"') order by a.PARTY_ID,a.INTO_DATE"
    result = sql.ExecQuery(db_code)
    result = pd.DataFrame(result, columns = ['party_id', 'type_id',  'type_name', 'into_date', 'out_date'])
    # result['start_date']=result.groupby('party_id',group_keys=False).apply(lambda x:pd.Series(np.where(((x['out_date'].shift(1)==x['into_date'])&(x['type_id']==x['type_id'].shift(1))),x['into_date'].shift(1),np.nan),index=x.index))
    # result=result[result['start_date'].notnull()|((result['out_date']!=result['into_date'].shift(-1))|(result['type_id']!=result['type_id'].shift(-1)))]
    result['into_date']=pd.to_datetime(result['into_date'],format='%Y-%m-%d') 
    result['out_date']=pd.to_datetime(result['out_date'],format='%Y-%m-%d')
    return result

# #取指数收益from wind 
def get_benchrt(benchname,datelist,read=False,save=False):
    if read==False:
        error,result=w.wsd(get_benchSecuCode(benchname), "open,high,low,close,pre_close,pct_chg", datelist[0].strftime("%Y-%m-%d"),datelist[-1].strftime("%Y-%m-%d"), "PriceAdj=F",usedf=True)
        result.columns=['open','high','low','close','pre_close','pct_chg']
        result.index=pd.to_datetime(result.index,format='%Y-%m-%d')  
        result.index.name='trddt'
    else:
        result=read_csv_self('originaldata/benchrt_'+re.sub("\D", "", benchname)+'_'+ED.strftime("%Y%m%d"),index=True,parse_dates=True,dtcolname='trddt',index_col=['trddt'])
        result=result.reindex(datelist)
        result.index.name='trddt'
    if save==True:
        to_csv_self(result,r'originaldata/benchrt_'+re.sub("\D", "", benchname)+'_'+ED.strftime("%Y%m%d")) #默认存表头
    result['pct_chg']=result['pct_chg']/100
    return result

# #取指数收益from 朝阳永续
def get_benchrt2(benchname,datelist):
    result=get_simpledatafromsql('[ZYYX].[dbo].[qt_idx_daily]',['trade_date','topen','high','low','tclose','lclose','change_rate'],['trade_date'],st=datelist[0],et=datelist[-1],keycol=['topen','high','low','tclose','lclose','change_rate'],usecode='',keydt='trade_date',wherecondition=" and index_code='"+re.sub("\D", "", get_benchSecuCode(benchname))+"'")
    result=result.rename(columns={'trade_date':'trddt','topen':'open','tclose':'close','lclose':'pre_close','change_rate':'pct_chg'})
    result=result.set_index('trddt')     
    result['pct_chg']=result['pct_chg']/100
    return result

# benchname='中证500';datelist_M=Dt_trade_M_PD1; datelist=Dt_all
#取指数的成分权重from wind，月底最后一个交易日更新
def get_benchcompoandw(benchname,datelist_M,datelist,read=False,save=False):
    if read==False:
        benchcode=get_benchSecuCode(benchname)
        result=pd.DataFrame()
        for idate in tqdm(datelist_M):
            errorCode,tempdata=w.wset("indexconstituent","date="+idate.strftime("%Y-%m-%d")+";windcode="+benchcode+";field=date,wind_code,sec_name,i_weight,industry",usedf=True)               
            result=pd.concat([result,tempdata],axis=0,join='outer')            
        result.columns=['trddt','SecuCode','SecuName','weight','indu_name']
        result.trddt=pd.to_datetime(result.trddt,format='%Y-%m-%d')
        result.weight=result.weight/100
        result['ticker_symbol']=[re.sub("\D", "", x) for x in result['SecuCode']]
    else:
        result=read_csv_self('originaldata/stock_bench_'+re.sub("\D", "", benchname)+'_'+ED.strftime("%Y%m%d"),dtype={'ticker_symbol':'str'},parse_dates=True,dtcolname='trddt')
    if save==True:
        to_csv_self(result,r'originaldata/stock_bench_'+re.sub("\D", "", benchname)+'_'+ED.strftime("%Y%m%d"),savecol=False) #默认存表头
    # result=result.set_index('trddt').groupby('ticker_symbol',group_keys=False).apply(lambda x: x.reindex(datelist,method='ffill')).reset_index().rename(columns={'index':'trddt'})
    # result=result.sort_values(['trddt','ticker_symbol'])
    result.set_index(['trddt','ticker_symbol'],inplace=True)
    result['weight']=result['weight'].astype('float')
    return result

#取指数成分股 from choice
def get_benchcompoandw2(benchname,datelist): 
    tablename='[FactorDB].[dbo].[INDEX_COMP_WT_'+re.sub("\D", "", get_benchSecuCode(benchname))+']'
    result=get_simpledatafromsql(tablename,['Trddt','Windcode','Weight'],['Trddt','Windcode'],st=datelist[0],et=datelist[-1],keycol=['Weight'],usecode='',keydt='Trddt')
    result['ticker_symbol']=[re.sub("\D", "", x) for x in result['Windcode']]
    result=result.rename(columns={'Trddt':'trddt','Weight':'weight'})
    result['trddt']=pd.to_datetime(result['trddt'],format='%Y-%m-%d') 
    result.set_index(['trddt','ticker_symbol'],inplace=True)
    return result

def get_simpledatafromsql(tablename,indicator,orderindicator,givenstocklist=[],st='',et='',keycol='',usecode='stock_code',keydt='publish_date',dtcol=[],fillmethod='na',has_merged_flag=False,wherecondition='',singletime=False,usetradedt=False,mergedate='',shifttime=0):
    indicator=indicator if isinstance(indicator,list) else indicator.split(',')
    orderindicator=orderindicator if isinstance(orderindicator,list) else orderindicator.split(',')
    db_code = "select "+','.join(indicator)+" from "+tablename+ " where 1=1 "
    
    if (singletime==True)&(fillmethod=='ffill')&(usetradedt==True):        
        originalet=et
        et=lagTradeDate(et,0,method='back')
        st=lagTradeDate(st,0,method='back')
    
    if usecode!='':
        givenstocklist=stocklist if len(givenstocklist)==0 else givenstocklist
        usecode_transfer='ticker_symbol' if usecode=='stock_code' else usecode
        stockcondition=','.join(givenstocklist[usecode_transfer].astype(str).tolist()) if usecode!='ticker_symbol' else ','.join(["'"+str(x)+"'" for x in givenstocklist[usecode_transfer].tolist()])        
        db_code = db_code + " and " + usecode+" in (" + stockcondition + ") "
    
    #时间筛选
    #早上9点前都算是前一天
    if et!='':
        et_plus=et+timedelta(hours=23+shifttime, minutes=59, seconds=59) if keydt in ['act_pubtime','entrytime'] else et+timedelta(hours=23, minutes=59, seconds=59)
    if (st!='')&(et==''):
        db_code = db_code+ " and "+keydt+">='"+st.strftime("%Y-%m-%d")+"' "
    elif (st=='')&(et!=''):
        db_code = db_code+ " and "+keydt+"<='"+et_plus.strftime("%Y-%m-%d")+"' "
    elif (st!='')&(et!=''):
        db_code = db_code+ " and "+keydt+">='"+st.strftime("%Y-%m-%d")+ "' and "+keydt+"<='"+et_plus.strftime("%Y-%m-%d")+"' "
    
    #其他条件
    if has_merged_flag==True:
        db_code = db_code+ " and merged_flag=1 "
    if len(wherecondition)>0:
        db_code = db_code+ wherecondition
    db_code = db_code+ " order by "+','.join(orderindicator)
    result = sql.ExecQuery(db_code)      
    result = pd.DataFrame(result, columns = indicator)
    
    #调整时间格式和数字格式
    dtcol=dtcol if len(dtcol)>0 else [x for x in indicator if ('date' in x.lower())|('time' in x.lower())]
    if len(dtcol)>0:
        for i in dtcol:
            try:
                result[i]=pd.to_datetime(result[i],format='%Y-%m-%d') 
            except:
                result[i]=pd.to_datetime(result[i],format='%Y%m%d')
    if keycol!='':
        if isinstance(keycol,list):
            for i in keycol:
                result[i]=result[i].astype('float')
        else:       
            result[keycol]=result[keycol].astype('float')
        
    if usecode=='stock_code':
        result=result.rename(columns={'stock_code':'ticker_symbol'})
    
    if (singletime==True)&(fillmethod=='ffill')&(usetradedt==True):      
        et=originalet
    
    return result

def testgroup(data_,iname,title,drop0=False,groupdataif=True,groupallif=True,feetest=0,shiftdtif=False,filterweight=True,plotindu=False,inbenchif=False,interval=1,testindu=False,excess_rt_if=True,accurateif=False,keycol_price='open'):
    #图片的名字
    title=title+'_'+benchfield+'内选股' if inbenchif==True else title
    title=title+'_全市场分组' if groupallif==True else title+'_行业内分组'
    title=title+'_interval='+str(interval)
    title=title+'减去基准' if excess_rt_if==True else title+'未减基准'
    
    data=data_.copy()
    
    #判断是否在基准内
    if inbenchif==True:
        data=data[data['w_b']>0]
    
    #分组
    if groupdataif==True:
        if groupallif==True:
            data['group']=data.groupby(level=0)[iname].apply(lambda x:cut_group(x,groupnum,drop0=drop0))
        else:
            data['group']=data.groupby(['trddt','indu_name'])[iname].apply(lambda x:cut_group(x,groupnum,drop0=drop0))
    else:
        # data['group']=np.where(data[iname]>0,1,0)
        data['group']=data[iname]
    
    #统计每组个数
    stock_select_num=data.groupby(['trddt','group'])['security_id'].count().unstack()
    stock_select_num_indu=data.groupby(['trddt','indu_name','group'], group_keys = False)['security_id'].count().unstack()
    groupnumlist=stock_select_num.columns
        
    #根据每组个数，计算等权权重
    data['weight_market']=1/data.groupby(['trddt','group'])['security_id'].transform("count")
    data['weight_indu']=1/data.groupby(['trddt','indu_name','group'])['security_id'].transform("count")
    # data['weight_indu']=data['weight_market']/data.groupby(['trddt','indu_name','group'])['weight_market'].transform("sum")

    #全市场等权分组表现
    r_market=data.groupby(['group'])['weight_market'].apply(lambda x:get_dayreturn(price_all,x.unstack(),feetest,shiftdtif=shiftdtif,filterweight=filterweight,interval=interval,accurateif=accurateif,keycol_price=keycol_price)[0])
    r_market=r_market.unstack(0)
    if excess_rt_if==True:#是否减去基准收益率
        r_market=r_market.sub(rt_bench_next1,axis=0) if (keycol_price=='open')&(accurateif==False) else r_market.sub(rt_bench,axis=0)  #只有在非精确法下，并且用open作为买入价格，才会减去指数的oto
    
    if not isinstance(groupnumlist[0],str):
        r_market['LS']=r_market[max(r_market.dropna(how='all',axis=1).columns)]-r_market[min(r_market.dropna(how='all',axis=1).columns)]    
        # r_market['LS']=r_market[max(groupnumlist)]-r_market[min(groupnumlist)]
    stat_r_market = r_market.dropna(how='all',axis=1).apply(lambda x: get_statofreturn(x,reversemmd=True))
    net_value=r_market.apply(lambda x:(1+x).cumprod())
    net_value.plot(title=title)
    plt.show()
    
    if testindu==True:
        #分行业等权分组表现            
        r_indu=data.groupby(['group'])['weight_indu'].progress_apply(lambda x:get_dayreturn(price_all,x.unstack(),feetest,shiftdtif=shiftdtif,filterweight=filterweight,induif=True,interval=interval,accurateif=accurateif,keycol_price=keycol_price)[0])
        r_indu=r_indu.swaplevel(0,1).unstack(2)    
        if excess_rt_if==True:#是否减去基准收益率
            r_indu=r_indu.sub(rt_indu_bench_next1,axis=0) if (keycol_price=='open')&(accurateif==False) else r_indu.sub(rt_indu_bench,axis=0)            
        r_indu=r_indu.stack().unstack('group') 
        
        if not isinstance(groupnumlist[0],str):
            r_indu['LS']=r_indu.groupby('indu_name',group_keys=False).apply(lambda x:x[max(x.dropna(how='all',axis=1).columns)]-x[min(x.dropna(how='all',axis=1).columns)])
            # r_indu['LS']=r_indu[max(groupnumlist)]-r_indu[min(groupnumlist)]
        r_indu=r_indu.dropna(axis=1,how='all')             
        stat_r_indu = r_indu.reset_index().set_index('trddt').groupby('indu_name').apply(lambda xx:xx.drop(['indu_name'],axis=1).dropna(how='all',axis=1).apply(lambda x:get_statofreturn(x,reversemmd=True)))       
        stat_r_indu=stat_r_indu.stack().unstack(0)
        
        if plotindu==True:
            net_value = r_indu.groupby(level=1).apply(lambda x:(1+x).cumprod())
            net_value = net_value.stack().unstack(1)
            for iindu in net_value.columns:
                if any(np.abs(stat_r_indu[iindu].loc[('sharpe',slice(None))].iloc[[0,-2,-1]])>1):    
                    net_value[iindu].unstack().plot(title=title+'_分行业_'+iindu)
                    plt.show() 
            
        return stock_select_num,stock_select_num_indu,stat_r_market,stat_r_indu,r_market,r_indu
    
    else:
        return stock_select_num,stock_select_num_indu,stat_r_market,[],r_market,[]

def get_filterweight(pos,shiftdtif=False,filterweight=True,interval=1,firstorlast='last'):
    pos=pos.fillna(0)
    pos['trddt_new']=(Dt_all_trade_forward1.reindex(pos.index)).values if shiftdtif==True else (Dt_all_trade_forward0.reindex(pos.index)).values
    pos.drop_duplicates(subset='trddt_new',keep='last',inplace=True)
    pos=pos.reset_index(drop=True).rename(columns={'trddt_new':'trddt'}).set_index('trddt')
    pos=pos[pos.index.notnull()]
    #隔几天换仓，接收n周，n月，n季度，n年
    if (isinstance(interval,str)):
        if (any([x in interval for x in ['D','W','M','Q','Y']])):
            interval=re.sub("\D", "", interval)+periodtofreq(re.findall(r'\D+', interval)[0])
            if firstorlast=='last':
                pos=(pos.groupby(pos.to_period(interval).index).tail(1)).reindex(pos.index,method='ffill')
            else:
                pos=(pos.groupby(pos.to_period(interval).index).head(1)).reindex(pos.index,method='ffill')
    elif isinstance(interval,int):
        pos=(pos.reindex(Dt_trade[::interval])).reindex(pos.index,method='ffill')
    else:
        pass
    if filterweight==True:
        pos0=pd.DataFrame(0,columns=pos.columns,index=[Dt_trade_PD[Dt_trade_PD.tolist().index(pos.index[0])-1]])
        pos0=pos0.append(pos)
        hasif=(stocklist_df['IPOdays']>=0).unstack()[pos.columns].fillna(False)
        haltif=stocklist_df['halt'].unstack()[pos.columns].fillna(False)
        ztif=stocklist_df['zt'].unstack()[pos.columns].fillna(False)
        dtif=stocklist_df['dt'].unstack()[pos.columns].fillna(False)
        for i in range(1,len(pos0.index)):
            iindex=pos0.index[i]
            #判断是否有，如果没上市则设为0
            pos0.iloc[i]=np.where(hasif.loc[iindex]==True,pos0.iloc[i],0)
            #如果增持，但是涨停或者停牌，则不能买入
            pos0.iloc[i]=np.where((pos0.iloc[i]>pos0.iloc[i-1])&((haltif.loc[iindex]==True)|(ztif.loc[iindex]==True)),pos0.iloc[i-1],pos0.iloc[i])
            #如果减持，但是跌停或者停牌，则不能卖出
            pos0.iloc[i]=np.where((pos0.iloc[i]<pos0.iloc[i-1])&((haltif.loc[iindex]==True)|(dtif.loc[iindex]==True)),pos0.iloc[i-1],pos0.iloc[i])
        pos=pos0.loc[pos.index]
    return pos

# Y_price=price_all;pos=weight_df.unstack();shiftdtif=shiftdtif;filterweight=filterweight;induif=False;interval=1;firstorlast='last'
#原始open买卖，或者单一价格买卖，并且不rebalance
def get_dayreturn(Y_price,pos,feetest,shiftdtif=False,filterweight=False,induif=False,interval=1,firstorlast='last',accurateif=False,keycol_price='open'):
    #先进行w的处理
    pos=pos.sort_index(axis=1)
    pos=get_filterweight(pos,shiftdtif=shiftdtif,filterweight=filterweight,interval=interval,firstorlast=firstorlast)
    pos0=pd.DataFrame(0,columns=pos.columns,index=[Dt_trade_PD[Dt_trade_PD.tolist().index(pos.index[0])-1]])
    pos0=pos0.append(pos)
    w_0=pos0.shift(1).loc[pos.index]
    w_1=pos0.loc[pos.index]
    w_delta=w_1-w_0
    turnover=w_delta.abs().sum(axis=1)
    shouxu=w_delta.abs()*feetest
    Y_price=Y_price.loc[pos.index]
    #accurateif=True：精确算法，根据买卖价格计算收益
    #accurateif=False：粗糙算法，举证乘法，rt*w1
    if accurateif==True: 
        w_weibian=w_1.where(w_delta<=0,w_0)
        result=w_weibian*Y_price['rt'].unstack()+w_delta.where(w_delta>0,0)*(Y_price['close']/Y_price[keycol_price]-1).unstack()+(-w_delta.where(w_delta<0,0))*(Y_price[keycol_price]/Y_price['pre_close']-1).unstack()-shouxu
    else:
        if keycol_price=='open':#open(0) to open(1) 
            result=Y_price['rt_next'].unstack()*w_1-shouxu
        else: #close(-1) to close(0)
            result=Y_price['rt'].unstack()*w_1-shouxu
    result=result.loc[:result.dropna(how='all',axis=0).index[-1]]
    #如果分行业，按照行业加总求和
    if induif==True:
        result=pd.DataFrame(result.stack(),columns=['rt'])
        result['indu_name']=stocklist_df['indu_name']
        result=result.groupby(['trddt','indu_name'])['rt'].sum()
    else:
        result=result.sum(axis=1)
    return result,turnover

def standardreturn(period):
    switcher = {'Y': 1,'Q': 4, 'M': 12,'W':52,'D':252}
    return switcher.get(period, "nothing")

def max_drawdown(df):
    md=((df.cummax()-df)/df.cummax()).max()
    return round(md,4)

def get_statofreturn(r,cummethod='cumprod',reversemmd=False):
    r=r.loc[r.dropna().index[0]:r.dropna().index[-1]]
    combine=pd.Series()
    if cummethod=='cumsum':
        net_value=r.cumsum()+1
    else:
        net_value=(r+1).cumprod()
    combine['net_value']=net_value.dropna()[-1]
    # combine['年化收益']=round(r.mean()*standardreturn(period),4)
    combine['年化收益']=round(net_value.dropna().iloc[-1]**(standardreturn(period)/len(r))-1,4) #复合年化收益
    combine['年化标准差']=round(r.std()*np.sqrt(standardreturn(period)),4)
    if reversemmd==True:
        combine['最大回撤']=round(max_drawdown(net_value),4) if net_value.dropna().iloc[-1]>1 else round(max_drawdown((-r+1).cumprod()),4)
    else:
        combine['最大回撤']=round(max_drawdown(net_value),4) 
    combine['sharpe']=round(combine['年化收益']/combine['年化标准差'],4)
    combine['Calmar']=round(combine['年化收益']/combine['最大回撤'],4)
    combine['胜率']=(r>0).sum()/((r!=0)&(r.notnull())).sum()
    r_week=r.groupby(r.to_period('W').index,group_keys= False).apply(lambda x:((x+1).cumprod()-1).iloc[-1])
    combine['周胜率']=(r_week>0).sum()/((r_week!=0)&(r_week.notnull())).sum()
    r_month=r.groupby(r.to_period('M').index,group_keys= False).apply(lambda x:((x+1).cumprod()-1).iloc[-1])
    combine['月胜率']=(r_month>0).sum()/((r_month!=0)&(r_month.notnull())).sum()  
    # combine['年均交易次数(单)']=round(((r!=0)&(r.notnull())).sum()/((r.index[-1]-r.index[0]).days/365),0)
    return combine

def get_reindexdata(result,keycol,keydt='trddt',fillmethod='na',fill_value=0,mergedate=''):    
    result=result.reset_index()
    result=result.rename(columns={keydt:'trddt'}) if keydt!='trddt' else result
    
    if len(result)>0:
        result=result.drop_duplicates(['ticker_symbol','trddt'],keep='last', inplace=False) 
    else:
        print('merge failed because data is null')
        return result
    
    result=result[result['trddt'].notnull()]
    result=result.set_index('trddt')
    keycol=[keycol] if isinstance(keycol,str) else keycol
    mergedate=Dt_all if len(mergedate)==0 else mergedate
    
    if fillmethod=='ffill':
        result=result.groupby('ticker_symbol')[keycol].apply(lambda x:x.reindex(mergedate,method='ffill'))
    elif fillmethod=='fill0':
        result=result.groupby('ticker_symbol')[keycol].apply(lambda x:x.reindex(mergedate,fill_value=0))
    elif fillmethod=='bfill':
        result=result.groupby('ticker_symbol')[keycol].apply(lambda x:x.reindex(mergedate,method='bfill'))
    elif fillmethod=='fill_value':
        result=result.groupby('ticker_symbol')[keycol].apply(lambda x:x.reindex(mergedate,fill_value=fill_value))
    else:
        result=result.groupby('ticker_symbol')[keycol].apply(lambda x:x.reindex(mergedate))
    result.index.names= ['ticker_symbol', 'trddt']
           
    result=result.reset_index().sort_values(['trddt','ticker_symbol']).set_index(['trddt','ticker_symbol'])[keycol]    
    
    #如果是单个返回值
    if (isinstance(result,pd.Series)):
        result.name='value'
    elif (len(result.columns)==1):
        result.columns=['value']
    
    if fillmethod=='fill0':
        result=result.fillna(0)
    
    return result

#####################################################################################################################
#   正文
#####################################################################################################################
path=r'C:\Users\admin\Desktop\work\temp_jobs\factor_tester'
os.chdir(path)
sql = SQLServer("202.101.23.166","poirot","Aliyun123456","TLSJ")

w.start();
w.isconnected()

readif=False
saveif=True
readif=True
saveif=False

period='D'
groupnum=5
fee=0.003
benchfield='中证500'
accurateif=False
keycol_price='open'
halt_st=timedelta(hours=9,minutes=30);halt_et=timedelta(hours=15)
# =============================================================================
# #：取时间和stocklist
# =============================================================================
SD=datetime.strptime("2017-01-01",'%Y-%m-%d')
# ED=datetime.strptime(str(date.today()),'%Y-%m-%d')
ED=datetime.strptime("2020-11-19",'%Y-%m-%d')
PD=datetime.strptime("2000-01-01",'%Y-%m-%d') #数据多提取10年，用于计算过去5年
PD1=datetime.strptime("2016-01-01",'%Y-%m-%d') #数据多提取1年，用于计算过去5年

#取时间
Dt_all= get_date(SD,ED,'D',istradeday=False)
Dt_trade = get_date(SD,ED,'D')
Dt_all_PD = get_date(PD,ED,'D',istradeday=False)
Dt_trade_PD = get_date(PD,ED,'D')
Dt_all_PD1 = get_date(PD1,ED,'D',istradeday=False)
Dt_trade_PD1 = get_date(PD1,ED,'D')
Dt_trade_M_PD1= get_date(PD1,ED,'M')

Dt_trade_long=get_date(PD-timedelta(30),ED+timedelta(30),'D') #前面后面都延长，保证下面两个函数有值
Dt_trade_previousday=pd.Series([Dt_all_PD[Dt_all_PD.tolist().index(x)-1] for x in Dt_trade],Dt_trade)
Dt_all_trade_forward1=pd.Series(pd.DatetimeIndex([lagTradeDate(x,1,'back') for x in Dt_all_PD]),index=Dt_all_PD)
Dt_all_trade_forward0=pd.Series(pd.DatetimeIndex([lagTradeDate(x,0,'forward') for x in Dt_all_PD]),index=Dt_all_PD)

#取ticker变更记录
tickerchange=get_tickerchange()

#取全部A股（不包括科创板）,包括未上市的
stocklist=get_stocklist(field='全部A股',st=PD1,et=ED)
# stocklist=get_stocklist(field='全部A股without科创板',st=PD1,et=ED)
#全部stocklist的价格和收益率
price_all=get_stockprice(Dt_trade_PD1,freq='D',read=readif,save=saveif)
price_all['rt_next']=price_all.groupby(level=1,group_keys=False).apply(lambda x:x['open'].shift(-1)/x['open']-1)
#全部stocklist的未复权价格
price_all_unadj=get_stockprice_unadj(Dt_trade_PD1,read=readif,save=saveif)

#取ST的股票
stock_ST=get_ST_stock()
#取股票状态
stock_status=get_tradestatus(Dt_trade_PD1)
#取停牌超过一天的股票
stock_halt=get_halt_stock()

#取股票涨跌的价格，判断是否以keycol_price价格买卖是否能够实现
price_limit=get_pricelimit(Dt_trade,read=readif,save=saveif)

#基准的收益率 
price_bench=get_benchrt(benchfield,Dt_trade,read=readif,save=saveif) #from wind
# price_bench=get_benchrt2(benchfield,Dt_trade) #from zyyx
rt_bench=price_bench['close']/price_bench['pre_close']-1
rt_bench_next1=price_bench['open'].shift(-1)/price_bench['open']-1
ibench=rt_bench_next1 if (keycol_price=='open')&(accurateif==False) else rt_bench #只有在非精确法下，并且用open作为买入价格，才会减去指数的oto

#基准的成分和权重
stock_bench=get_benchcompoandw(benchfield,Dt_trade_M_PD1,Dt_all,read=readif,save=saveif) #from wind
# stock_bench=get_benchcompoandw2(benchfield,Dt_trade) #from choice
datelist_benchcmp=stock_bench.index.get_level_values(0).unique()

#申万行业的table
indu_table=get_indu_table(PD1,induname='申万',industry_level=1)
# to_csv_self(indu_table,'originaldata/indu_table')
#个股的申万一级行业分类
stock_indu=get_stockindu(PD1,induname='申万',industry_level=1)

#所有交易日股票集合，增加了ST，w_b，行业
stocklist_df = OrderedDict()
for idate in tqdm(Dt_trade):
    temp=stocklist[ (stocklist['into_date'].notnull()) & (stocklist['into_date'] <= idate)\
                            & ((stocklist['out_date'].isnull()) | (stocklist['out_date']>idate))][['security_id','party_id','ticker_symbol']].reset_index(drop=True)

    temp=temp.sort_values('ticker_symbol')
    temp['IPOdays']=(idate-pd.DatetimeIndex(stocklist.set_index('security_id').loc[temp.security_id,'ipo_date'])).days.tolist()
    temp['ST']=temp.security_id.isin(stock_ST['security_id'][(stock_ST.begin_date<= idate)&((stock_ST.end_date>=idate)|(stock_ST.end_date.isnull()))].tolist())
    temp['halt']=temp.security_id.isin(stock_halt['security_id'][(stock_halt.begin_time<= (idate+halt_st))&((stock_halt.resump_time>(idate+halt_et))|(stock_halt.resump_time.isnull()))].tolist()) #停牌精确到小时
    temp['w_b']=pd.merge(temp[['ticker_symbol']],stock_bench.loc[datelist_benchcmp[datelist_benchcmp<=idate].max()]['weight'].reset_index(),how="left",on='ticker_symbol')['weight'].fillna(0)
    temp['indu']=stock_indu[(stock_indu.into_date<= idate)&((stock_indu.out_date>idate)|(stock_indu.out_date.isnull()))].drop_duplicates(subset='party_id',keep='first', inplace=False).set_index('party_id').reindex(temp['party_id'])['type_id'].tolist()    
    temp['indu_name']=stock_indu[(stock_indu.into_date<= idate)&((stock_indu.out_date>idate)|(stock_indu.out_date.isnull()))].drop_duplicates(subset='party_id',keep='first', inplace=False).set_index('party_id').reindex(temp['party_id'])['type_name'].tolist()     
    temp=temp.set_index('ticker_symbol')
    stocklist_df[idate]=temp
stocklist_df=pd.concat(stocklist_df.values(), keys=stocklist_df.keys())
stocklist_df.index.names = ['trddt', 'ticker_symbol']
stocklist_df[['pre_close','open','high','low','close','rt','rt_next']]=price_all[['pre_close','open','high','low','close','rt','rt_next']]
stocklist_df['zt']=((stock_status.reindex(stocklist_df.index)==3)|(price_limit.reindex(stocklist_df.index)['limit_up_price']==price_all_unadj.reindex(stocklist_df.index)[keycol_price]))
stocklist_df['dt']=((stock_status.reindex(stocklist_df.index)==6)|(price_limit.reindex(stocklist_df.index)['limit_down_price']==price_all_unadj.reindex(stocklist_df.index)[keycol_price]))

#行业rt_以bench的行业为基准 昨收到今收的收益率
rt_indu_bench=stocklist_df.groupby(level=0,group_keys=False).apply(lambda xx:xx.groupby('indu_name',group_keys=False).apply(lambda x:(x['w_b']*x['rt']).sum()/x['w_b'].sum()))
rt_indu_bench_next1=stocklist_df.groupby(level=0,group_keys=False).apply(lambda xx:xx.groupby('indu_name',group_keys=False).apply(lambda x:(x['w_b']*x['rt_next']).sum()/x['w_b'].sum()))
ibench_indu=rt_indu_bench_next1 if (keycol_price=='open')&(accurateif==False) else rt_indu_bench

#存放因子的df
factor_df = OrderedDict()
for idate in Dt_all:
    temp=stocklist[ (stocklist['into_date'].notnull()) & (stocklist['into_date'] <= idate)\
                            & ((stocklist['out_date'].isnull()) | (stocklist['out_date']>idate))][['security_id','party_id','ticker_symbol']].reset_index(drop=True)
    temp=temp.sort_values('ticker_symbol')
    temp['IPOdays']=(idate-pd.DatetimeIndex(stocklist.set_index('security_id').loc[temp.security_id,'ipo_date'])).days.tolist()
    temp['ST']=temp.security_id.isin(stock_ST['security_id'][(stock_ST.begin_date<= idate)&((stock_ST.end_date>idate)|(stock_ST.end_date.isnull()))].tolist())
    temp['halt']=temp.security_id.isin(stock_halt['security_id'][(stock_halt.begin_time<= (idate+halt_st))&((stock_halt.resump_time>(idate+halt_et))|(stock_halt.resump_time.isnull()))].tolist()) #停牌精确到小时
    temp['w_b']=pd.merge(temp[['ticker_symbol']],stock_bench.loc[datelist_benchcmp[datelist_benchcmp<=idate].max()]['weight'].reset_index(),how="left",on='ticker_symbol')['weight'].fillna(0)
    temp['indu']=stock_indu[(stock_indu.into_date<= idate)&((stock_indu.out_date>idate)|(stock_indu.out_date.isnull()))].drop_duplicates(subset='party_id',keep='first', inplace=False).set_index('party_id').reindex(temp['party_id'])['type_id'].tolist()    
    temp['indu_name']=stock_indu[(stock_indu.into_date<= idate)&((stock_indu.out_date>idate)|(stock_indu.out_date.isnull()))].drop_duplicates(subset='party_id',keep='first', inplace=False).set_index('party_id').reindex(temp['party_id'])['type_name'].tolist()     

    factor_df[idate]=temp.set_index('ticker_symbol')
factor_df=pd.concat(factor_df.values(), keys=factor_df.keys())
factor_df.index.names = ['trddt', 'ticker_symbol']
# factor_df=factor_df[(factor_df['ST']==False)]

# =============================================================================
# 因子测试
# =============================================================================
# factor_df['factor1']=read_csv_self(r"E:/盈科/因子库/factor/sentimentR8_20201119",index=True,index_col=['trddt','ticker_symbol'],dtcolname=['trddt'],parse_dates=True,dtype={'ticker_symbol':'str'})['value']

# stock_select_num,stock_select_num_indu,stat_r_market,stat_r_indu,r_market,r_indu=testgroup(factor_df,'factor1','情绪因子',drop0=False,accurateif=False,groupallif=True,groupdataif=True,feetest=0,shiftdtif=True,filterweight=False)
#写因子
Momentum_len=5
temp=price_all.groupby(level=1,group_keys=False)['close'].apply(lambda x:x/x.shift(Momentum_len)-1)
factor_df['factor1']=get_reindexdata(temp,temp.name,keydt='trddt',fillmethod='ffill')['value']   
# stock_select_num,stock_select_num_indu,stat_r_market,stat_r_indu,r_market,r_indu=testgroup(factor_df,'factor1','动量',drop0=True,accurateif=False,groupallif=True,groupdataif=True,feetest=0,shiftdtif=True,filterweight=False)
stock_select_num,stock_select_num_indu,stat_r_market,stat_r_indu,r_market,r_indu=testgroup(factor_df,'factor1','动量'+str(Momentum_len),drop0=False,accurateif=True,groupallif=True,groupdataif=True,feetest=0,shiftdtif=True,filterweight=False)
