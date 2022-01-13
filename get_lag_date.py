# -*- coding: utf-8 -*-
# @author: poirot

from importpy import * #导入常用模块
from SQLServer import SQLServer
sql = SQLServer("192.168.99.46","poirot","Aliyun123456","TLSJ")
import sys
sys.path.append(r'C:\Users\admin\Desktop\work\tools\python_fn')

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

def lagTradeDate(dt,num,Dt_trade_long,method='back'):
#1.如果num=0，method='back'，找到的是比dt小的最大交易日日；
#2.如果num=0，method='forward'，找到的是比dt大的最小交易日；
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