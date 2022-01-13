# -*- coding: utf-8 -*-
"""
Created on Thu Aug  6 16:08:30 2020

@author: admin
"""
#自己的模块
import sys #添加路径，导入模块
sys.path.append(r'F:\python_fn')
sys.path.append(r'C:\Users\admin\Desktop\work\tools\python_fn')
from SQLServer import SQLServer
from importpy import * #导入常用模块
from Toolfunction import *  #tool函数
 
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
        #st转换到freq初，et转换到freq底
        if freq=='W':
            st=st+timedelta(-st.weekday()) if st.weekday()!=0 else st
            et=et+timedelta(6-et.weekday()) if et.weekday()!=6 else et
        elif freq=='M':
            st=datetime(year=st.year, month=st.month, day=1)
            et=datetime(year=et.year, month=et.month, day=calendar.monthrange(et.year, et.month)[1])            
        elif freq=='Q':
            st=datetime(year=st.year,month= st.month-(st.month-1) % 3 , day=1)
            et=datetime(et.year+1,1,1)+timedelta(-1) if (et.month-(et.month-1) % 3)==10 else datetime(et.year, (et.month-(et.month-1) % 3)+ 3, 1)+timedelta(-1)
        elif freq=='A':
            st=st.replace(month=1, day=1)
            et=et.replace(month=12, day=31)
        #生成日期区间
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
        price_limit=get_simpledatafromsql('mkt_limit',['security_id','trade_date','limit_up_price','limit_down_price'],['security_id','trade_date'],givenstocklist=givenstocklist,st=datelist[0],et=datelist[-1],keycol=['limit_up_price','limit_down_price'],usecode='security_id',keydt='trade_date',mergedataif=False)
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


# #取指数收益from 朝阳永续
def get_benchrt2(benchname,datelist):
    result=get_simpledatafromsql('[ZYYX].[dbo].[qt_idx_daily]',['trade_date','topen','high','low','tclose','lclose','change_rate'],['trade_date'],st=datelist[0],et=datelist[-1],keycol=['topen','high','low','tclose','lclose','change_rate'],usecode='',keydt='trade_date',mergedataif=False,wherecondition=" and index_code='"+re.sub("\D", "", get_benchSecuCode(benchname))+"'")
    result=result.rename(columns={'trade_date':'trddt','topen':'open','tclose':'close','lclose':'pre_close','change_rate':'pct_chg'})
    result=result.set_index('trddt')     
    result['pct_chg']=result['pct_chg']/100
    return result

#取指数成分股 from choice
def get_benchcompoandw2(benchname,datelist): 
    tablename='[FactorDB].[dbo].[INDEX_COMP_WT_'+re.sub("\D", "", get_benchSecuCode(benchname))+']'
    result=get_simpledatafromsql(tablename,['Trddt','Windcode','Weight'],['Trddt','Windcode'],st=datelist[0],et=datelist[-1],keycol=['Weight'],usecode='',keydt='Trddt',mergedataif=False)
    result['ticker_symbol']=[re.sub("\D", "", x) for x in result['Windcode']]
    result=result.rename(columns={'Trddt':'trddt','Weight':'weight'})
    result['trddt']=pd.to_datetime(result['trddt'],format='%Y-%m-%d') 
    result.set_index(['trddt','ticker_symbol'],inplace=True)
    return result

def get_simpledatafromsql(tablename,indicator,orderindicator=[],givenstocklist=[],st='',et='',keycol='',mergedataif=False,usecode='',keydt='publish_date',dtcol=[],fillmethod='na',has_merged_flag=False,wherecondition='',singletime=False,usetradedt=False,mergedate='',shifttime=0,keydtformat="%Y-%m-%d",colname=[]):
    indicator=indicator if isinstance(indicator,list) else indicator.split(',')
    orderindicator=orderindicator if isinstance(orderindicator,list) else orderindicator.split(',')
    db_code = "select "+','.join(indicator)+" from "+tablename+ " where 1=1 "
    
    if (singletime==True)&(fillmethod=='ffill')&(usetradedt==True):        
        originalet=et
        et=lagTradeDate(et,0,method='back')
        st=lagTradeDate(st,0,method='back')
    
    if usecode!='':
        usecode_transfer='ticker_symbol' if usecode=='stock_code' else usecode
        if len(givenstocklist)==0:
            givenstocklist=stocklist
        if isinstance(givenstocklist,pd.DataFrame):
            givenstocklist=givenstocklist[usecode_transfer]
        stockcondition=','.join([str(x) for x in givenstocklist]) if usecode!='ticker_symbol' else ','.join(["'"+str(x)+"'" for x in givenstocklist])        
        db_code = db_code + " and " + usecode+" in (" + stockcondition + ") "
    
    #时间筛选
    #早上9点前都算是前一天
    if ('time' in keydt)|(shifttime>0):
        if st!='':
            st_plus=st+timedelta(hours=shifttime, minutes=0, seconds=0)
            db_code = db_code+ " and "+keydt+">='"+st_plus.strftime(keydtformat+" %H:%M:%S")+"' "
        if et!='':
            et_plus=et+timedelta(hours=23+shifttime, minutes=59, seconds=59)
            db_code = db_code+ " and "+keydt+"<='"+et_plus.strftime(keydtformat+" %H:%M:%S")+"' "
    else:
        if st!='':
            db_code = db_code+ " and "+keydt+">='"+st.strftime(keydtformat)+"' "
        if et!='':   
            db_code = db_code+ " and "+keydt+"<='"+et.strftime(keydtformat)+"' "
    
    #其他条件
    if has_merged_flag==True:
        db_code = db_code+ " and merged_flag=1 "
    if len(wherecondition)>0:
        db_code = db_code+ wherecondition
    if len(orderindicator)>0:
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
    
    if len(colname)>0:
        result.columns=colname
        
    # #是否只返回一列，并mergedata
    # if mergedataif==True:
    #     result=get_mergedata(result,keycol=keycol,keydt=keydt,et=et,shifttime=shifttime,usecode=usecode_transfer,fillmethod=fillmethod,singletime=singletime,mergedate=mergedate)
    
    return result

# trddt_s=SD;trddt_e=ED
def get_port_fromsql(trddt_s='', trddt_e='', modelid=1):
    if (trddt_s=='')&(trddt_e==''):
        sqlstr="select Trddt, Windcode, Weight from [ModelRoom].[dbo].[MD_PORT] where ModelID = " + str(modelid)
    elif (trddt_s=='')&(trddt_e!=''):
        trddt_e=trddt_e.strftime("%Y-%m-%d") if not isinstance(trddt_e,str) else trddt_e   
        sqlstr="select Trddt, Windcode, Weight from [ModelRoom].[dbo].[MD_PORT] where Trddt <= \'" + trddt_e + "\' and ModelID = " + str(modelid)
    elif  (trddt_s!='')&(trddt_e==''):  
        trddt_s=trddt_s.strftime("%Y-%m-%d") if not isinstance(trddt_s,str) else trddt_s
        sqlstr="select Trddt, Windcode, Weight from [ModelRoom].[dbo].[MD_PORT] where Trddt >= \'" + trddt_s + "\' and ModelID = " + str(modelid)    
    else:
        trddt_s=trddt_s.strftime("%Y-%m-%d") if not isinstance(trddt_s,str) else trddt_s
        trddt_e=trddt_e.strftime("%Y-%m-%d") if not isinstance(trddt_e,str) else trddt_e        
        sqlstr="select Trddt, Windcode, Weight from [ModelRoom].[dbo].[MD_PORT] where Trddt >= \'" + trddt_s + "\' and Trddt <= \'" + trddt_e + "\' and ModelID = " + str(modelid)
    port = sql.ExecQuery(sqlstr)
    port = pd.DataFrame(port, columns = ['trddt', 'Windcode', 'Weight'])
    port['trddt'] = pd.to_datetime(port['trddt'], format='%Y-%m-%d')
    port['ticker_symbol'] = [x.split('.')[0] for x in port['Windcode']]
    port = port.set_index(['trddt','ticker_symbol']) 
    port = port.sort_index()
    port = port['Weight']
    return port

#filename=r'E:\盈科\组合优化\originaldata\多因子LRX选股202001_202012';col_name_dt='trade_dt';col_name_code='s_info_windcode';col_name_w='weight_hold'
def get_port_fromlocal(filename,col_name_dt,col_name_code,col_name_w,sheet_name='Sheet1',trddt_s='', trddt_e='',shiftdt=False,fillw=False):
    try:
        weight_original=pd.read_excel(filename+'.xlsx',sheet_name=sheet_name,parse_dates=True,dtype={col_name_code:'str'})
    except:
        weight_original=read_csv_self(filename,parse_dates=True,dtype={col_name_code:'str'})
       
    weight_original=weight_original.rename(columns={col_name_w:'Weight',col_name_dt:'trddt'})
    weight_original['ticker_symbol']=[re.sub("\D", "", x) if '.' in x else x.zfill(6) for x in weight_original[col_name_code]] 
    weight_original['trddt']=pd.to_datetime(weight_original['trddt'])
    weight_original=weight_original.set_index(['trddt','ticker_symbol']).sort_index()
    weight_original=weight_original['Weight']
    
    if (shiftdt==True)|(fillw==True):
        result=weight_original.unstack().fillna(0)
        if shiftdt==True:#权重往后延一天
            result['trddt_new']=(Dt_all_trade_forward1.reindex(result.index)).values 
            result.drop_duplicates(subset='trddt_new',keep='last',inplace=True)
            result=result.reset_index(drop=True).rename(columns={'trddt_new':'trddt'}).set_index('trddt')
        if fillw==True:
            result=result.reindex(Dt_trade[(Dt_trade>=result.index[0])&(Dt_trade<=result.index[-1])],method='ffill')
        weight_original=result.stack()
        weight_original=weight_original[weight_original!=0]
        weight_original.name='Weight'
    
    if (trddt_s!=''):
        weight_original=weight_original.loc[(weight_original.index.get_level_values(0)>=trddt_s)]        
    if (trddt_e!=''):
        weight_original=weight_original.loc[(weight_original.index.get_level_values(0)<=trddt_e)]

    return weight_original

def attribution_unit(iweight,idate,keycol_price='open'):
    #portfolio中每一个行业的权重与收益
    idata = stocklist_df.loc[idate].copy()
    idata['w_p']=iweight.reindex(idata.index,fill_value=0)
    idata['r_p']=idata['rt']*idata['w_p'] if keycol_price=='close' else idata['rt_next']*idata['w_p']
    idata['r_b']=idata['rt']*idata['w_b'] if keycol_price=='close' else idata['rt_next']*idata['w_b']
    
    grouped_temp=idata.groupby('indu_name')['r_b','r_p','w_b','w_p'].sum()
    #归一
    grouped_temp['r_p'] = grouped_temp['r_p'] / grouped_temp['w_p']
    grouped_temp['r_b'] = grouped_temp['r_b'] / grouped_temp['w_b']
    grouped_temp['r_p'] = grouped_temp['r_p'].fillna(0)
    
    #基准中每一个行业的权重与收益    
    allocation_daily = (grouped_temp['w_p'] -grouped_temp['w_b'] )*grouped_temp['r_b'] 
    allocation_daily.at['总和'] = allocation_daily.sum()
    selection_daily = (grouped_temp['r_p'] - grouped_temp['r_b'])*grouped_temp['w_b']
    selection_daily.at['总和'] = selection_daily.sum()  
    cross_daily =  ( grouped_temp['w_p'] -grouped_temp['w_b']) * (grouped_temp['r_p'] - grouped_temp['r_b'])
    cross_daily.at['总和'] = cross_daily.sum()
    
    performance = pd.DataFrame()
    performance['allocation'] = allocation_daily
    performance['selection'] = selection_daily
    performance['cross'] = cross_daily
    performance['excess_return'] = performance['allocation'] + performance['selection'] + performance['cross']
    performance = performance.T
    performance.index.name = 'ReturnDivision'
    performance['trddt'] = idate
    performance['trddt'] = performance['trddt'].dt.date
    # performance['modelid'] = modelid
    performance.reset_index(inplace = True)
    performance.set_index(['trddt','ReturnDivision'], inplace = True)    
    
    return performance

def attribution_multi(iweight,idate,variables,keycol_price='open'):
    lastdate=lagTradeDate(idate,-1,'back')
    W_p_R_b = variables.loc[lastdate,'W_p_R_b'] if lastdate in variables.index else 1
    W_b_R_b = variables.loc[lastdate, 'W_b_R_b'] if lastdate in variables.index else 1
    W_b_R_p = variables.loc[lastdate, 'W_b_R_p'] if lastdate in variables.index else 1
    W_p_R_p = variables.loc[lastdate, 'W_p_R_p'] if lastdate in variables.index else 1
    
    #portfolio中每一个行业的权重与收益
    idata = stocklist_df.loc[idate].copy()
    idata['w_p']=iweight.reindex(idata.index,fill_value=0)
    idata['r_p']=idata['rt']*idata['w_p'] if keycol_price=='close' else idata['rt_next']*idata['w_p']
    idata['r_b']=idata['rt']*idata['w_b'] if keycol_price=='close' else idata['rt_next']*idata['w_b']
    
    grouped_temp=idata.groupby('indu_name')['r_b','r_p','w_b','w_p'].sum()
    #归一
    grouped_temp['r_p'] = grouped_temp['r_p'] / grouped_temp['w_p']
    grouped_temp['r_b'] = grouped_temp['r_b'] / grouped_temp['w_b']
    grouped_temp['r_p'] = grouped_temp['r_p'].fillna(0)
    
    #基准中每一个行业的权重与收益       
    allocation_daily = (grouped_temp['w_p'] * W_p_R_b - grouped_temp['w_b'] * W_b_R_b) * grouped_temp['r_b']
    allocation_daily.at['总和'] = allocation_daily.sum()
        
    selection_daily = (grouped_temp['r_p'] * W_b_R_p - grouped_temp['r_b'] * W_b_R_b) * grouped_temp['w_b']
    selection_daily.at['总和'] = selection_daily.sum() 
        
    cross_daily = grouped_temp['w_p'] * grouped_temp['r_p'] * W_p_R_p + grouped_temp['w_b'] * grouped_temp['r_b'] * W_b_R_b \
                - grouped_temp['w_p'] * grouped_temp['r_b']  * W_p_R_b - grouped_temp['w_b']* grouped_temp['r_p'] * W_b_R_p
    cross_daily.at['总和'] = cross_daily.sum()
            
    W_p_R_b = (1 + (grouped_temp['w_p'] * grouped_temp['r_b']).sum()) * W_p_R_b
    W_b_R_b = (1 + (grouped_temp['w_b'] * grouped_temp['r_b']).sum()) * W_b_R_b
    W_b_R_p = (1 + (grouped_temp['w_b']* grouped_temp['r_p']).sum()) * W_b_R_p
    W_p_R_p = (1 + (grouped_temp['w_p'] * grouped_temp['r_p']).sum()) * W_p_R_p
        
    variables.at[idate, 'W_p_R_b'] = W_p_R_b
    variables.at[idate, 'W_b_R_b'] = W_b_R_b
    variables.at[idate, 'W_b_R_p'] = W_b_R_p
    variables.at[idate, 'W_p_R_p'] = W_p_R_p
    
    performance = pd.DataFrame()
    performance['allocation'] = allocation_daily
    performance['selection'] = selection_daily
    performance['cross'] = cross_daily
    performance['excess_return'] = performance['allocation'] + performance['selection'] + performance['cross']
    performance = performance.T
    performance.index.name = 'ReturnDivision'
    performance['trddt'] = idate
    performance['trddt'] = performance['trddt'].dt.date
    # performance['modelid'] = modelid
    performance.reset_index(inplace = True)
    performance.set_index(['trddt','ReturnDivision'], inplace = True)    
    
    return performance

def get_attribution_result(df,modelid,keycol_price='open'):
    datelist = df.index.get_level_values(0).unique()
    
    variables=pd.DataFrame()
    result_unit=pd.DataFrame()
    result_multi=pd.DataFrame()
    for idate in tqdm(datelist):
        if idate not in Dt_trade:
            continue    
        iresult_unit=attribution_unit(df.loc[idate],idate) 
        result_unit=pd.concat([result_unit,iresult_unit],axis=0)
        
        iresult_multi=attribution_multi(df.loc[idate],idate,variables)
        result_multi=pd.concat([result_multi,iresult_multi],axis=0)
    
    result_multi=result_multi.groupby(['ReturnDivision']).cumsum()
           
    #单个时间，不考虑累乘
    result_unit['modelid']=modelid;result_unit=result_unit.reset_index();result_unit=result_unit.rename(columns={'trddt':'Trddt'})
    result_unit = result_unit[['Trddt','modelid','ReturnDivision']+result_unit.columns.drop(['Trddt','modelid','ReturnDivision']).tolist()]
    
    #多个时间，累乘且累加结果    
    result_multi['modelid']=modelid;result_multi=result_multi.reset_index();result_multi=result_multi.rename(columns={'trddt':'Trddt'})
    result_multi = result_multi[['Trddt','modelid','ReturnDivision']+result_multi.columns.drop(['Trddt','modelid','ReturnDivision']).tolist()]
    
    return result_unit,result_multi

# def get_exposure(df):
#     datelist = df.index.get_level_values(0).unique()    
#     exposure=sql.get_df_from_db_1("select trade_date,ticker_symbol,BETA,MOMENTUM,SIZE,EARNYILD,RESVOL,GROWTH,BTOP,LEVERAGE,LIQUIDTY,SIZENL from dy1d_exposure where trade_date >='"+datelist[0].strftime("%Y%m%d")+"' and trade_date <='"+datelist[-1].strftime("%Y%m%d")+"'",parse_dates=['trade_date'])
#     exposure=exposure.rename(columns={'trade_date':'trddt','BETA':'Beta','MOMENTUM':'动量','SIZE':'市值','EARNYILD':'盈利','RESVOL':'残差波动率','GROWTH':'成长','BTOP':'账面市值比','LEVERAGE':'杠杆','LIQUIDTY':'流动性','SIZENL':'非线性市值'}).set_index(['trddt','ticker_symbol'])
#     exposure=exposure.apply(lambda x:x.astype('float'),axis=0)
#     exposure=exposure.sort_index()
#     result_exposure=exposure.mul(df.loc[df.index.get_level_values(0).isin(Dt_trade)],axis=0).groupby(level=0).apply(lambda x:x.sum(axis=0))
#     result_exposure=result_exposure[(result_exposure!=0).any(axis=1)]
#     return result_exposure,result_exposure.mean(),result_exposure.min(),result_exposure.max()

def get_exposure(df_):
    df=df_.copy()
    df=df.loc[df.index.get_level_values(0).isin(Dt_trade)]
    datelist = df.index.get_level_values(0).unique()  
    #取基准权重和行业
    tempdata = stocklist_df.loc[datelist][['indu_name','w_b']]
    tempdata['w_p']=df.reindex(tempdata.index,fill_value=0)
    tempdata=tempdata[(tempdata['w_b']>0)|(tempdata['w_p']>0)]
    tempdata['df_a']=tempdata['w_p']-tempdata['w_b']
    
    #行业暴露
    exposure_indu=pd.get_dummies(tempdata['indu_name'],drop_first=False)
    exposure_indu=exposure_indu.apply(lambda x:x.astype('float'),axis=0)
    result_exposure_indu=exposure_indu.mul(tempdata['df_a'],axis=0).groupby(level=0).apply(lambda x:x.sum(axis=0))
    result_exposure_indu=result_exposure_indu[(result_exposure_indu!=0).any(axis=1)]
    
    #风险暴露
    exposure_risk=sql.get_df_from_db_1("select trade_date,ticker_symbol,BETA,MOMENTUM,SIZE,EARNYILD,RESVOL,GROWTH,BTOP,LEVERAGE,LIQUIDTY,SIZENL from dy1d_exposure where trade_date >='"+datelist[0].strftime("%Y%m%d")+"' and trade_date <='"+datelist[-1].strftime("%Y%m%d")+"'",parse_dates=['trade_date'])
    exposure_risk=exposure_risk.rename(columns={'trade_date':'trddt','BETA':'Beta','MOMENTUM':'动量','SIZE':'市值','EARNYILD':'盈利','RESVOL':'残差波动率','GROWTH':'成长','BTOP':'账面市值比','LEVERAGE':'杠杆','LIQUIDTY':'流动性','SIZENL':'非线性市值'}).set_index(['trddt','ticker_symbol'])
    exposure_risk=exposure_risk.apply(lambda x:x.astype('float'),axis=0)
    exposure_risk=exposure_risk.sort_index()
    result_exposure_risk=exposure_risk.mul(tempdata['df_a'],axis=0).groupby(level=0).apply(lambda x:x.sum(axis=0))
    result_exposure_risk=result_exposure_risk[(result_exposure_risk!=0).any(axis=1)]
    
    return {'行业主动暴露':{'result_exposure_indu':result_exposure_indu,'result_exposure_indu_mean':result_exposure_indu.mean(),'result_exposure_indu_min':result_exposure_indu.min(),'result_exposure_indu_max':result_exposure_indu.max()},'风险主动暴露':{'result_exposure_risk':result_exposure_risk,'result_exposure_risk_mean':result_exposure_risk.mean(),'result_exposure_risk_min':result_exposure_risk.min(),'result_exposure_risk_max':result_exposure_risk.max()}}    

def get_minbar_price(df):
    sql_min=SQLServer("202.101.23.166","poirot","Aliyun123456","tlsj_min_bar_month")
    df_month=df.reset_index().copy()
    df_month['TLcode']=stocklist.set_index(['ticker_symbol']).loc[df_month['ticker_symbol'],'TLcode'].tolist()
    df_month=df_month.set_index(['trddt','TLcode'])
    df_month=df_month.groupby(df_month.index.get_level_values(0).to_period('M')).apply(lambda x:sorted(x.index.get_level_values(1).unique()))
    datelist=[x.strftime("%Y%m") for x in df_month.index]
    result=pd.DataFrame()
    for idate in tqdm(datelist):
        #不用视图
        db_code = "select TLcode,Trddt,OpenPrice,ClosePrice FROM [tlsj_min_bar_month].[dbo].[ashare_"+idate+"] where Bartime='09:30:00' and TLcode in ( "+','.join(["'"+str(x)+"'" for x in df_month.loc[idate]])+") order by Trddt,TLcode"
        # #用视图
        # db_code = "select TLcode,Trddt,OpenPrice,ClosePrice FROM ashare_"+idate+"_1minclose where TLcode in ( "+','.join(["'"+str(x)+"'" for x in df_month.loc[idate]])+") order by Trddt,TLcode"
        temp = sql_min.ExecQuery(db_code) 
        temp = pd.DataFrame(temp , columns = ['TLcode','trddt','open','close'])
        result=pd.concat([result,temp],axis=0)
    
    result['trddt']=pd.to_datetime(result['trddt'],format='%Y-%m-%d') 
    result['TLcode']=[x.strip() for x in result['TLcode']]
    result['open']=result['open'].astype('float')
    result['close']=result['close'].astype('float')
    result['ticker_symbol']=stocklist.set_index(['TLcode']).loc[result['TLcode'].values,'ticker_symbol'].values
    result=result.set_index(['trddt','ticker_symbol'])
    sql_min.closesql()
    return result

# deltahour=0;deltaminute=10
def get_minbar_price_between(df,deltahour=0,deltaminute=10):
    sql_min=SQLServer("202.101.23.166","poirot","Aliyun123456","tlsj_min_bar_month")
    df_month=df.reset_index().copy()
    df_month['TLcode']=stocklist.set_index(['ticker_symbol']).loc[df_month['ticker_symbol'],'TLcode'].tolist()
    df_month=df_month.set_index(['trddt','TLcode'])
    df_month=df_month.groupby(df_month.index.get_level_values(0).to_period('M')).apply(lambda x:sorted(x.index.get_level_values(1).unique()))
    datelist=[x.strftime("%Y%m") for x in df_month.index]
    result=pd.DataFrame()
    # #不用视图
    # sttime=datetime.strptime("09:30:00",'%H:%M:%S')
    # ettime=sttime+timedelta(hours=deltahour,minutes=deltaminute)
    # for idate in tqdm(datelist):
    #     iresult=pd.DataFrame()
    #     for jstock in df.index.get_level_values(1).unique():
    #         db_code = "select Trddt,min(LowPrice) as min_price,max(HighPrice) max_price,sum(Amount) as amount, sum(Volume) as volume FROM [tlsj_min_bar_month].[dbo].[ashare_"+idate+"] where Bartime>'"+sttime.strftime('%H:%M:%S')+"' and Bartime<='"+ettime.strftime('%H:%M:%S')+"' and TLcode='"+jstock+"' group by Trddt order by Trddt"
    #         temp = sql_min.ExecQuery(db_code) 
    #         if len(temp)>0:
    #             temp = pd.DataFrame(temp , columns = ['trddt','min_price','max_price','amount','volume'])
    #             temp['TLcode']=jstock
    #             iresult=pd.concat([iresult,temp],axis=0)
    #         else:#没有该证券，或者停牌
    #             pass 
    #     result=pd.concat([result,iresult],axis=0)
    #用视图
    for idate in tqdm(datelist):        
        db_code = "select TLcode,Trddt,min_price,max_price,vwap FROM ashare_"+idate+"_vwap_"+str(deltahour*60+deltaminute)+" where TLcode in ( "+','.join(["'"+str(x)+"'" for x in df_month.loc[idate]])+") order by Trddt,TLcode"
        temp = sql_min.ExecQuery(db_code) 
        temp = pd.DataFrame(temp , columns = ['TLcode','trddt','min_price','max_price','vwap'])
        result=pd.concat([result,temp],axis=0)

    result['trddt']=pd.to_datetime(result['trddt'],format='%Y-%m-%d') 
    result['TLcode']=[x.strip() for x in result['TLcode']]
    result['min_price']=result['min_price'].astype('float');
    result['max_price']=result['max_price'].astype('float');
    # result['vwap']=(result['amount'].astype('float')/result['volume'].astype('float'));
    result['vwap']=result['vwap'].astype('float');
    result['ticker_symbol']=stocklist.set_index(['TLcode']).loc[result['TLcode'].values,'ticker_symbol'].values
    result=result.set_index(['trddt','ticker_symbol'])
    sql_min.closesql()
    return result

# ibench=rt_bench;ibench_indu=rt_indu_bench;weight_df_=df;title='ModelID'+str(modelid)+'_fee'+str(feetest);accurateif=True;shiftdtif=False;filterweight=True;indulist=[];plotindu=False;zhongxinghua=False;inbenchif=False;longshort='long';interval=1
def teststrategy(ibench,ibench_indu,weight_df_,title,feetest=0,shiftdtif=False,filterweight=True,indulist=[],plotindu=False,zhongxinghua=False,inbenchif=False,longshort='long',interval=1,testindu=False,accurateif=True,keycol_price='open'):
    weight_df=weight_df_.copy()
    weight_df.name='Weight'
    # title=title+'_'+benchfield+'内选股' if inbenchif==True else title
    # title=title+'_行业中性化' if zhongxinghua==True else title
    # title=title+'_'+longshort if longshort!='long' else title
    # title=title+'_interval='+str(interval) if interval>1 else title
    # if longshort in ['long','short']:
    #     title=title+'减去基准'
    # title=title+'_fee'+str(feetest)
    # title=title+'_filterweight'+str(filterweight)
    
    if (longshort=='short')&((weight_df<0).sum()==0):
        weight_df=-weight_df
    
    stock_select_num=weight_df.groupby(level=0).apply(lambda x:((x>0)|(x<0)).sum())
       
    #全市场表现    
    r_market,turnover=get_dayreturn(stocklist_df,price_all,weight_df.unstack(),feetest,shiftdtif=shiftdtif,filterweight=filterweight,interval=interval,accurateif=accurateif,keycol_price=keycol_price)
    finalindex=r_market.index if r_market.index[-1]==ibench.index[-1] else r_market.index[r_market.index<=ibench.index[-1]]
    r_market=r_market.loc[finalindex];turnover=turnover.loc[finalindex];stock_select_num=stock_select_num.loc[finalindex]
    
    if longshort=='long':
        alpha_market=r_market.sub(ibench.loc[finalindex],axis=0)
    elif longshort=='short':
        alpha_market=r_market.sub(-ibench.loc[finalindex],axis=0)
    else:
        alpha_market=r_market
    
    stat_alpha = get_statofreturn(alpha_market)
    stat_alpha['平均换手率']=turnover.mean()
    stat_alpha['平均日持仓数量']=stock_select_num.mean()

    stat_alpha_year = alpha_market.groupby(alpha_market.index.year).apply(lambda x:get_statofreturn(x)).unstack(0)

    net_value=(1+alpha_market).cumprod()
    net_value_bench=(1+ibench.loc[finalindex]).cumprod()
    mmd=(net_value.cummax()-net_value)/net_value.cummax()
    alpha_market_month=alpha_market.groupby(alpha_market.to_period('M').index,group_keys= False).apply(lambda x:((x+1).cumprod()-1).iloc[-1])
    net_value.plot(title=title)
    plt.show()
    
    if testindu==True:
    #分行业表现 
        weight_df_indu=pd.DataFrame(weight_df)
        weight_df_indu['indu_name']=stocklist_df['indu_name']
        indu_sum=weight_df_indu.groupby(['trddt','indu_name'])['Weight'].transform("sum")   
        weight_df_indu['Weight']= np.where(indu_sum==0,0,weight_df_indu['Weight']/indu_sum) #每个行业权重归一
        stock_select_num_indu=weight_df_indu.groupby(['trddt','indu_name']).apply(lambda x:((x>0)|(x<0)).sum()).unstack(1)
        r_indu,_=get_dayreturn(stocklist_df,price_all,weight_df_indu['Weight'].unstack(),feetest,shiftdtif=shiftdtif,filterweight=filterweight,interval=interval,accurateif=accurateif,keycol_price=keycol_price,induif=True)
        r_indu=r_indu.unstack(1)
        r_indu=r_indu.loc[finalindex]
        if longshort=='long':   
            alpha_indu=r_indu.sub(ibench_indu.loc[finalindex],axis=0)
        elif longshort=='short':
            alpha_indu=r_indu.sub(-ibench_indu.loc[finalindex],axis=0)
        else:
            alpha_indu=r_indu
 
        alpha_indu=alpha_indu.dropna(axis=1,how='all')             
        stat_alpha_indu = alpha_indu.apply(lambda x:get_statofreturn(x))
        stat_alpha_indu.loc['stock_select_num']=stock_select_num_indu.mean().values
        
        stat_alpha_indu_year = alpha_indu.apply(lambda xx:xx.groupby(xx.index.year).apply(lambda x:get_statofreturn(x))).reset_index()
        
        if plotindu==True:
            net_value_indu = alpha_indu.apply(lambda x:(1+x).cumprod())
            for iindu in net_value_indu.columns:
                if ((len(indulist)==0)&(stat_alpha_indu[iindu].loc['sharpe']>1))|((len(indulist)>0) &(iindu in indulist)):
                    net_value_indu[iindu].plot(title=title+'分行业_'+iindu)
                    plt.show() 
        result={'r':r_market,'alpha':alpha_market,'alpha_month':alpha_market_month,'net_value':net_value,'net_value_bench':net_value_bench,'每日回撤':mmd,'stock_select_num':stock_select_num,'turnover':turnover,'stat_alpha':stat_alpha,'stat_alpha_year':stat_alpha_year,'r_indu':r_indu,'alpha_indu':alpha_indu,'stock_select_num_indu':stock_select_num_indu,'stat_alpha_indu':stat_alpha_indu,'stat_alpha_indu_year':stat_alpha_indu_year }
    else:                
        result={'r':r_market,'alpha':alpha_market,'alpha_month':alpha_market_month,'net_value':net_value,'net_value_bench':net_value_bench,'每日回撤':mmd,'stock_select_num':stock_select_num,'turnover':turnover,'stat_alpha':stat_alpha,'stat_alpha_year':stat_alpha_year  }
        
    return result

def get_shift_and_refreq_weight(pos,shiftdtif=False,interval=1,firstorlast='last'):
    pos=pos.sort_index(axis=1)
    pos=pos.fillna(0)
    
    #shiftdt部分 -------------------------------------------------------------   
    if shiftdtif==True:#权重往后延一天
        pos['trddt_new']=(Dt_all_trade_forward1.reindex(pos.index)).values 
    else:#找最近交易日
        pos['trddt_new']=(Dt_all_trade_forward0.reindex(pos.index)).values
    pos.drop_duplicates(subset='trddt_new',keep='last',inplace=True)
    pos=pos.reset_index(drop=True).rename(columns={'trddt_new':'trddt'}).set_index('trddt')
    pos=pos[pos.index.notnull()]
    pos=pos[pos.index<=Dt_trade[-1]]    #防止pos过多，不在Dt_trade中

    #refreq部分 ------------------------------------------------------------- 
    #隔几天换仓，接收n周，n月，n季度，n年
    if (isinstance(interval,str)):
        if (any([x in interval for x in ['D','W','M','Q','Y']])):
            interval=re.sub("\D", "", interval)+periodtofreq(re.findall(r'\D+', interval)[0])
            if firstorlast=='last':
                pos=(pos.groupby(pos.to_period(interval).index).tail(1)).reindex(pos.index,method='ffill')
            else:
                pos=(pos.groupby(pos.to_period(interval).index).head(1)).reindex(pos.index,method='ffill')
    elif (isinstance(interval,list))|(isinstance(interval,pd.Series))|(isinstance(interval,pd.Index)):
        pos=(pos.reindex(interval)).reindex(Dt_trade[(Dt_trade>=pos.index[0])],method='ffill')
    elif isinstance(interval,int):
        if (interval==1)|(interval==0):
            pass
        else:
            pos=(pos.reindex(Dt_trade[::interval])).reindex(pos.index,method='ffill')
    else:
        pass
    
    return pos

#函数用于权重filter 涨停 跌停   
def get_filterweight(ipos_,ipos_last,ihasif,ihaltif,iztif,idtif):
    ipos=ipos_.copy()
    #判断是否有，如果没上市则设为0
    ipos=np.where(ihasif==True,ipos,0)
    #如果增持，但是涨停或者停牌，则不能买入
    ipos=np.where((ipos>ipos_last)&((ihaltif==True)|(iztif==True)),ipos_last,ipos)
    #如果减持，但是跌停或者停牌，则不能卖出
    ipos=np.where((ipos<ipos_last)&((ihaltif==True)|(idtif==True)),ipos_last,ipos)
    return ipos

# Y_price=price_all;pos=weight_df.unstack();shiftdtif=shiftdtif;filterweight=filterweight;induif=False;interval=1;firstorlast='last'
#原始open买卖，或者单一价格买卖，并且不rebalance
def get_dayreturn(stocklist_df,Y_price,pos,feetest,shiftdtif=False,filterweight=False,induif=False,interval=1,firstorlast='last',accurateif=False,keycol_price='open'):
    #先进行w的处理
    pos=get_shift_and_refreq_weight(pos,shiftdtif=shiftdtif,interval=interval,firstorlast=firstorlast)
    pos0=pd.DataFrame(0,columns=pos.columns,index=[Dt_trade_PD[Dt_trade_PD.tolist().index(pos.index[0])-1]])
    pos0=pos0.append(pos)
    #判断是否进行filter
    if filterweight==True:
        hasif=(stocklist_df['IPOdays']>=0).unstack()[pos.columns].fillna(False)
        haltif=stocklist_df['halt'].unstack()[pos.columns].fillna(False)
        ztif=stocklist_df['zt'].unstack()[pos.columns].fillna(False)
        dtif=stocklist_df['dt'].unstack()[pos.columns].fillna(False)
        for i in range(1,len(pos0.index)):
            iindex=pos0.index[i]
            pos0.iloc[i]=get_filterweight(pos0.iloc[i],pos0.iloc[i-1],hasif.loc[iindex],haltif.loc[iindex],ztif.loc[iindex],dtif.loc[iindex])
    #开始计算收益
    pos=pos0.loc[pos.index]
    w_0=pos0.shift(1).loc[pos.index]
    w_1=pos0.loc[pos.index]
    w_delta=w_1-w_0
    turnover=w_delta.abs().sum(axis=1)
    shouxu=w_delta.abs()*feetest
    Y_price=Y_price.loc[(Y_price.index.get_level_values(0)>=pos.index[0])&(Y_price.index.get_level_values(0)<=pos.index[-1])]
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
    # combine['年化收益']=round(r.mean()*standardreturn('D'),4)
    combine['年化收益']=round(net_value.dropna().iloc[-1]**(standardreturn('D')/len(r))-1,4) #复合年化收益
    combine['年化标准差']=round(r.std()*np.sqrt(standardreturn('D')),4)
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

#####################################################################################################################
#   正文
#####################################################################################################################
# path=r'F:\BigQuant'
# path=r'E:\盈科\策略1'
# os.chdir(path)

# SD='2020-01-01';ED=str(date.today());benchfield='中证500';modelid=10;keycol_price='open';vwaptime=0;feetest=0.00075;testindu=False;readfromsql=True
def main_funtion(SD,ED,benchfield='中证500',modelid=10,keycol_price='open',vwaptime=10,feetest=0.00075,testindu=False,readfromsql=True):
    #对全局变量进行global声明
    global sql,Dt_trade,Dt_trade_PD,Dt_trade_long,Dt_all_trade_forward1,Dt_all_trade_forward0,stocklist,stocklist_df,price_all
    sql = SQLServer("192.168.99.46","poirot","Aliyun123456","TLSJ")
    
    #参数
    # benchfield='中证500'
    # modelid=10
    # keycol_price='vwap'
    # feetest=0.00075
    # testindu=False
    # vwaptime=10
    # readfromsql=True
    vwaptime=vwaptime if keycol_price=='vwap' else 0
    if (vwaptime>0)&(keycol_price=='vwap'):
        halt_st=timedelta(hours=9,minutes=30);halt_et=timedelta(hours=9,minutes=(30+vwaptime))
    elif keycol_price=='close':
        halt_st=timedelta(hours=14,minutes=59);halt_et=timedelta(hours=15)
    else:
        halt_st=timedelta(hours=9,minutes=30);halt_et=timedelta(hours=9,minutes=31)
    
    
    SD=datetime.strptime(SD,'%Y-%m-%d')
    ED=datetime.strptime(ED, '%Y-%m-%d')
    PD=datetime.strptime("2000-01-01",'%Y-%m-%d') #数据多提取10年，用于计算过去5年
    PD1=SD-timedelta(10) #数据多提取10天
    
    #取时间
    Dt_all= get_date(SD,ED,'D',istradeday=False)
    Dt_trade = get_date(SD,ED,'D')
    Dt_all_PD = get_date(PD,ED,'D',istradeday=False)
    Dt_trade_PD = get_date(PD,ED,'D')
    Dt_all_PD1 = get_date(PD1,ED,'D',istradeday=False)
    Dt_trade_PD1 = get_date(PD1,ED,'D')
    Dt_trade_M_PD1= get_date(PD1,ED,'M')
    
    Dt_trade_long=get_date(PD-timedelta(30),ED+timedelta(30),'D') #前面后面都延长，保证下面两个函数有值
    Dt_all_trade_forward1=pd.Series(pd.DatetimeIndex([lagTradeDate(x,1,'back') for x in Dt_all_PD]),index=Dt_all_PD)
    Dt_all_trade_forward0=pd.Series(pd.DatetimeIndex([lagTradeDate(x,0,'forward') for x in Dt_all_PD]),index=Dt_all_PD)
    
    #取权重
    if readfromsql==True:  
        df = get_port_fromsql(SD, ED, modelid)
        if len(df)==0:
            return "No weight data"
    else:
        if modelid==2:
            df = get_port_fromlocal(r'F:\BigQuant\多因子LRX选股202001_202012',col_name_dt='trade_dt',col_name_code='s_info_windcode',col_name_w='weight_hold',sheet_name='Sheet1',shiftdt=True,fillw=False,trddt_s=SD,trddt_e=ED)
        else:
            return "No weight data"
    
    modelinfo=sql.get_df_from_db_1("select * from [ModelRoom].[dbo].[MD_DICT] where modelid="+str(modelid))
    
    #取全部A股（不包括科创板）,包括未上市的
    stocklist=get_stocklist(field='全部A股',st=PD1,et=ED)
    #全部stocklist的价格和收益率
    price_all=get_stockprice(Dt_trade,freq='D',read=False,save=False)
    price_all['rt_next']=price_all.groupby(level=1,group_keys=False).apply(lambda x:x['open'].shift(-1)/x['open']-1)
    #全部stocklist的未复权价格
    price_all_unadj=get_stockprice_unadj(Dt_trade,read=False,save=False)
    
    if (keycol_price=='vwap')|(keycol_price=='1min_close'):
        #复权因子
        adjf = get_simpledatafromsql('mkt_equd_adj',['trade_date','ticker_symbol', 'accum_adj_factor'],['trade_date','ticker_symbol'], st=Dt_trade_PD1[0],et=Dt_trade_PD1[-1],usecode='ticker_symbol',keydt='trade_date',keycol='accum_adj_factor',mergedataif=False)
        adjf = adjf.rename(columns={'trade_date':'trddt'})
        adjf = adjf.set_index(['trddt','ticker_symbol'])
        
        #未复权的分钟价
        #开盘第一分钟收盘价
        price_all_unadj_1min=get_minbar_price(df)['close']
        
        price_all_unadj['1min_close']=price_all_unadj_1min
        price_all['1min_close']=price_all_unadj_1min*adjf.iloc[:,0]
    
    if vwaptime>0:
        #开盘vwaptime分钟的vwap和max和min
        price_all_unadj_barmin=get_minbar_price_between(df,deltaminute=vwaptime)
        
        price_all_unadj[['min_price','max_price','vwap']]=price_all_unadj_barmin[['min_price','max_price','vwap']]
        price_all[['min_price','max_price','vwap']]=price_all_unadj_barmin[['min_price','max_price','vwap']].apply(lambda x:x*adjf.iloc[:,0])
    
    #取ST的股票
    stock_ST=get_ST_stock()
    #取停牌超过一天的股票
    stock_halt=get_halt_stock()
    #取股票涨跌的价格，判断是否以keycol_price价格买卖是否能够实现
    price_limit=get_pricelimit(Dt_trade,read=False,save=False)
    
    #基准的收益率 
    price_bench=get_benchrt2(benchfield,Dt_trade) #from zyyx
    rt_bench=price_bench['close']/price_bench['pre_close']-1
    
    #基准的成分和权重
    stock_bench=get_benchcompoandw2(benchfield,Dt_trade-timedelta(30)) #from choice
    datelist_benchcmp=stock_bench.index.get_level_values(0).unique()
    
    #个股的申万一级行业分类
    stock_indu=get_stockindu(PD1,induname='申万',industry_level=1)
    
    #当前市场有的股票集合，增加了ST，w_b，行业
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
    if keycol_price=='vwap':
        stocklist_df['zt']=((price_limit.reindex(stocklist_df.index)['limit_up_price']==price_all_unadj.reindex(stocklist_df.index)['min_price']))
        stocklist_df['dt']=((price_limit.reindex(stocklist_df.index)['limit_down_price']==price_all_unadj.reindex(stocklist_df.index)['max_price']))
    else:
        stocklist_df['zt']=((price_limit.reindex(stocklist_df.index)['limit_up_price']==price_all_unadj.reindex(stocklist_df.index)[keycol_price]))
        stocklist_df['dt']=((price_limit.reindex(stocklist_df.index)['limit_down_price']==price_all_unadj.reindex(stocklist_df.index)[keycol_price]))
    stocklist_df[['pre_close','open','high','low','close','rt','rt_next']]=price_all[['pre_close','open','high','low','close','rt','rt_next']]
    
    #行业rt_以bench的行业为基准 昨收到今收的收益率
    rt_indu_bench=stocklist_df.groupby(level=0,group_keys=False).apply(lambda xx:xx.groupby('indu_name',group_keys=False).apply(lambda x:(x['w_b']*x['rt']).sum()/x['w_b'].sum()))
    
    
    #回测------------------------------------------------------------------------------
    result_strategy=teststrategy(rt_bench,rt_indu_bench,df,'ModelID'+str(modelid)+'_fee'+str(feetest),keycol_price=keycol_price,accurateif=True,feetest=feetest,shiftdtif=False,filterweight=True,indulist=[],plotindu=False,zhongxinghua=False,inbenchif=False,longshort='long',interval=1,testindu=testindu)
    
    #Brinson归因------------------------------------------------------------------------------
    result_unit,result_multi=get_attribution_result(df,modelid,keycol_price=keycol_price)
    
    #风险归因------------------------------------------------------------------------------
    result_exposure=get_exposure(df)
        
    sql.closesql()
    
    return {'模型信息':modelinfo,'回测':result_strategy,'归因':{'result_unit':result_unit,'result_multi':result_multi},'风险':result_exposure}


if __name__ == "__main__":
    result=main_funtion('2020-01-01',str(date.today()),benchfield='中证500',modelid=1,keycol_price='open',vwaptime=0,feetest=0.00075,testindu=True,readfromsql=True)