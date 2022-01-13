# -*- coding: utf-8 -*-
# @author: poirot

from importpy import * #导入常用模块
import sys
sys.path.append(r'C:\Users\admin\Desktop\work\tools\python_fn')
from SQLServer_df import SQLServer_df
SQL46_TLSJ = SQLServer_df("202.101.23.166","poirot","Aliyun123456","TLSJ")

## 功能：用于转换证券代码
## tickcer_symbol 是'000001','000978'这种，Ticker是'1','978'这种int
## 输入字段大小写无所谓，但是输出会按照code_types里的，比如windcode:Windcode
def sec_code_converting(code_raw_df,input_type='Windcode',output_type='TLcode',replace=False):
    global SQL46_TLSJ    
    
    # check args
    code_types=set(['TLcode','Windcode','Ticker','sec_short_name','ticker_symbol','security_id','party_id'])
    if not (set([input_type,output_type]) < code_types):
        raise ValueError("input_type should be one of " + str(code_types))
    if not isinstance(code_raw_df,pd.DataFrame):
        raise ValueError('code_raw_df should be DataFrame!')
        
    # 识别code_raw_df字段名小写
    cndict={'tlcode':'TLcode','windcode':'Windcode','ticker':'Ticker','sec_short_name':'sec_short_name','ticker_symbol':'ticker_symbol','security_id':'security_id','party_id':'party_id'}    
    colname_lower=code_raw_df.columns.str.lower().to_list()
    code_raw_df.columns=[(cndict[cn] if cndict.get(cn) else cn) for cn in colname_lower]
    
    # remove white space
    if not pd.api.types.is_integer_dtype(code_raw_df[input_type]):
        code_raw_df[input_type]=code_raw_df[input_type].apply(lambda x:x.replace(" ",""))

    
    # get tab from FactorDB
    sqlstr=r"select * from [TLSJ].[dbo].[yk_stocklist]"
    seccode_tab=SQL46_TLSJ.ExecQuery(sqlstr)
    seccode_tab=pd.DataFrame(seccode_tab)
    seccode_tab['Ticker']=seccode_tab[['ticker_symbol']].astype('int')
    
    # set index
    seccode_tab.set_index(input_type,inplace=True)
    code_raw_df=code_raw_df.reset_index().set_index(input_type)
    code_raw_df[output_type]=seccode_tab[output_type]
    code_raw_df.reset_index(inplace=True)
    
    # whether replace
    if replace==True:
        code_raw_df.drop([input_type],axis=1)
    
    return(code_raw_df)
    







