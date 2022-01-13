# -*- coding: utf-8 -*-
"""
Created on Thu Aug 27 15:37:00 2020

@author: Admin
"""

import pandas as pd
import numpy as np
from scipy.optimize import minimize #动态优化
from sqlalchemy import create_engine
engine = create_engine('mssql+pymssql://poirot:Aliyun123456@192.168.99.46/ModelRoom')
from sqlalchemy.types import VARCHAR, Float, Integer, Date, Numeric, Time
from datetime import datetime,date,timedelta
import sys #添加路径，导入模块
sys.path.append(r'D:\anacoda\selfku')
from SQLServer import SQLServer
sql = SQLServer("192.168.99.46","poirot","Aliyun123456","ModelRoom")

def rank_data(data,drop0=False,direction=True):
    data_rank=data.copy()
  
    if drop0==True:
        data0index=data_rank[data_rank==0].index
        datano0index=data_rank[data_rank!=0].index
        data_rank.loc[datano0index]=data_rank.loc[datano0index].rank(method='dense',ascending=direction,pct=True)
        if direction==False:#如果正排序，则0是最小的；如果负排序，0是最大的
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

# data=test;w0='';wsum=1;controlindu=True;controlmv=True;limitindu=0.03;limitmv=0.15;limits=0;limitlower=0.0001;limitup=0.03;absvalue=True;
def optimization(data,w0='',wsum=1,predfactordf=[],predfactorvalue=False,prefactorwb=True,controlindu=True,controlmv=False,limitindu=0.03,limitmv=0.15,limits=0,limitlower=0.0001,limitup=0.03,absvalue=True):
    data_=data.copy()
    data_=data_.dropna(axis=1,how='all')
    data_=data_.fillna(0)
    # data_['alpha']=(data_['alpha']-data_['alpha'].mean())/data_['alpha'].std()
    # data_['alpha']= [x**(1/3) if x>0 else 0 for x in data_['alpha']]
    data_['alpha']=rank_data(data_['alpha'],drop0=True,direction=True)
    w_b=np.array(data_['w_b'])  #生成指数成分股权重矩阵

    ##part1：数据准备
    #1、得到目标函数的系数矩阵
    c=np.array(data_['alpha'])   

    #2、得到约束条件1,2数据：控制组合相对于基准指数的行业和市值因子暴露
    if controlindu==True:
        f = pd.get_dummies(data_,columns=['indu'],drop_first=False)  #得到行业虚拟变量
        f = np.array(f.T)  #将得到的控制因子转化为矩阵形式
    if controlmv==True:
        f_mv = np.array(data_['mv'].T)  #提取因子数据,并转化为矩阵形式
    
    #3、得到约束条件3数据：控制组合相对于基准指数的风格因子偏离
    if len(predfactordf)>0:
        f_pos_name=predfactordf[predfactordf>0].index
        f_neg_name=predfactordf[predfactordf<0].index
        f_eq_name=predfactordf[predfactordf==0].index
        if len(f_pos_name)>0: f_pos=np.array(data_[f_pos_name].T) 
        if len(f_neg_name)>0: f_neg=np.array(data_[f_neg_name].T)
        if len(f_eq_name)>0:f_eq=np.array(data_[f_eq_name].T)
        
    ##part2：进行优化
    fun= lambda w: -w.dot(c) #构建目标函数
    # def fun(w):
    #     res=-(w.dot(c))
    #     fun.count += 1
    #     print('res = ', res, '  j = ', fun.count)
    #     return res
    # fun.count = 0
    
    cons=[{'type': 'eq', 'fun': lambda w: np.sum(w)-wsum,'jac':lambda w:np.ones_like(w_b)}]#构建约束条件6组合权重之和为1

    if controlindu==True:#如果限制行业
        if limitindu==0:
            cons.append({'type': 'eq','fun':lambda w:f.dot(w)-f.dot(w_b)})
        else:
            if absvalue==True: #偏离绝对值
                cons.extend([{'type': 'ineq', 'fun':lambda w: limitindu-(f.dot(w_b)-f.dot(w))}, #构建约束条件1和2的不等式右边
                          {'type': 'ineq', 'fun':lambda w: (f.dot(w)-f.dot(w_b))+limitindu}])  #构建约束条件1和2的不等式左边
            else:#偏离相对值
                cons.extend([{'type': 'ineq', 'fun':lambda w: (1+limitindu)*f.dot(w_b)-f.dot(w)}, #构建约束条件1和2的不等式右边
                          {'type': 'ineq', 'fun':lambda w: f.dot(w)-(1-limitindu)*f.dot(w_b)}])  #构建约束条件1和2的不等式左边
    
    if controlmv==True:#如果限制市值
        if limitmv==0:
            cons.append({'type': 'eq','fun':lambda w:f_mv.dot(w)-f_mv.dot(w_b)})
        else:
            if absvalue==True: #偏离绝对值
                cons.extend([{'type': 'ineq', 'fun':lambda w: limitmv-(f_mv.dot(w_b)-f_mv.dot(w))}, #构建约束条件1和2的不等式右边
                          {'type': 'ineq', 'fun':lambda w: (f_mv.dot(w)-f_mv.dot(w_b))+limitmv}])  #构建约束条件1和2的不等式左边
            else:#偏离相对值
                cons.extend([{'type': 'ineq', 'fun':lambda w: (1+limitmv)*f_mv.dot(w_b)-f_mv.dot(w)}, #构建约束条件1和2的不等式右边
                          {'type': 'ineq', 'fun':lambda w: f_mv.dot(w)-(1-limitmv)*f_mv.dot(w_b)}])  #构建约束条件1和2的不等式左边
    
    if limits>0:#如果限制个股权重
        cons.extend([{'type': 'ineq', 'fun':lambda w: (w-w_b)+limits*np.ones(len(data_))}, #构建约束条件3个股相对于基准指数成分股的偏离，最多不超过0.5%，不等式右边
        {'type': 'ineq', 'fun':lambda w: limits*np.ones(len(data_))-(w-w_b)}]) #构建约束条件3个股相对于基准指数成分股的偏离，最多不超过0.5%，不等式左边
    
    if len(predfactordf)>0:#如果限制风格暴露
        if predfactorvalue==True:#有具体值
            if prefactorwb==True:
                if len(f_pos_name)>0: cons=cons+[{'type': 'ineq', 'fun':lambda w: f_pos.dot(w)-f_pos.dot(w_b)-predfactordf[f_pos_name].abs(),'jac':lambda w:f_pos}]
                if len(f_neg_name)>0: cons=cons+[{'type': 'ineq', 'fun':lambda w: -(f_neg.dot(w)-f_neg.dot(w_b)+predfactordf[f_neg_name].abs()),'jac':lambda w:-f_neg}]
                if len(f_eq_name)>0: cons=cons+[{'type': 'eq', 'fun':lambda w: f_eq.dot(w)-f_eq.dot(w_b)-predfactordf[f_eq_name],'jac':lambda w:f_eq}]
            else:
                if len(f_pos_name)>0: cons=cons+[{'type': 'ineq', 'fun':lambda w: f_pos.dot(w)-predfactordf[f_pos_name].abs(),'jac':lambda w:f_pos}]
                if len(f_neg_name)>0: cons=cons+[{'type': 'ineq', 'fun':lambda w: -(f_neg.dot(w)+predfactordf[f_neg_name].abs()),'jac':lambda w:-f_neg}]
                if len(f_eq_name)>0: cons=cons+[{'type': 'eq', 'fun':lambda w: f_eq.dot(w)-predfactordf[f_eq_name],'jac':lambda w:f_eq}]

        else:#无具体值
            if prefactorwb==True:
                if len(f_pos_name)>0: cons=cons+[{'type': 'ineq', 'fun':lambda w: f_pos.dot(w)-f_pos.dot(w_b),'jac':lambda w:f_pos}]
                if len(f_neg_name)>0: cons=cons+[{'type': 'ineq', 'fun':lambda w: -(f_neg.dot(w)-f_neg.dot(w_b)),'jac':lambda w:-f_neg}]
                if len(f_eq_name)>0: cons=cons+[{'type': 'eq', 'fun':lambda w: f_eq.dot(w)-f_eq.dot(w_b),'jac':lambda w:f_eq}]
            else:
                if len(f_pos_name)>0: cons=cons+[{'type': 'ineq', 'fun':lambda w: f_pos.dot(w),'jac':lambda w:f_pos}]
                if len(f_neg_name)>0: cons=cons+[{'type': 'ineq', 'fun':lambda w: -(f_neg.dot(w)),'jac':lambda w:-f_neg}]
                if len(f_eq_name)>0: cons=cons+[{'type': 'eq', 'fun':lambda w: f_eq.dot(w),'jac':lambda w:f_eq}]

    
    data_['lower_bound']=np.where((data_['alpha']>0)|(data_['w_b']>0),limitlower,0)
    data_['up_bound']=np.where((data_['alpha']>0)|(data_['w_b']>0),limitup,0)
    bound=tuple(zip(data_['lower_bound'],data_['up_bound'])) #建立约束条件4，个股权重上限，及权重的取值范围
    w0=np.ones_like(w_b)/len(w_b) if w0=='' else np.array(w0)
    # w0=np.array(data_['w_b']) if w0=='' else np.array(w0)
    res = minimize(fun,w0,jac=lambda w:-c, method='SLSQP', bounds=bound,constraints=cons,options={'maxiter':1e5,'ftol':1e-4}) 
    
    resultw=pd.Series(res['x'],index=data_.index).reindex(data.index,fill_value=0)
#    res['message']
    return res['success'],resultw,res

if __name__ == '__main__':
    modelid=9
    sysdate=date.today()-timedelta(1)
    sysdate0=sysdate.strftime("%Y%m%d")
    test=pd.read_csv(r"E:\盈科\组合优化\rawdata"+sysdate0+".csv",encoding='gbk')
    test=test.set_index('windcode')
    
    status,rst,res=optimization(test,w0='',wsum=1,controlindu=True,controlmv=False,limitindu=0.01,limitmv=0.01,limits=0,absvalue=True)
    if status==True:
        rst=rst[rst!=0]
        rst.name='Weight'
        rst=pd.DataFrame(rst)
        rst['Trddt']=sysdate
        rst['ModelID']=9
        rst['Dealtime']='09:30:00'
        rst=rst.reset_index()
        dtypedict = {'Trddt':Date,'Windcode': VARCHAR,'Weight': Float,'ModelID': Integer,'Dealtime':Time}        
        code = "delete from MD_PORT where modelid = " + str(modelid) + " and trddt = \'" + sysdate0 + "\'"
        sql.ExecQuery(code, returnif = False)
        rst.to_sql(name = 'MD_PORT', con = engine,if_exists='append', dtype=dtypedict,index = False)
           