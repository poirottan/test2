# -*- coding: utf-8 -*-
"""
Created on Wed Nov 18 18:00:01 2020

@author: Admin
"""

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
import calendar
from collections import OrderedDict #保持Key的顺序
from collections import Iterable #判断是否可以循环

#tqdm显示进程
from tqdm import tqdm 
tqdm.pandas(desc="my bar!") #保证tqdm在pandas的apply和groupby apply中也可以用
#tqdm(, ncols=1)保证进度条显示在一行

#warning
import warnings
warnings.filterwarnings('ignore')

#调试模块，记录每行运行时间
#from line_profiler import LineProfiler