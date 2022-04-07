# -*- coding: utf-8 -*-
"""
Created on Sun Apr 11 18:45:48 2021

@author: sims
"""
import sys, datetime, os
import pandas  as pd

from   comDef  import *
from   comApi  import *

plateFile               = workpath + "行业板块.xls"                 #行业板块
idealFile               = workpath + "概念板块.xls"                 #概念板块
plateList               = []
plateName               = []

stock                   = []
plate                   = []

amountUnit              = 100000000  #1亿
summ                    = 0
midd                    = 0
avrg                    = 0
midd1                   = 0
avrg1                   = 0
upnum                   = 0 
dwnum                   = 0 
flnum                   = 0 
grow10                  = 0 
grow05                  = 0 
down10                  = 0 
down05                  = 0 
######################################################################
############################# 股票复盘 ###############################
######################################################################
def getPlateCode() : 
    global plateList, plateName
    if not os.path.exists(plateFile) :
        print("%s :: line %3d : ############### %s not exists"\
        %("comDef", sys._getframe().f_lineno, plateFile))
        sys.exit(0)
    if not os.path.exists(idealFile) :
        print("%s :: line %3d : ############### %s not exists"\
        %("comDef", sys._getframe().f_lineno, idealFile))
        sys.exit(0)
                
    dfp                 = pd.read_csv(plateFile, sep='\t', encoding='gbk')   
    dfi                 = pd.read_csv(idealFile, sep='\t', encoding='gbk') 
    dfp.drop([len(dfp)-1], inplace=True)
    dfi.drop([len(dfi)-1], inplace=True)
    dfp                 = dfp.loc[:,['代码', '名称']]
    dfi                 = dfi.loc[:,['代码', '名称']]
    df                  = pd.concat([dfp,dfi], ignore_index=True)
    code                = df['代码'].tolist()
    newlist             = list(map(lambda x: ['SH'+ x[2:8]], code))
    df['代码']           = pd.DataFrame(newlist)
    for i in range(len(df)):
        plateList.append(df['代码'].iloc[i]) 
        plateName.append(df['名称'].iloc[i]) 
    print("%s :: line %3d : ############### 行业板块 = %d, 概念板块 = %d, 总计 = %d\n"\
    %("comDef", sys._getframe().f_lineno, len(dfp), len(dfi), len(df)))  
        
def getDaysReplay(endDate, iputList, iputName):
    global rsltpath, codeList, codeName, plateList, plateName  
    replay_stock        = rsltpath + endDate + "\\replay_stock.csv"
    dfs                 = pd.read_csv(replay_stock, encoding='gbk')
    print("\n%s :: line %3d : ############### update replay with stockNum = %d"\
    %("plateApi", sys._getframe().f_lineno, len(dfs)))  
    
    dfs['amount']       = round(dfs['amount'] / amountUnit, 2)  
    summ                = dfs['amount'].sum()
    midd                = dfs['amount'].median()
    avrg                = dfs['amount'].mean()
    print("成交金额 = %.1f亿, 中位数 = %.1f亿, 平均 = %.1f亿" %(summ, midd, avrg))
    midd1               = dfs['grow'].median()
    avrg1               = dfs['grow'].mean()
    upnum               = len(dfs[dfs['grow'] >  0.0])
    mount10             = len(dfs[dfs['amount']> 10.0])
    mount5              = len(dfs[dfs['amount']> 5.0])
    dwnum               = len(dfs[dfs['grow'] <  0.0])
    flnum               = len(dfs[dfs['grow'] == 0.0])
    grow10              = len(dfs[dfs['grow'] >= 9.7])
    grow05              = len(dfs[dfs['grow'] >=   5]) 
    down10              = len(dfs[dfs['grow'] <=-9.7])
    down05              = len(dfs[dfs['grow'] <=  -5]) 
    print("上涨个数 = %4d, 下跌个数   = %4d, 平盘个数 = %3d, 涨幅中位数 = %.1f,平均 = %.1f" %(upnum, dwnum, flnum, midd1, avrg1))
    print("涨停个数 = %4d, 涨幅>5个数 = %4d" %(grow10, grow05))
    print("跌停个数 = %4d, 跌幅>5个数 = %4d" %(down10, down05))
    print("成交量   > %4d 亿个数 = %4d"        %(10, mount10))
    print("成交量   > %4d 亿个数 = %4d"        %(5,  mount5))
        
def getPlateImage(endDate, codeList, codeName, testFlag, testCode):
    if testFlag:
        iputList    = list(map(lambda x: [x, codeName[codeList.index(x)], endDate, "板块", testFlag], testCode))
    else:
        iputList    = list(map(lambda x,y: [x,y,endDate, "板块", testFlag], codeList, codeName))
    print("%s :: line %3d : ############### 处理板块 with codeNum = %d"\
    %("comDef", sys._getframe().f_lineno, len(iputList))) 
    starttime           = datetime.datetime.now()
    update_succ_num     = 0
    update_grow_list    = []
    update_turl_list    = []
    update_poke_list    = []
    for i in range (len(iputList)) :
        code            = iputList[i][0]
        name            = iputList[i][1]
        endDate         = iputList[i][2]
        testFlag        = iputList[i][4]
        daysfile        = dayspath + code
        if not os.path.exists(daysfile) :
            continue
        
        data            = pd.read_csv(daysfile, parse_dates=[0])
        data            = data[~data['volume'].isin([0])] 
        data.sort_values('date', ascending=True, inplace=True)
        data            = procMaData(data)
        data            = procCciData(data)
        data['grow']    = ((data['close'] / data['close'].shift(1) - 1) * 100)
        data['vrate']   = ((data['amount']/ data['amount'].shift(1) - 1) * 100)
        
        rst             = getPlateGrow(code, name, testFlag, data)
        if str(rst[0]) == 'True':
            update_grow_list.append(rst)
            update_succ_num     += 1
            
        rst             = getPlateTurtle(code, name, testFlag, data)
        if str(rst[0]) == 'True':
            update_turl_list.append(rst)
            update_succ_num     += 1
            
        rst             = getPlateBottle(code, name, testFlag, data)
        if str(rst[0]) == 'True':
            update_poke_list.append(rst)
            update_succ_num     += 1
            
    endtime             = datetime.datetime.now()
    print("%s :: line %3d : update_succ_num = %d，use time = %s Second"\
    %("comDef", sys._getframe().f_lineno, update_succ_num, (endtime - starttime).seconds))
        
    if update_grow_list :
        df              = pd.DataFrame(update_grow_list)
        df.columns      = ['flag', 'code', 'name', 'Bot', 'Top', 'cci', 'growAll', 'amount', 'days', 'grow', 'info']
        df.drop(['flag'], axis=1, inplace=True)
        df.sort_values('grow', ascending=False, inplace=True)
        print("%s :: line %3d : ############### 板块涨幅大于2.5%% num = %d"\
        %("comDef", sys._getframe().f_lineno, len(update_grow_list))) 
        print (df)
        
    for i in range(len(update_grow_list)):
        for j in range(len(update_turl_list)):
            #print (len(update_grow_list), i, len(update_turl_list), j)
            if update_grow_list[i][1] == update_turl_list[j][1]:
                #print("delete same item = %s" %(update_turl_list[j]))
                del update_turl_list[j]
                break
    if update_turl_list :
        df              = pd.DataFrame(update_turl_list)
        df.columns      = ['flag', 'code', 'name', 'Bot', 'Top', 'cci', 'growAll', 'amount', 'days', 'grow', 'info']
        df.drop(['flag'], axis=1, inplace=True)
        df.sort_values('grow', ascending=False, inplace=True)
        print("%s :: line %3d : ############### 板块海归 num = %d ###############"\
        %("comDef", sys._getframe().f_lineno, len(update_turl_list))) 
        print (df)     
        
    total               = update_grow_list + update_turl_list
    #print(len(update_grow_list), len(update_turl_list), len(total))
    platefile           = rsltpath + endDate + "\\plategrow.csv"
    df                  = pd.DataFrame(total)
    df.columns          = ['flag', 'code', 'name', 'Bot', 'Top', 'cci', 'growAll', 'amount', 'days', 'grow', 'info']
    df.drop(['flag'], axis=1, inplace=True)
    del df['cci']
    #df.sort_values('grow', ascending=False, inplace=True)
    df.to_csv(platefile, index=False, encoding="GBK")
    
    for i in range(len(total)):
        for j in range(len(update_poke_list)):
            if total[i][1] == update_poke_list[j][1]:
                del update_poke_list[j]
                break
    if update_poke_list :
        df              = pd.DataFrame(update_poke_list)
        df.columns      = ['flag', 'code', 'name', 'Bot', 'Top', 'cci', 'growAll', 'amount', 'days', 'grow', 'info']
        df.drop(['flag'], axis=1, inplace=True)
        del df['cci']
        df.sort_values('grow', ascending=False, inplace=True)
        print("%s :: line %3d : ############### 板块低点 num = %d ###############"\
        %("comDef", sys._getframe().f_lineno, len(update_poke_list)))
        print (df)  

        
######################################################################
############################# 寻找板块日线涨幅 ########################
######################################################################        
def getPlateGrow(code, name, testFlag, data):
    findFlag        = False
    minidx          = data['close'].iloc[-21:].idxmin()
    mincls          = data['close'].iloc[-21:].min()
    close           = list(map(lambda x : round(x, 2), data['close'].tolist()))
    grow0           = round(close[-1] / mincls, 2)
    cci             = round(data['cci'].iloc[-1], 1)
    amount          = round(data['amount'].iloc[-1], 1)
    grow1           = round(data['grow'].iloc[-1], 1)
    days            = len(data) - minidx - 1
    
    if  data['grow'].iloc[-1] >= 2.5 \
    and True :
        findFlag    = True
        result      = [findFlag, code, name, mincls, close[-1], cci, grow0, str(amount)+"亿", days, grow1, "日涨幅"]
        return result
    return findFlag, code, name
######################################################################
############################# 寻找板块海归 ############################
######################################################################
def getPlateTurtle(code, name, testFlag, data):
    findFlag        = False
    close           = list(map(lambda x : round(x, 2), data['close'].tolist()))  
    minidx          = data['close'].iloc[-21:].idxmin()
    mincls          = data['close'].iloc[-21:].min()
    cci             = round(data['cci'].iloc[-1], 1)
    amount          = round(data['amount'].iloc[-1], 1)
    grow1           = round(data['grow'].iloc[-1], 1)
    
    if  close[-1] == data['close'].iloc[-21:].max()\
    and close[-1] >= data['ma_5'].iloc[-1]\
    and close[-1] >= data['ma_10'].iloc[-1]\
    and grow1 >= 1.5\
    and True:
        days        = len(data) - minidx - 1
        grow0       = round(close[-1]  / mincls, 2)
        findFlag    = True
        result      = [findFlag, code, name, mincls, close[-1], cci, grow0, str(amount)+"亿", days, grow1, "海归"]
        return result
    return findFlag, code, name
######################################################################
############################# 寻找板块低点 ############################
######################################################################        
def getPlateBottle(code, name, testFlag, data):
    findFlag        = False
    close           = list(map(lambda x : round(x, 2), data['close'].tolist()))         
    rst             = procPeakList(close)
    peak            = procBotTopValue(rst)
    botIndx         = peak[0]
    lastBot         = peak[1]
    lastTop         = peak[3]
    
    days            = len(data) - botIndx - 1
    grow0           = round(close[-1]/lastBot, 2) 
    cci             = round(data['cci'].iloc[-1], 1)
    amount          = round(data['amount'].iloc[-1], 1)
    grow1           = round(data['grow'].iloc[-1], 1)
                
    if  days <= 10 \
    and close[-1] >= lastTop \
    and grow1 >= 1.5\
    and True:
        findFlag    = True
        result      = [findFlag, code, name, lastTop, close[-1], cci, grow0, str(amount)+"亿", days, grow1, "低点"]
        return result
    return findFlag, code, name