# -*- coding: utf-8 -*-
# -*- coding:gb2312 -*-

"""
Created on Mon Oct 10 22:59:41 2016

@author: Administrator
"""

import os
import sys
import pandas as pd
import numpy  as np
import warnings
#from interval import Interval

warnings.filterwarnings( "ignore" )

vlList                  = [5, 10]
maList                  = [5, 10, 20]
hfList                  = [5, 10, 20, 30]

isMacdMode              = 0     #0:short; 1:long
macdFast                = 24 if isMacdMode == 1 else 12
macdSlow                = 52 if isMacdMode == 1 else 26
macdMidd                = 18 if isMacdMode == 1 else 9

topAmount               = 0
topChange               = 0
topGrow                 = 0

maxch                   = 0
empty                   = pd.DataFrame()

percnet                 = 10
mlevel                  = 0
clevel                  = 0
glevel                  = 0
times                   = 2

######################################################################
############################# 各种算法处理买点 ########################
######################################################################
def getStockBuy(code, name, endDate, baseInfo, testFlag, hfile, dfile, info):
    global maxch
    findEnab            = 1
    maxch               = len(name) if len(name) > maxch else maxch
    name                = name.rjust(maxch)
    days                = readDataFromFile(code, name, dfile)
    half                = readDataFromFile(code, name, hfile)
    # 日线>20/10线
    flag                = washInvalidData(code, name, days, half)
    if  flag == False : return flag, code, name
    
    if findEnab and 1:  # 涨停/大阳后回调破新高
        result          = findAjustGrow(code[2:], name, testFlag, days, half, 9.7, "9D.Seal.")
        if result : return result
        result          = findAjustGrow(code[2:], name, testFlag, days, half, 7.0, "8D.GtG.")
        if result : return result
        result          = findWeekGrow1(code[2:], name, testFlag, days, half, 10, "7W.GtG")
        if result : return result  

    if findEnab and 1:  # 天量后调整不破
        result          = findGreatVolm(code[2:], name, testFlag, days, half, "6D.GtV")
        if result : return result
        result          = findWeekGrow2(code[2:], name, testFlag, days, half, "5W.GtV")
        if result : return result  

    return False, code, name
    sys.exit(0)

############################# 日线/half清洗数据 ####################
def washInvalidData(code, name, days, half) :
    if len(days) < maList[-1] : return False
    for ma in maList:
        days['cls'+str(ma)] = days.close.rolling(ma).mean().round(decimals=2)
    if days.close.iloc[-1]  < days['cls20'].iloc[-1] : return False
    if days.close.iloc[-1]  < days['cls10'].iloc[-1] : return False
    days['seal']            = [ 1 if a >= 9.5 else 0 for a in days.grow]
    procTurtleData(days, maList[-1])
    return True
############################# 合并月/周线数据 ######################
def getMergData(otype, data) :
    period              = "W" if otype == "week" else "M"
    data['date']        = pd.to_datetime(data['date']) #把字符串转换成时间信息    
    data                = data.set_index('date')
    df2                 = data.resample          (period,closed="right",label="right").last()
    #df2['open']         = data['open'].resample  (period,closed="right",label="right").first().round(decimals=2) 
    df2['high']         = data['high'].resample  (period,closed="right",label="right").max().round(decimals=2) 
    df2['low']          = data['low'].resample   (period,closed="right",label="right").min().round(decimals=2) 
    df2['close']        = data['close' ].resample(period,closed="right",label="right").last().round(decimals=2)    
    df2['volume']       = data['volume'].resample(period,closed="right",label="right").sum().round(decimals=1)  
    df2['amount']       = data['amount'].resample(period,closed="right",label="right").sum().round(decimals=1)
    df2['change']       = data['change'].resample(period,closed="right",label="right").sum().round(decimals=2)       
    df2                 = df2[~df2['volume'].isin([0])]
    df2['grow']         = ((df2['close']  / df2['close'].shift(1) - 1) * 100).round(decimals=2)
    del df2['open']
    df2.reset_index(inplace=True)
    return df2
############################# 获取fireup ##########################
def findGreatGrow(code, name, testFlag, df, delta):
    df['gtg']           = (df.close.rolling(delta).max() / df.close.shift(delta)).round(decimals=2)
    df['sln']           = (df.seal.rolling(delta).sum())
    df['crs']           = [1 if a >= 1.2 and b >= 1 else 0 for a,b in zip(df.gtg, df.grow)]
    gtlist              = df.crs.where(df.crs == 1).dropna().index
    #del df['open'], df['high'], df['low']
    return gtlist
def findCrossVolm(code, name, testFlag, df, delta):
    for ma in vlList:
        df['vol'+str(ma)]   = df.volume.rolling(ma).mean().round(decimals=2)
    df['vrate']             = (df.vol5 / df.vol10).round(decimals=2)
    df['vrmax']             = df.vrate == df.vrate.rolling(vlList[-1]).max()
    df['vrmax']             = [1 if a and b >= delta else 0 for a,b in zip(df.vrmax, df.vrate)]
    df['maxc']              = df.close.rolling(vlList[-1]).max()
    gvlist                  = df.vrmax.where(df.vrmax == 1).dropna().index
    for ma in vlList:
        del df['vol'+str(ma)]
    return gvlist
############################# 日线大涨后调整 #######################
def findAjustGrow(code, name, testFlag, df, hf, wins, qos):
    info                = ""
    slist               = df.grow.where(df.grow >= wins).dropna().index
    if len(slist) == 0 : return ""
    point               = slist[-1]
    gaps                = df.index[-1] - point
    if gaps > 21 : return ""
    if df.grow.iloc[-1] < 0 : return ""
    
    alist               = df.grow.iloc[point:].where(df.grow < 0).dropna().index
    close               = df['close'].tolist()
    change              = df['change'].iloc[-1]
    amount              = df['amount'].iloc[-1]
    minval              = df.close.tail(90).min()
    grow                = round(close[-1] / minval, 2)
    if len(alist) == 0 :
        info            = str(qos) + "9 " + str(change).rjust(5) + "%"
        if df.grow.tail(9).where(df.grow >= wins).dropna().count() == 1 : return ""
        if amount < 4 or change < 2 : return ""
    else :
        info            = str(qos) + "8 " + str(change).rjust(5) + "%"
        idx             = alist[0]
        if close[-1] < close[idx] : return ""
        if df.index[-1] == idx : return ""
        if not (amount >= 4 and change > 2) : return ""

    if True \
    and True :
        result          = [True, code, name.rjust(4), close[point], str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""
def findWeekGrow1(code, name, testFlag, df, hf, wins, qos):
    info                = ""
    wk                  = getMergData("week", df)
    slist               = wk.grow.where(wk.grow >= wins).dropna().index
    if len(slist) == 0 : return ""
    point               = slist[-1]
    gaps                = wk.index[-1] - point
    if gaps > 11 : return ""
    if wk.grow.iloc[-1] < 0 : return ""
    if df.grow.iloc[-1] < 0 : return ""
    
    alist               = wk.grow.iloc[point:].where(wk.grow < 0).dropna().index
    close               = wk['close'].tolist()
    change              = df['change'].iloc[-1]
    amount              = df['amount'].iloc[-1]
    minval              = df.close.tail(90).min()
    grow                = round(close[-1] / minval, 2)
    if len(alist) == 0 :
        info            = str(qos) + "9 " + str(change).rjust(5) + "%"
        if len(slist) == 1 : return ""
        if close[-1] < close[slist[-2]] : return ""
    else :
        info            = str(qos) + "8 " + str(change).rjust(5) + "%"
        idx             = alist[0]
        if close[-1] < close[idx] : return ""
        if df.index[-1] == idx : return ""
        if testFlag :
            print(code, name, close[idx], close[-1])
            print(df.grow.iloc[point:])

    if True \
    and (amount >= 4 and change >= 2)\
    and True :
        result          = [True, code, name.rjust(4), close[point], str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""
############################# 成就放量后调整 #######################
def findGreatVolm(code, name, testFlag, df, hf, qos):
    info                = ""
    gvlist              = findCrossVolm(code, name, testFlag, df, 1.5)
    if len(gvlist) == 0 : return ""
    close               = df['close'].tolist()
    change              = df['change'].iloc[-1]
    amount              = df['amount'].iloc[-1]
    minval              = df.close.tail(90).min()
    grow                = round(close[-1] / minval, 2)
    point               = gvlist[-1] 
    gaps                = df.index[-1] - point
    
    sumchange           = df.change.iloc[point-2:point+1].sum()
    summoney            = df.amount.iloc[point-2:point+1].sum()
    if gaps > 30 : return ""
    if close[-1] < df.cls5.iloc[-1] : return ""
    if not (sumchange >= 8 or summoney >= 10) : return ""
    if df.grow.iloc[-1] <= -5 : return ""
    
    peak                = df.close.iloc[point-5:].idxmax()
    if peak == df.index[-1] :
        info            = str(qos) + "9." + str(change).rjust(5) + "%"
    else :
        info            = str(qos) + "8." + str(change).rjust(5) + "%"
        if close[-1] <= close[peak+1] : return ""
    if info == "" : return ""
    
    if True \
    and ((amount > 4 and change > 2) or (amount > 3 and df.seal.iloc[-1])) \
    and (grow < 1.5 or (grow >= 1.5 and df.tur.iloc[-1])) \
    and True :
        result          = [True, code, name.rjust(4), close[peak], str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""
def findWeekGrow2(code, name, testFlag, df, hf, qos):
    info                = ""
    week                = getMergData("week", df)
    gvlist              = findCrossVolm(code, name, testFlag, week, 1.5)
    if len(gvlist) == 0 : return ""
    close               = week['close'].tolist()
    change              = week['change'].iloc[-1]
    amount              = week['amount'].iloc[-1]
    point               = gvlist[-1] 
    gaps                = week.index[-1] - point
    minval              = week.close.iloc[point-10:].min()
    grow                = round(close[-1] / minval, 2)
    
    if gaps > 30 : return ""
    if close[-1] < df.cls5.iloc[-1] : return ""
    if df.grow.iloc[-1] <= -5 : return ""
    
    peak                = week.close.iloc[point-5:].idxmax()
    if peak == week.index[-1] :
        info            = str(qos) + "9." + str(change).rjust(5) + "%"
    else :
        info            = str(qos) + "8." + str(change).rjust(5) + "%"
        if close[-1] <= close[peak+1] : return ""
    if info == "" : return ""
    
    if True \
    and ((amount > 4 and change > 2) or (amount > 3 and df.seal.iloc[-1])) \
    and (grow < 1.5 or (grow >= 1.5 and df.tur.iloc[-1])) \
    and True :
        result          = [True, code, name.rjust(4), close[peak], str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""


######################################################################
############################# 基础板块函数 ############################
######################################################################
def findPlateBuy(code, name, testFlag, days):
    info                    = ""
    close                   = days['close'].tolist()
    amount                  = days['amount'].iloc[-1]
    sclose                  = strClose(close[-1])
    lens                    = len(days)
    minidx                  = days['close'].tail(250).idxmin()
    grow                    = round(close[-1] / close[minidx], 2)
    gaps                    = lens - minidx
    if lens < 30:
        return False, code, name
    # 找出年线最低点
    if gaps <= 5 :
        info                = "9.9 最低点"
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", close[minidx], grow, gaps, info]
        return result

    # 找出最近一次放量后，调整出现低点      
    vlist                   = days.where(days.v == -1).dropna().index
    point                   = vlist[-1] if len(vlist) else 0
    idx                     = days['amount'].iloc[:point].tail(20).idxmax()    
    grow                    = round(close[-1] / close[idx], 2) 
    gaps                    = lens - point
    if testFlag :
        print(code, name, idx, point, close[idx], close[point], close[-1], grow, gaps)
        print(days.tail(30))
    if close[-1] >= close[idx] and gaps <= 5:
        info                = "9.8 调整结束"
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", close[point], grow, gaps, info]
        return result
    # 找出5日成交量低点
    mlist                   = days.where(days.m == -1).dropna().index
    if 0 == len(mlist):
        print(code, name, lens)
        return False, code, name
    mpoint                  = mlist[-1] if len(mlist) else 0
    midx                    = days['amount'].iloc[:mpoint].tail(20).idxmax()    
    grow                    = round(close[-1] / close[midx], 2) 
    gaps                    = lens - mpoint
    if close[-1] >= close[midx] and gaps <= 5:
        info                = "9.6 缩量结束"
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", close[point], grow, gaps, info]
        return result
    # 海归
    midx                    = days['close'].tail(60).idxmin()    
    grow                    = round(close[-1] / close[midx], 2) 
    if days['tur'].iloc[-1] == 1 :
        info                = "9.4 海归"
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", close[point], grow, gaps, info]
        return result
    return False, code, name
######################################################################
############################# 从文件读数据 ############################
def strClose(close):
    return str(close).rjust(6)
def readDataFromFile(code, name, path):
    if not os.path.exists(path) :
        return empty
    data                    = pd.read_csv(path, parse_dates=[0])
    data.sort_values('date', ascending=True, inplace=True)
    return data
def procTurtleData(df, period) :
    # apply函数仿真时间是队列操作的3倍
    df['tur']               = df.close == df.close.rolling(period).max()
    df['tur']               = [1 if a else 0 for a in (df.tur)]
def procMaData(df) :
    for ma in maList:
        df['cls'+str(ma)]   = df.close.rolling(ma).mean().round(decimals=2)
def delMaData(df) :
    for ma in maList:
        del df['cls'+str(ma)]
def procMacdData(df) :                         
    df['sema']              = pd.DataFrame.ewm(df.close,span=macdFast).mean().round(decimals=6) 
    df['lema']              = pd.DataFrame.ewm(df.close,span=macdSlow).mean().round(decimals=6)   
    df['dif']               = (df.sema - df.lema).round(decimals=3)       
    df['dea']               = pd.DataFrame.ewm(df.dif,span=macdMidd).mean().round(decimals=3)
    df['macd']              = 2*(df.dif - df.dea).round(decimals=2)
    del df['sema'], df['lema']
    #del df['macd']
    df['dlt']               = df.dif.shift(-1)
    #df['cross']             = [1 if (a<c and b>=c) else -1 if (a>0 and c>0 and a>=c and b<c) else 0 for a,b,c in zip(df.dif, df.dlt, df.dea)]
    df['cross']             = [1 if ((a<=0 or c<=0) and a<c and b>=c) else \
                              -1 if ((a>0 and c>0) and a>=c and b<c)  else \
                               0 for a,b,c in zip(df.dif, df.dlt, df.dea)]
    del df['dlt']
def getDaysReplay(endDate, baseFile):
    global percnet, mlevel, clevel, glevel
    global topAmount, topChange, topGrow
    df                  = pd.read_csv(baseFile, sep='\t', encoding='gbk')
    df.drop(df.tail(1).index, inplace=True)             # drop last rows
    df['amount']        = round(df['总金额'] / 10000, 2)
    df['change']        = [0 if a == '--  ' else float(a) for a in df['换手%']]
    df['grow']          = [0 if a == '--  ' else float(a) for a in df['涨幅%']]
    summ                = df['amount'].sum()
    mlevel              = np.percentile(df.amount, 100 - percnet)
    clevel              = np.percentile(df.change, 100 - percnet)
    glevel              = np.percentile(df.grow,   100 - percnet)
    topAmount           = mlevel
    topChange           = clevel
    topGrow             = glevel
    print("成交总额(%.1f)亿, Top%d%% 成交量(%.1f)亿, 换手率（%.1f%%）,涨幅(%.1f%%)" %(summ, percnet, mlevel, clevel, glevel))
    grow10              = len(df[df['grow'] >= 9.7])
    grow05              = len(df[df['grow'] >=   5])
    down10              = len(df[df['grow'] <=-9.7])
    down05              = len(df[df['grow'] <=  -5])
    print("涨停个数(%3d), >=5个数(%3d)" %(grow10, grow05))
    print("跌停个数(%3d), <=5个数(%3d)" %(down10, down05))
#####################################################################
############################# 基础函数结束 ###########################
#####################################################################


