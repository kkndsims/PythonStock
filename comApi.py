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

dayAmount               = 5
dayChange               = 3
dayClose                = 66

weekOfYear              = 52 * 4
daysOfYear              = 250 * 1.2

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
    isSeal              = info['封单额'].iloc[-1] != '--  '
    tolAmount           = float(info['流通市值'].iloc[-1].replace("亿","").replace(" ",""))
    if isSeal and 1:
        result          = findSealGrow(code[2:], name, testFlag, days, half, tolAmount, "9D.Seal.")
        if result : return result

    # 日线>20/10线
    flag                = washInvalidData(code, name, days, half)
    if  flag == False : return flag, code, name

    if findEnab and 1:
        # 日线连续放量后破新高
        result          = findGreatVolm(code[2:], name, testFlag, days, half, "8DV.GtV")
        if result : return result
        # 周线天量后调整破新高
        result          = findWeekGrow2(code[2:], name, testFlag, days, half, "7W2.GtV")
        if result : return result
        # 周线连续放量后破新高
        result          = findWeekGrow1(code[2:], name, testFlag, days, half, 12, "6W1.GtG")
        if result : return result
        
    if findEnab and 1:  # 大阳后回调破新高
        result          = findAjustGrow(code[2:], name, testFlag, days, half, 7, "5D.GtG.")
        if result : return result    

    return False, code, name
    sys.exit(0)

############################# 日线清洗数据 ####################
def washInvalidData(code, name, days, half) :
    if len(days) < maList[-1] : return False
    # 站上均线
    for ma in maList:
        days['cls'+str(ma)] = days.close.rolling(ma).mean().round(decimals=2)
    if days.close.iloc[-1]  < days['cls5' ].iloc[-1] : return False
    if days.close.iloc[-1]  < days['cls10'].iloc[-1] : return False
    if days.close.iloc[-1]  < days['cls20'].iloc[-1] : return False
    if days.close.iloc[-1]  > dayClose : return False
    days['seal']            = [ 1 if a >= 9.7 else 0 for a in days.grow]
    procTurtleData(days, maList[-1])
    for ma in maList:
        del days['cls'+str(ma)]
    
    # 非超跌反弹,非冲高下跌
    procMacdData(days)
    if days.dif.iloc[-1] < 0 : return False
    #if days.dea.iloc[-1] < 0 : return False
    if days.high.iloc[-1] / days.close.iloc[-1] > 1.05 : return False
    
    # 日线活跃有量或大阳
    change      = days['change'].iloc[-1]
    amount      = days['amount'].iloc[-1]
    flag1       = amount > dayAmount and change >= dayChange
    flag2       = amount > max(dayAmount-1.5, 3) and change >= 3*dayChange
    if not (flag1 or flag2) : return False
    #flag3       = days.grow.iloc[-1] > 5 
    #flag4       = days.tur.iloc[-1]
    #flag5       = True if volcnt == 2 else False
    #if not (flag1 or flag2 or flag3 or flag4 or flag5) : return False
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
    df2['change1']      = data['change1'].resample(period,closed="right",label="right").sum().round(decimals=2)      
    df2                 = df2[~df2['volume'].isin([0])]
    df2['grow']         = ((df2['close']  / df2['close'].shift(1) - 1) * 100).round(decimals=2)
    del df2['open']
    df2.reset_index(inplace=True)
    procTurtleData(df2, maList[-1])
    return df2
############################# 日线/周线放量区间 ##########################
def findCrossVolm(code, name, testFlag, df, otype):
    for ma in vlList:
        df['vol'+str(ma)]   = df.volume.rolling(ma).mean().round(decimals=2)
    # [vol > 5/10, vol < 5/10] ：找出放量到缩量的区间范围
    wins                    = 10 if otype == "days" else 5
    df['bigv']              = [1 if a > max(b, c) else 0 for a,b,c in zip(df.volume, df.vol5, df.vol10)]
    df['vsft']              = df.bigv.shift(-1)
    df['cntc']              = df.bigv.rolling(wins, center=False).sum()
    df['cnts']              = df.cntc.shift(1)
    df['sta']               = [ 1 if a == 1 and b == 0 else 0 for a,b in zip(df.bigv, df.cnts)]
    df['end']               = [-1 if a == 0 and b == 0 and c == 1 else 0 for a,b,c in zip(df.bigv, df.cntc, df.cnts)]
    sta                     = list(df.sta.where(df.sta ==  1).dropna().index)
    end                     = list(df.end.where(df.end == -1).dropna().index)
    for ma in vlList:
        del df['vol'+str(ma)]
    del df['vsft']
    del df['cntc'], df['cnts']
    del df['sta'],  df['end']
    #print(code, name, sta, end, df.index[-1])
    if len(sta) == 0 : return [0, 0, 0]
    if len(end) == 0 or end[-1] < sta[-1] : end.append(df.index[-1]+wins-1)
    if end[0] < sta[0] : end.pop(0)
    if (len(sta) != len(end)) :
        print(sta, '\n', end)
        print(code, name, len(sta), len(end))
        sys.exit(0)

    # 找出一年内最大的成交量区间
    scope                   = daysOfYear if otype == "days" else weekOfYear 
    wtlv                    = 0 if df.index[-1] < scope else df.index[-1] - scope
    tol                     = [[a,b-(wins-1)] if a > wtlv else [0,0] for a,b in zip(sta, end)]
    omt                     = []
    maxchg                  = 0

    for a in tol :
        if a[0] > 0 and a[1] - a[0] > wins and a not in omt : 
            chg     = round(df.change.iloc[a[0] : a[1]+1].sum(), 1)
            maxchg  = chg if chg > maxchg else maxchg
            if chg >= maxchg * 0.95 :
                a.append(chg)
                omt.append(a)
    #print(code, name, omt)
    #print(code, name, tol)
    #sys.exit(0)
    return omt[-1] if len(omt) else [0, 0, 0]
############################# 日线大阳后连涨或回调破新高 #######################
def findSealGrow(code, name, testFlag, df, hf, tolAmount, qos):
    info                = ""
    close               = df['close'].tolist()
    change              = df['change'].iloc[-1]
    amount              = df['amount'].iloc[-1]
    df['seal']          = [ 2 if a >= 19.7 else 1 if a >= 9.7 else 0 for a in df.grow]
    minval              = df.close.tail(30).min()
    minidx              = df.close.tail(30).idxmin()
    sealcnt             = df.seal.iloc[minidx:].sum()
    seal2               = df.seal.tail(2).sum()
    seal3               = df.seal.tail(3).sum()
    grow                = round(close[-1] / minval, 2)
    waterlvl            = 3 if tolAmount < 50 else 5
    #if grow >= 2   : return ""
    if amount < waterlvl : return ""
    # 连续5板高危
    if sealcnt > 5 : return ""
    # 首板放巨量
    if sealcnt == 1 and amount >= 2*waterlvl :
        info            = str(qos) + "9.FS " + str(change).rjust(5) + "%"
    # 3板成妖:有3必有5，有5必有7
    elif seal3 == 3:
        info            = str(qos) + "8.3C " + str(change).rjust(5) + "%"
    # 2板定龙头
    elif seal2 == 2:
        info            = str(qos) + "7.2C " + str(change).rjust(5) + "%"
    else :
        if sealcnt == 1 : return ""
        if grow > 1.6   : return ""
        if close[-1] != max(close[minidx:]) : return ""
        info            = str(qos) + "6.xx" + str(change).rjust(5) + "%"
        #return ""
    
    if info == "" : return ""
    if True \
    and sealcnt != 4 \
    and True :
        result          = [True, code, name.rjust(4), minval, str(amount)+"亿", close[-1], grow, sealcnt, info]
        return result 
    return ""
############################# 日线大阳后连涨或回调破新高 #######################
def findAjustGrow(code, name, testFlag, df, hf, wins, qos):
    info                = ""
    close               = df['close'].tolist()
    change              = df['change'].iloc[-1]
    amount              = df['amount'].iloc[-1]
    minval              = df.close.tail(90).min()
    grow                = round(close[-1] / minval, 2)
    gaps                = df.index[-1] - df.close.tail(90).idxmin()
    procTurtleData(df, 90)
    if df.tur.iloc[-1] == 0 : return ""
    if df.tur.iloc[-2] == 1 : return ""
    if grow > 1.5 : return ""
    flag1               = amount >= dayAmount and change >= dayChange
    flag2               = amount >= dayAmount - 1 and df.grow.iloc[-1] >= wins
    if not (flag1 or flag2) : return ""
    
    info                = str(qos) + "8 " + str(change).rjust(5) + "%"
    if True \
    and True :
        result          = [True, code, name.rjust(4), minval, str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""
############################# 日线放量后连涨或回调破新高 #######################
def findGreatVolm(code, name, testFlag, df, hf, qos):
    info                = ""
    close               = df['close'].tolist()
    if df.tur.iloc[-1] == 0 : return ""
    # 获取最近换手率最高的区域
    gvlist              = findCrossVolm(code, name, testFlag, df, "days")
    if gvlist[0] == 0 : return ""
    sta                 = gvlist[0]
    end                 = gvlist[1]
    clsmax              = df.close.iloc[sta:end+1].max()
    gaps                = df.index[-1] - end
    if gaps > 30 : return ""
    if close[-1] < clsmax : return ""
    if testFlag :
        print(code, name, gvlist, gaps, clsmax, close[-1])
    
    change              = df['change'].iloc[-1]
    amount              = df['amount'].iloc[-1]
    minval              = df.close.iloc[sta-10:].min()
    grow                = round(close[-1] / minval, 2)
    if gaps == 0 :
        info            = str(qos) + "9." + str(change).rjust(5) + "%"
    else :
        info            = str(qos) + "8." + str(change).rjust(5) + "%"
    if True \
    and grow < 1.6 \
    and True :
        result          = [True, code, name.rjust(4), clsmax, str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""
############################# 周线天量后新高或回调破新高 #######################
def findWeekGrow2(code, name, testFlag, df, hf, qos):
    # 周线天量 ：
    # 1）量价齐声,一波冲顶；（边拉边洗）
    # 2）量能放大,股价不涨, 2/3周后回落调整,再创新高（吸筹洗盘）
    info                = ""
    change              = df['change'].iloc[-1]
    amount              = df['amount'].iloc[-1]
    if df.tur.iloc[-1] == 0 : return ""
    
    wins                = weekOfYear
    delta               = 25
    week                = getMergData("week", df)
    point               = week.change1.tail(wins).idxmax() # 使用week换手率
    high                = week.close.iloc[point-3:point+3].max()
    gaps                = week.index[-1] - point
    if gaps > delta : return ""
    if week.close.iloc[-1] < high : return ""
    
    minval              = week.close.iloc[point-10:].min()
    peak                = week.close.iloc[point-10:].idxmax()
    close               = week['close'].tolist()
    grow                = round(close[-1] / minval, 2)
    # 周线没有冲高回撤

    if week.high.iloc[-1] / close[-1] > 1.07 : return ""
    if testFlag :
        print(code, name, gaps, week.close.iloc[-1], week.close.iloc[point])
        print(code, name, peak, week.index[-1], grow)

    if gaps == 0:
        info            = str(qos) + "9." + str(change).rjust(5) + "%"
        if grow > 2.5 : return ""  # 谨防滞涨出货
    else : 
        info            = str(qos) + "8." + str(change).rjust(5) + "%"
        if grow > 2 : return ""    # 调整破新高 
    if info == "" : return ""
    
    if True \
    and True :
        result          = [True, code, name.rjust(4), close[peak], str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""
############################# 周线连续放量后破新高 #######################
def findWeekGrow1(code, name, testFlag, df, hf, wins, qos):
    info                = ""
    change              = df['change'].iloc[-1]
    amount              = df['amount'].iloc[-1]
    if df.tur.iloc[-1] == 0 : return ""
    
    wk                  = getMergData("week", df)
    delta               = 25
    close               = wk['close'].tolist()
    if wk.high.iloc[-1] / wk.close.iloc[-1] > 1.07 : return ""
    # 获取最近换手率最高的区域
    gvlist              = findCrossVolm(code, name, testFlag, wk, "week")
    if gvlist[0] == 0 : return ""
    sta                 = gvlist[0]
    end                 = gvlist[1]
    gaps                = wk.index[-1] - end
    clsmax              = wk.close.iloc[sta:end+1].max()
    minval              = wk.close.tail(gaps+10).min()
    grow                = round(close[-1] / minval, 2)
    if gaps > delta : return ""
    if close[-1] < clsmax : return ""
    if grow > 1.6 : return ""
    if testFlag :
        print(code, name, gvlist, gaps, clsmax, close[-1])

    if gaps == 0 :
        info            = str(qos) + "9." + str(change).rjust(5) + "%"
    else :
        info            = str(qos) + "8." + str(change).rjust(5) + "%"
    if True \
    and True :
        result          = [True, code, name.rjust(4), clsmax, str(amount)+"亿", close[-1], grow, gaps, info]
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


