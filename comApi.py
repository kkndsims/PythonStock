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
import datetime

warnings.filterwarnings( "ignore" )

vlList                  = [5, 10]
maList                  = [5, 10, 20]
hfList                  = [5, 10, 20, 30]

isMacdMode              = 0     #0:short; 1:long
macdFast                = 24 if isMacdMode == 1 else 12
macdSlow                = 52 if isMacdMode == 1 else 26
macdMidd                = 18 if isMacdMode == 1 else 9

dayAmount               = 4
dayChange               = 3
dyaLevel                = 0.95
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
############################# 板块算法处理买点 ########################
######################################################################
def findPlateBuy(code, name, testFlag, days):
    week                = pd.DataFrame()
    # 日线站上5/10/20线
    flag                = washPlateData(code, name, days)
    if  flag == False : return flag, code, name
    
    if 1 :
        result          = findPlateDaysMacd(code[2:], name, testFlag, days, week, "9PD.")
        if result : return result
        
        week            = getMergData("week", days)
        flag            = findPlateWeekMacd(code[2:], name, testFlag, days, week, "8PW.")
        if result : return result

    return False, code, name
######################################################################
############################# 个股算法处理买点 ########################
######################################################################
def getStockBuy(code, name, endDate, baseInfo, testFlag, hfile, dfile, info):
    global maxch
    findEnab            = 1
    maxch               = len(name) if len(name) > maxch else maxch
    name                = name.rjust(maxch)
    days                = readDataFromFile(code, name, dfile)
    half                = readDataFromFile(code, name, hfile)
    isSeal              = info['封单额'].iloc[-1] != '--  ' or days.grow.iloc[-1] > 9.5
    tolAmount           = float(info['流通市值'].iloc[-1].replace("亿","").replace(" ",""))
    if isSeal and 1 :
        result          = findSealGrow(code[2:], name, testFlag, days, half, tolAmount, "9S.")
        if result : return result

    # 日线站上5/10/20线
    flag                = washDaysData(code, name, days, half)
    if  flag == False : return flag, code, name
    
    week                = getMergData("week", days)
    flag                = washWeekData(code, name, week, days)
    if  flag == False : return flag, code, name

    if findEnab and 1 :
        # 日线dif调整后上涨中继
        result          = findDaysDif(code[2:], name, testFlag, days, week, "8D.")
        if result : return result
        # 周线dif调整后上涨中继
        result          = findWeekDif(code[2:], name, testFlag, days, week, "7W.")
        if result : return result

    if findEnab and 1 :
        if days.tur.iloc[-1] == 0 : return False, code, name
        if week.tur.iloc[-1] == 0 : return False, code, name
        # 周线天量后调整破新高
        result          = findWeekVol2(code[2:], name, testFlag, week, days, "6W.V9")
        if result : return result
        # 周线连续放量且破新高
        result          = findWeekVol1(code[2:], name, testFlag, week, days, "6W.V8")
        if result : return result

    return False, code, name
    sys.exit(0)

############################# 日线清洗数据 ####################
def washDaysData(code, name, days, half) :
    if len(days) < maList[-1] : return False
    # 站上均线
    for ma in maList:
        days['cls'+str(ma)] = days.close.rolling(ma).mean().round(decimals=2)
    if days.close.iloc[-1]  < days['cls5' ].iloc[-1] : return False
    if days.close.iloc[-1]  < days['cls10'].iloc[-1] : return False
    if days.close.iloc[-1]  < days['cls20'].iloc[-1] : return False
    for ma in maList:
        del days['cls'+str(ma)]
        
    days['seal']            = [ 1 if a >= 9.7 else 0 for a in days.grow]
    procTurtleData(days, maList[-1])
    if days.tur.tail(10).sum() == 0 : return False

    # 日线活跃有量或大阳
    days['lvl'] = [ 1 if a >= 3 and b >= 2 else 0 for a,b in zip(days.amount, days.change)]
    flag1       = days.grow.tail(10).where(days.grow >= 5).dropna().sum()
    flag2       = days.lvl.tail(10).sum()
    del days['lvl']
    if not (flag1 and flag2) : return False
    return True
############################# 周线清洗数据 ####################
def washWeekData(code, name, week, days) :
    del week['seal']
    if week.close.iloc[-1] < week.close.iloc[-2] : return False
    
    for ma in maList[:2]:
        week['cls'+str(ma)] = week.close.rolling(ma).mean().round(decimals=2)
    if week.close.iloc[-1]  < week['cls5' ].iloc[-1] : return False
    if week.close.iloc[-1]  < week['cls10'].iloc[-1] : return False
    for ma in maList[:2]:
        del week['cls'+str(ma)]
    
    for ma in vlList:
        week['vol'+str(ma)] = week.volume.rolling(ma).mean().round(decimals=2)
    procTurtleData(week, maList[-1])
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
    if 'change' in df2.columns:
        df2['change']   = data['change'].resample(period,closed="right",label="right").sum().round(decimals=2)  
    if 'change1' in df2.columns:
        df2['change1']  = data['change1'].resample(period,closed="right",label="right").sum().round(decimals=2)      
    df2                 = df2[~df2['volume'].isin([0])]
    df2['grow']         = ((df2['close']  / df2['close'].shift(1) - 1) * 100).round(decimals=2)
    del df2['open']
    df2.reset_index(inplace=True)
    procTurtleData(df2, maList[-1])
    return df2
############################# macd区间 ##########################
def findMacdCross(code, name, testFlag, df, otype):
    procMacdData(df)
    sta             = list(df.cross.where(df.cross ==  1).dropna().index)
    if len(sta) == 0 : return [0, 0, 0]
    P0              = sta[-1]
    adj             = df.cross.iloc[P0:].where(df.cross == -1).dropna().index
    ctn             = df.cross.iloc[P0:].where(df.cross == 2).dropna().index
    P1              = adj[-1] if len(adj) else 0
    P2              = ctn[-1] if len(ctn) else 0
    if testFlag :
        print(code, name, [P0, P1, P2])
    return [P0, P1, P2]
############################# 日线/周线放量区间 ##########################
def findVolCross2(code, name, testFlag, df, wins, otype) :
    # [vol > 5/10, vol < 5/10] ：找出放量到缩量的区间范围
    df['s5']        = df.vol5.shift(1)
    df['s10']       = df.vol10.shift(1)
    df['sta']       = [ 1 if a >= b and c < d else 0 for a,b,c,d in zip(df.vol5, df.vol10, df.s5, df.s10)]
    df['end']       = [-1 if a < b and c >= d else 0 for a,b,c,d in zip(df.vol5, df.vol10, df.s5, df.s10)]
    df['big']       = [1 if a > max(b, c) else 0 for a,b,c in zip(df.volume, df.vol5, df.vol10)]
    sta             = list(df.sta.where(df.sta ==  1).dropna().index)
    end             = list(df.end.where(df.end == -1).dropna().index)
    del df['s5'], df['s10'], df['sta'], df['end']
    #print(code, name, sta, end, df.index[-1])
    if len(sta) == 0 : return [0, 0, 0]
    if len(end) == 0 or end[-1] < sta[-1] : end.append(df.index[-1])
    if end[0] < sta[0] : end.pop(0)
    if (len(sta) != len(end)) :
        print(sta, '\n', end)
        print(code, name, len(sta), len(end))
        sys.exit(0)

    # 找出最近放量区间
    scope           = 7 if otype == "days" else 7
    lens            = df.index[-1]
    for i in range(len(sta)-1, 0, -1) :
        P0          = sta[i]
        P1          = end[i]
        P2          = df.big.iloc[P0:P1].sum()
        if P2 >= scope : return [P0, P1, P2]
        if lens - P0 >= wins : return [0, 0, 0]
    return [0, 0, 0]
############################# 日线/周线放量区间 ##########################
def findVolCross1(code, name, testFlag, df, wins, otype) :
    df['big']       = [1 if a > max(b, c) else 0 for a,b,c in zip(df.volume, df.vol5, df.vol10)]
    df['cntc']      = df.big.rolling(wins, center=False).sum()
    hwins           = wins * 0.7
    end             = list(df.cntc.where(df.cntc >= hwins).dropna().index)
    if len(end) == 0 : return [0, 0]
    P1              = end[-1]
    P2              = int(df.cntc.iloc[P1])
    del df['big'], df['cntc']
    return [P1, P2]
############################# 日线大阳后连涨或回调破新高 #######################
def findSealGrow(code, name, testFlag, df, hf, tolAmount, qos):
    info                = ""
    if df.grow.iloc[-1] < 0 : return ""
        
    P0, P1, P2          = findMacdCross(code, name, testFlag, df, "days")
    if df.dif.iloc[-1]  < df.dea.iloc[-1] : return ""
    if df.dea.iloc[-1]  < 0 : return ""

    wk                  = getMergData("week", df)
    P0, P1, P2          = findMacdCross(code, name, testFlag, df, "week")
    if wk.dif.iloc[-1]  < wk.dea.iloc[-1] : return ""
    if wk.dif.iloc[-1]  < 0 : return ""
    
    df['seal']          = [ 2 if a >= 19.7 else 1 if a >= 9.7 else 0 for a in df.grow]
    change              = df['change'].iloc[-1]
    amount              = df['amount'].iloc[-1]
    close               = wk['close'].tolist()
    minval              = wk.close.tail(20).min()
    grow                = round(close[-1] / minval, 2)
    sealcnt             = df.seal.tail(5).sum()
    waterlvl            = 3 if tolAmount < 50 else 5
    info                = str(qos) + " " + str(change).rjust(5) + "%"
    if grow >= 1.7  : return ""
    if sealcnt >= 4 : return ""   # 连续4板高危
    if amount < waterlvl : return ""

    if True \
    and True :
        result          = [True, code, name.rjust(4), minval, str(amount)+"亿", close[-1], grow, sealcnt, info]
        return result 
    return ""
############################# 日线放量后连涨或回调破新高 #######################
def findDaysDif(code, name, testFlag, df, wk, qos):
    info            = ""
    P0, P1, P2      = findMacdCross(code, name, testFlag, df, "days")
    if P0 == 0 : return ""
    
    close           = df['close'].tolist()
    change          = df['change'].iloc[-1]
    amount          = df['amount'].iloc[-1]
    grow            = round(close[-1] / close[P0-1], 2)
    gaps            = df.index[-1] - P0
    if df.seal.iloc[P0:].sum() == 0 : return ""
    if not (amount >= dayAmount and change >= dayChange) : return ""
    if df.delta.iloc[-1] < df.delta.iloc[-2] : return ""
    if df.delta.iloc[-2] < df.delta.iloc[-3] : return ""

    # dif cross dea <  0轴, days/week/month共振
    # dif cross dea >= 0轴, days/week/month共振
    if (P1 + P2 == 0) or (P1 > 0 and P2 > 0) :
        procMacdData(wk)
        gaps        = df.index[-1] - P2 if P2 else gaps
        grow        = round(wk.close.iloc[-1] / wk.close.tail(20).min(), 2)
        if grow > 1.7 : return ""
        if wk.delta.iloc[-1] < wk.delta.iloc[-2] : return ""
        if wk.dif.iloc[-1] >= wk.dea.iloc[-1] :
            if wk.dea.iloc[-1] >= 0 : 
                mth     = getMergData("month", df)
                info    = str(qos) + "8.DW  " + str(change).rjust(5) + "%"
                procMacdData(mth)
                if mth.delta.iloc[-1] < mth.delta.iloc[-2] : return ""
                if mth.dif.iloc[-1] >= mth.dea.iloc[-1] >= 0 : 
                    info= str(qos) + "9.DWM " + str(change).rjust(5) + "%"
            else :
                if P2 == 0 : return ""
                if df.seal.iloc[P1:].sum() == 0 : return ""
                info    = str(qos) + "7.D   " + str(change).rjust(5) + "%"
    # 日线聚集,缩量新高
    else :
        print(code, name, P0, P1, P2, info)
        info        = str(qos) + "6.D   " + str(change).rjust(5) + "%"
    
    clsmax          = max(max(close[P0:]), max(close[-20:]))
    if close[-1] < clsmax * dyaLevel : return ""
    if info == "" : return ""
    if testFlag:
        print(code, name, P0, P1, P2, info)
    
    if True \
    and True :
        result          = [True, code, name.rjust(4), close[P0], str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""
############################# 周线放量后连涨或回调破新高 #######################
def findWeekDif(code, name, testFlag, df, wk, qos):
    info            = ""
    P0, P1, P2      = findMacdCross(code, name, testFlag, wk, "days")
    if P0 == 0 : return ""
    
    close           = wk['close'].tolist()
    change          = df['change'].iloc[-1]
    amount          = df['amount'].iloc[-1]
    gaps            = wk.index[-1] - P0
    grow            = round(close[-1] / wk.close.tail(max(gaps,20)).min(), 2)
    if df.delta.iloc[-1] < df.delta.iloc[-2] : return ""
    if df.delta.iloc[-2] < df.delta.iloc[-3] : return ""
    if wk.delta.iloc[-1] < wk.delta.iloc[-2] : return ""
    #if close[-1] < wk.close.tail(max(gaps,20)).max() * 0.85 : return ""
    if not (amount >= dayAmount and change >= dayChange) : return ""
    if grow > 1.7 : return ""
    if testFlag :
        print(code, name, P0, P1, P2)

    if  P2 > 0 :
        if P2 < P1 : return ""
        gaps        = wk.index[-1] - P2
        info        = str(qos) + "W  " + str(change).rjust(5) + "%"
    else :
        info        = str(qos) + "M  " + str(change).rjust(5) + "%"
        mth         = getMergData("month", df)
        procMacdData(mth)
        if mth.delta.iloc[-1] < mth.delta.iloc[-2] : return ""
        if mth.dif.iloc[-1] < mth.dea.iloc[-1] : return "" 
        if mth.dif.iloc[-1] < 0 : return "" 
        
    if True \
    and True :
        result          = [True, code, name.rjust(4), close[P0], str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""
############################# 周线天量后新高或回调破新高 #######################
def findWeekVol2(code, name, testFlag, wk, df, qos):
    # 周线天量 ：
    # 1）量价齐升,一波冲顶；（边拉边洗）
    # 2）量能放大,股价不涨, 2/3周后回落调整,再创新高（吸筹洗盘）
    info                = ""
    change              = df['change'].iloc[-1]
    amount              = df['amount'].iloc[-1]
    delta               = 25
    point               = wk.change1.tail(weekOfYear).idxmax() # 使用wk换手率
    gaps                = wk.index[-1] - point
    high                = wk.close.iloc[point-3:point+3].max()
    if gaps > delta : return "" 
    if wk.close.iloc[-1] < high * 0.95 : return ""
    
    minval              = wk.close.iloc[point-delta:].min()
    close               = wk['close'].tolist()
    grow                = round(close[-1] / minval, 2)
    if testFlag :
        print(code, name, gaps, wk.close.iloc[-1], wk.close.iloc[point])
        
    if True \
    and grow < 1.6 \
    and (amount >= dayAmount and change >= dayChange) \
    and True :
        info            = str(qos) + "  " + str(change).rjust(5) + "%"
        result          = [True, code, name.rjust(4), minval, str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""
############################# 周线连续放量 #######################
def findWeekVol1(code, name, testFlag, wk, df, qos):
    info                = ""
    P1, P2              = findVolCross1(code, name, testFlag, wk, 10, "week")
    if P1 == 0 : return ""
    
    change              = df['change'].iloc[-1]
    amount              = df['amount'].iloc[-1]
    close               = wk['close'].tolist()
    gaps                = wk.index[-1] - P1
    minval              = wk.close.tail(max(gaps, 30)).min()
    grow                = round(close[-1] / minval, 2)
    if gaps >= 15 : return ""
    if wk.dif.iloc[-1] < 0 : return ""
    if testFlag :
        print(code, name, P1, P2, wk.index[-1], gaps, grow)
        print(wk.tail(30))
        
    if True \
    and grow < 1.6 \
    and (amount >= dayAmount and change >= dayChange) \
    and True :
        info            = str(qos) + "  " + str(change).rjust(5) + "%"
        result          = [True, code, name.rjust(4), minval, str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""
######################################################################

######################################################################
############################# 板块处理函数 ############################
######################################################################
############################# 大盘复盘 ################################
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
    print("\n%s :: line %3d : ############### 大盘 : "\
    %("comDef", sys._getframe().f_lineno))
    print("成交总额(%.1f)亿, Top%d%% 成交量(%.1f)亿, 换手率（%.1f%%）,涨幅(%.1f%%)" %(summ, percnet, mlevel, clevel, glevel))
    grow10              = len(df[df['grow'] >= 9.7])
    grow05              = len(df[df['grow'] >=   5])
    down10              = len(df[df['grow'] <=-9.7])
    down05              = len(df[df['grow'] <=  -5])
    print("涨停个数(%3d), >=5个数(%3d)" %(grow10, grow05))
    print("跌停个数(%3d), <=5个数(%3d)" %(down10, down05))
def washPlateData(code, name, days) :
    for ma in maList:
        days['cls'+str(ma)] = days.close.rolling(ma).mean().round(decimals=2)
    if days.close.iloc[-1]  < days['cls5' ].iloc[-1] : return False
    if days.close.iloc[-1]  < days['cls10'].iloc[-1] : return False
    if days.close.iloc[-1]  < days['cls20'].iloc[-1] : return False
    for ma in maList:
        del days['cls'+str(ma)]
    procTurtleData(days, maList[-1])
    if days.tur.iloc[-1] == 0 : return False
    return True
def findPlateDaysMacd(code, name, testFlag, df, wk, qos) :
    info                = ""
    P0, P1, P2          = findMacdCross(code, name, testFlag, df, "days")
    if P0 == 0 : return ""
    if df.amount.iloc[-1] < 30 : return ""
    if df.grow.iloc[-1] < 0.5  : return ""

    gaps                = df.index[-1] - P0
    close               = df.close.tolist()
    amount              = df['amount'].iloc[-1]
    info                = str(qos) + "8  "
    if df.delta.iloc[-1] < df.delta.iloc[-2] : return ""
    if df.delta.iloc[-2] < df.delta.iloc[-3] : return ""
    if (P1 > 0 and P2 > 0) :
        gaps            = df.index[-1] - P2 if P2 else gaps
        info            = str(qos) + "9  "
    result              = [True, code, name.rjust(4), close[P0], str(amount)+"亿", close[-1], df.grow.iloc[-1], gaps, info]
    return result
def findPlateWeekMacd(code, name, testFlag, df, wk, qos) :
    info                = ""
    P0, P1, P2          = findMacdCross(code, name, testFlag, wk, "week")
    if P0 == 0 : return ""

    gaps                = wk.index[-1] - P0
    close               = wk.close.tolist()
    amount              = wk['amount'].iloc[-1]
    if wk.delta.iloc[-1] < wk.delta.iloc[-2] : return ""
    if P2 > 0 :
        info            = str(qos) + "9  "
    result              = [True, code, name.rjust(4), close[P0], str(amount)+"亿", close[-1], wk.grow.iloc[-1], gaps, info]
    return result
######################################################################
############################# 板块处理函数 ############################
######################################################################

######################################################################
############################# 个股处理函数 ############################
######################################################################
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
    del df['macd']
    df['sdif']              = df.dif.shift(1)
    df['sdea']              = df.dea.shift(1)
    #df['dgld']              = [1 if (a < b) else 0 for a,b in zip(df.dif, df.dea)]
    #df['dglz']              = [1 if (a < 0) else 0 for a,b in zip(df.dif, df.dea)]
    df['delta']             = df.dif - df.dea
    df['cross']             = [1 if (a<b and c>=d) else -1 if (a>=b and c<d) else 0 \
                              for a,b,c,d in zip(df.sdif, df.sdea, df.dif, df.dea)]
    df['cross']             = [2 if (a==1 and b>=0) else 1 if(a==1 and b<0) else a \
                              for a,b in zip(df.cross, df.dif)]
    #del df['dea']
    del df['sdif'], df['sdea']
######################################################################
############################# 个股处理函数 ############################
######################################################################


