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
warnings.filterwarnings( "ignore" )

maList                  = [5, 20, 60, 144] 
amList                  = [5, 10]    
wkList                  = [5, 10, 20, 30]
mtList                  = [5, 10]

isMacdMode              = 0     #0:short; 1:long
macdFast                = 24 if isMacdMode == 1 else 12
macdSlow                = 52 if isMacdMode == 1 else 26
macdMidd                = 18 if isMacdMode == 1 else 9

daysQos                 = 9
weekQos                 = daysQos - 1
mothQos                 = weekQos - 1

maxch                   = 0
empty                   = pd.DataFrame()

######################################################################
############################# 各种算法处理买点 ########################
######################################################################
def getStockBuy(code, name, baseInfo, testFlag, mfile, dfile, wfile, info):
    global maxch
    mothEnab            = 0
    weekEnab            = 0
    daysEnab            = 1
    findEnab            = 1
    maxch               = len(name) if len(name) > maxch else maxch
    name                = name.rjust(maxch)
    days                = readDataFromFile(code, name, dfile)
    if len(days) == 0:
        return False, code, name
    
    #日线：涨停后缩量调整结束， 连板， 反包等处理 
    if daysEnab :
        # 保留最近有涨停，或者成交活跃的个股
        if not washTodayData(days, 20, testFlag): return False, code, name
        
        if findEnab :                   # 连板 + 反包
            result      = findSealing(code[2:], name, testFlag, days, info, 9)
            if result : return result
        if findEnab :                   # 盘整结束破前高
            result      = findDaysContinue(code[2:], name, testFlag, days, 120, 20, 8)
            if result : return result
        #if findEnab and 0:
        #    result      = findDaysDif(code[2:], name, testFlag, days, 120, 20, "9.6 D.DIF_")
        #    if result : return result  
           
    if mothEnab or weekEnab :
        #去除成交不足或不够活跃的个股
        if not washLongData(days, 20, testFlag): return False, code, name
        #周线：4个月内周线破1年历史放量，盘整破成本线
        if weekEnab :
            week        = getMergData("week", days)
            result      = getLongMaxVolPoint(code[2:], name, testFlag, week,  50, 16, 1.3, '8.9 W.V_')
            if result : return result
        #月线：2年内有月线破5年历史量，盘整破成本线
        if mothEnab :
            month       = getMergData("month", days)
            result      = getLongMaxVolPoint(code[2:], name, testFlag, month, 60, 20, 1.3, '7.9 M.V_')
            #result      = findMonthTurtle(code[2:], name, testFlag, data, week, days, 20, mothQos)
            if result : return result
    return False, code, name
    sys.exit(0)
############################# 日线/周线/月线清洗数据 ####################
def washTodayData(df, period, testFlag) :
    procTurtleData(df, period)
    procMaData(df)          # 处理close均线
    if df.close.iloc[-1] < df.ma_5.iloc[-1] : return 0
    delMaData(df)
    
    gap                     = int(period//2)
    procVector(df, gap)     # 找出close的顶/底
    df['seal']              = [1 if a >= 9.5 else 0 for a in df.grow]
    isseal                  = 1 if df.seal.tail(gap).sum() >= 1 else 0
    istur                   = 1 if df.tur.tail(gap).sum()  >= 1 else 0
    isa                     = 1 if df.amount.iloc[-1] >= 2 and df.change.iloc[-1] >= 2 else 0
    lgyx                    = [1 if a >= 7 else 0 for a in df.grow]
    isb                     = np.sum(lgyx[-gap:])
    flag                    = 1 if istur or (isa + isseal + isb) else 0
    if testFlag : print("istur(%d) seal(%d) isa(%d)" %(istur, isseal, isa))
    return flag
def washLongData(df, period, testFlag) :
    procTurtleData(df, period)
    df['seal']              = [1 if a >= 9.5 else 0 for a in df.grow]
    istur                   = 1 if df.tur.iloc[-1] == 1 else 0
    isseal                  = 1 if df.seal.tail(int(period//2)).sum() >= 1 else 0
    isa                     = 1 if df.amount.iloc[-1] >= 2 and df.change.iloc[-1] >= 15 else 0
    isb                     = 1 if df.amount.iloc[-1] >= 3 and df.change.iloc[-1] >=  5 else 0
    isc                     = 1 if df.amount.iloc[-1] >= 8 and df.change.iloc[-1] >=  2 else 0
    flag                    = 1 if istur and ((isa + isb + isc) or isseal) else 0
    if testFlag : print("tur(%d) seal(%d) isa(%d) isb(%d) isc(%d)" %(istur, isseal, isa, isb, isc))
    return flag
############################# 合并月/周线数据 ######################## 
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
    del df2['open']
    df2.reset_index(inplace=True)
    return df2
############################# 日线连板/反包 ###################################
def findSealing(code, name, testFlag, days, inn, qos):
    funcEntry               = False
    if len(days) < 10 : return False, code, name
    scnt0                   = days['seal'].tail(1).sum()
    scnt1                   = days['seal'].tail(3).sum()
    scnt2                   = days['seal'].tail(10).sum()
    scnt                    = max(scnt1, scnt2)
    grow                    = days.grow.tolist()
    flag0                   = 1 if (scnt >= 3 and scnt0) else 0
    flag1                   = 1 if grow[-2] <= -7 and grow[-1] >= 7 else 0
    if (flag0 or flag1) :
        close               = days['close'].tolist()
        sclose              = strClose(close[-1])
        amount              = days['amount'].iloc[-1]
        change              = days['change'].iloc[-1]
        minval              = days.close.tail(144).min()
        grow1               = round(close[-1] / minval, 2)
        info                = str(qos) + ".9 " + str(scnt) + "连板 " if flag0 else str(qos) + ".8 反包 "
        info                += str(change).rjust(5) + "%"
        bot                 = days.where(days.v == -1).dropna().index
        point               = 0 if len(bot) == 0 else bot[-1]
        gaps                = len(days) - point
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", minval, grow1, gaps, info]
        return result
    return []
############################# 日线放量大阳，调整后继续上涨 ##########################
def findDaysContinue(code, name, testFlag, df, period, delta, qos):
    funcEntry               = False
    procMacdData(df)            # 获取dif，dea
    
    vlist                   = df.v.where(df.v != 0).dropna().tolist()
    vidx                    = df.v.where(df.v != 0).dropna().index
    if not vlist : return []
    point                   = vidx[-1] if len(vidx) else 0
    gaps                    = len(df) - point - 1
    dif                     = df['dif'].tolist()
    dea                     = df['dea'].tolist()
    close                   = df['close'].tolist()
    change                  = df.change.iloc[-1]
    amount                  = df.amount.iloc[-1]
    tur                     = df.tur.iloc[-1]
    vec                     = close[point]
    cnt                     = 0
    info                    = ""
    # 有顶点，调整结束
    if vlist[-1] == 1 :
        if not (dif[point] > dea[point] and dea[point] >= 0) : return []
        if not close[-1] >= close[point] : return []
        if     close[-2] >= close[point] : return []
        cnt                 = len(df['dif'].iloc[point+1:].where(df.dif < df.dea).dropna())
        if cnt == 0 : 
            info            = str(qos) + ".9 HP " + str(change).rjust(5) + "%"
        else :
            info            = str(qos) + ".7 TZ " + str(change).rjust(5) + "%"
        if testFlag:
            print(code, name, len(df), "top:", point, gaps, cnt, info)
    # 有低点，调整破前高
    if vlist[-1] == -1 :
        tidx                = df.where(df.v == 1).dropna().index
        tpoint              = tidx[-1] if len(tidx) else 0
        top                 = close[tpoint]
        if not (dif[tpoint] > dea[tpoint] and dea[tpoint] >= 0) : return []
        if not close[-1] >= top : return []
        if     close[-2] >= top : return []
        cnt                 = len(df['dif'].iloc[tpoint+1:point+1].where(df.dif < df.dea).dropna())
        #print(code, name, tpoint, point, gaps1, cnt, len(df['dif'].iloc[tpoint+1:point+1]))
        if cnt == 0:
            info            = str(qos) + ".8 HP " + str(change).rjust(5) + "%"
        else:
            info            = str(qos) + ".6 Tur " + str(change).rjust(5) + "%"
        if testFlag:
            print(code, name, len(df), "bot:", tpoint, top, gaps, info)
        
    if info == "" : return []              
    minval                  = df['close'].tail(90).min()
    grow                    = round(close[-1] / minval, 2)
    if True\
    and grow < 1.5 \
    and ((change >= 5 and amount >= 3) or (change >= 2 and amount >= 8)) \
    and True :
        sclose              = strClose(close[-1])
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", vec, grow, gaps, info]
        return result
    return []
############################# 寻找最近的最大换手率， 且日线大于最大换手率的成本 ##########################
def getLongMaxVolPoint(code, name, testFlag, df, period, delta, grgap, qos):
    funcEntry               = False
    # 找出最近period的最大成交量，并剔除峰值不在delta内，或者峰值出现不到3根(调整未结束)
    idx                     = df['volume'].tail(period).idxmax()
    gaps                    = len(df) - idx
    if not (gaps < delta and gaps > 3) : return []
    
    # TO DO ：在低点处放量，不是在中间或高点放量
    mcls                    = df['close'].iloc[idx-2:idx+3].max()
    mins                    = df['close'].iloc[idx-2:idx+3].min()
    sumch                   = df['change'].iloc[idx-2:idx+3].sum()
    cost                    = 0
    for i in range(idx-2, idx+3):
        avrg                = (df['close'].iloc[i] + df['high'].iloc[i] + df['low'].iloc[i]) / 3
        cost                += avrg * (df['change'].iloc[i] / sumch)
        if testFlag: print(i, df['close'].iloc[i], df['change'].iloc[i], sumch, avrg, cost)
    
    cost                    = round(cost, 2)
    mlst                    = df['close'].iloc[-1]
    grow                    = round(mlst / cost, 2)
    grow1                   = round(mlst / mins, 2)
    if testFlag:
        print(code, name, idx, gaps, delta, mcls, mlst, cost, grow, grgap)
    if  grow >= 1.05 and grow < grgap and grow1 < 1.8 \
    and df['close'].iloc[-2]  < cost \
    and True :

        sclose              = strClose(mcls)
        change              = df.change.iloc[-1]
        amount              = df.amount.iloc[-1]
        info                = str(qos) + str(change).rjust(5) + "%" + "_" + str(cost)
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", mlst, grow, gaps, info]
        return result
    return []


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
############################# 基础处理函数 ############################
######################################################################
def strClose(close):
    return str(close).rjust(6)
############################# 从文件读数据 ###########################
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
        df['ma_'+str(ma)]   = df.close.rolling(ma).mean().round(decimals=2)
def delMaData(df) :
    for ma in maList:
        del df['ma_'+str(ma)]
def procBasicInfo(df, tur, gap) :
    baseFuncEntry           = False
    procTurtleData(df, tur) # 处理海龟
    procMaData(df)          # 处理close均线
    #procPrice (df, gap)     # 处理放量大涨
    procSeal  (df, gap)     # 处理首次涨停
    procMoney (df, gap)     # 找出5天成交额的顶/底
    procVector(df, gap)     # 找出close的顶/底
def procMacdData(df) :                         
    df['sema']              = pd.DataFrame.ewm(df.close,span=macdFast).mean().round(decimals=6) 
    df['lema']              = pd.DataFrame.ewm(df.close,span=macdSlow).mean().round(decimals=6)   
    df['dif']               = (df.sema - df.lema).round(decimals=2)       
    df['dea']               = pd.DataFrame.ewm(df.dif,span=macdMidd).mean().round(decimals=2)
    df['macd']              = 2*(df.dif.round(decimals=2) - df.dea.round(decimals=2))
    del df['sema'], df['lema'], df['macd']
    #df['dlt']               = df['dif'] - df['dea']
    #step                    = int(macdSlow/2)
    #index                   = 0 if len(df) < step else len(df) - step + 1
    #peak                    = df.dlt.rolling(macdSlow, center=True).max()
    #poke                    = df.dlt.rolling(macdSlow, center=True).min()
    #for i in range(index, len(df) - 1):
    #    peak[i]             = df.dlt.iloc[i-step:].max()
    #    poke[i]             = df.dlt.iloc[i-step:].min()
    #df['p']                 = [1 if (a == b) else -1 if (a == c) else 0 for a, b, c in zip(df.dlt, peak, poke)]
    #df['p'].loc[-1]         = 0
    #df['b']                 = [0 if (a != -1) else 1 if (a == -1 and b > 0 and c > 0) else -1 for a,b,c in zip(df.p, df.dif, df.dea)]
    #del df['dlt'], df['dif'], df['dea']
def procSeal  (df, gap) :
    df['seal']              = [1 if a >= 9.7 else 0 for a in df.grow]
    df['fstS']              = df['seal'].rolling(gap).sum()
    df['fstS']              = [1 if a == 1 and b == 1 else 0 for a, b in zip(df.seal, df.fstS)]
def procPrice (df, gap) :
    step                    = int(gap/2)
    index                   = 0 if len(df) < step else len(df) - step + 1
    df['price']             = (df.grow * df.rate).round(decimals=1)
    ppeak                   = df.price == df.price.rolling(gap, center=True).max()
    for i in range(index, len(df) - 1):
        ppeak[i]            = True if df.price[i] == df.price.iloc[i-step:].max() else False
    df['p']                 = ppeak
    df['p'].iloc[-1]         = None
def procMoney (df, gap) :
    step                    = int(gap/2)
    index                   = 0 if len(df) < step else len(df) - step + 1
    for i in amList:
        df['am'+str(i)]     = df.amount.rolling(i).mean().round(decimals=2)
    mpeak                   = df.am5.rolling(gap, center=True).max()
    mpoke                   = df.am5.rolling(gap, center=True).min()
    for i in range(index, len(df) - 1):
        mpeak[i]            = df.am5.iloc[i-step:].max()
        mpoke[i]            = df.am5.iloc[i-step:].min()
    df['m']                 = [1 if (a == b) else -1 if (a == c) else 0 for a, b, c in zip(df.am5, mpeak, mpoke)]
    df['m1']                = df['m'].shift(-1)
    df['m']                 = [a - 1 if (a == b and a ==  1) else a for a, b in zip(df.m, df.m1)]
    df['m']                 = [a + 1 if (a == b and a == -1) else a for a, b in zip(df.m, df.m1)]
    df['m'].loc[-1]         = 0
    del df['m1']
def procVector(df, gap) :
    #step                    = int(gap/2)
    #index                   = 0 if len(df) < step else len(df) - step + 1
    cpeak                   = df.close.rolling(gap, center=True).max()
    cpoke                   = df.close.rolling(gap, center=True).min()
    #for i in range(index, len(df) - 1):
    #    cpeak[i]            = df.close.iloc[i-step:].max()
    #    cpoke[i]            = df.close.iloc[i-step:].min()
    df['v']                 = [1 if (a == b) else -1 if (a == c) else 0 for a, b, c in zip(df.close, cpeak, cpoke)]
    #df['v1']                = df['v'].shift(-1)
    #df['v']                 = [a - 1 if (a == b and a ==  1) else a for a, b in zip(df.v, df.v1)]
    #df['v']                 = [a + 1 if (a == b and a == -1) else a for a, b in zip(df.v, df.v1)]
    #df['v'].loc[-1]         = 0
    #del df['v1']
    #sys.exit(0)
#####################################################################
############################# 基础函数结束 ###########################
#####################################################################
    
    
#####################################################################
############################# 废弃函数 ###############################
#####################################################################
############################# 月线海龟 or 月线最低点换手 ###########################
#月线上出现低点附近放巨量，且周线海龟，看日线的买点
def findMonthTurtle(code, name, testFlag, data, week, days, period, num):
    funcEntry               = False
    # 找出历史高点
    lens                    = len(data)
    close                   = data.close.tolist()
    cmaxidx                 = data.close.idxmax()
    gaps                    = lens - cmaxidx
    if gaps < period:
        return False, code, name
    # 最近n个月出现高点以来的历史成交量，换手率60%以上        
    vmaxidx                 = data['volume'].iloc[cmaxidx+2:].idxmax()
    gaps                    = lens - vmaxidx
    if gaps >= 10 or gaps <= 3 or close[-1] < close[vmaxidx] \
    or data['change'].iloc[vmaxidx] < 50 :
        return False, code, name
    # 计算周线close海龟    
    procTurtleData(week, period)
    procTurtleData(days, period)
    if week.tur.iloc[-1] == 0 or days.tur.iloc[-1] == 0:
        return False, code, name
    # 日线加速上涨 
    sclose                  = strClose(close[-1])    
    change                  = data.change.iloc[-1]
    amount                  = data.amount.iloc[-1]
    if not ((amount >= 3 and change >= 5) or days.grow.iloc[-1] > 5):
        return False, code, name
    cmin                    = close[vmaxidx]
    grow                    = round(close[-1] / cmin, 2)
    info                    = str(num) + ".9 " + "M.buy " + str(change).rjust(5) + "%"
    result                  = [True, code, name.rjust(4), sclose, str(amount)+"亿", cmin, grow, gaps, info]
    return result
############################# 周线海龟 or 周线最低点换手 ##########################
#周线从最高点下来的最低点附近，出现放量，经过调整后，周线突破上次放量的股价，看日线的买点
def findWeekTurtle(code, name, testFlag, week, days, period, num):
    funcEntry               = False
    # 找出历史高点
    lens                    = len(week)
    close                   = week.close.tolist()
    cmaxidx                 = week.close.idxmax()
    gaps                    = lens - cmaxidx
    if gaps < period:
        return False, code, name
    # 最近n周出现高点以来的历史成交量,且洗盘至少4个月后破放量股价
    vmaxidx                 = week['volume'].iloc[cmaxidx+5:].idxmax()
    gaps                    = lens - vmaxidx
    if gaps < 12 or close[-1] < close[vmaxidx] :
        return False, code, name
    # 计算周线close海龟    
    procTurtleData(week, period)
    procTurtleData(days, period)
    if week.tur.iloc[-1] == 0 or days.tur.iloc[-1] == 0:
        return False, code, name
    # 日线加速上涨 
    sclose                  = strClose(close[-1])    
    change                  = days.change.iloc[-1]
    amount                  = days.amount.iloc[-1]
    if not ((amount >= 3 and change >= 5) or days.grow.iloc[-1] > 5):
        return False, code, name
    cmin                    = close[vmaxidx]
    grow                    = round(close[-1] / cmin, 2)
    info                    = str(num) + ".9 " + "W.buy " + str(change).rjust(5) + "%"
    result                  = [True, code, name.rjust(4), sclose, str(amount)+"亿", cmin, grow, gaps, info]
    return result
############################# 日线海龟 ##########################
def getLastFireUp(df, gap, grow) :
    df['f']                 = [1 if (a >= grow) else 0 for a in df.grow]
    df['fsum']              = df['f'].rolling(gap).sum()
    df['f']                 = [1 if (a == 1 and b == 1) else 0 for a, b in zip(df.f, df.fsum)]
    del df['fsum']
    firelist                = df.where(df.f == 1).dropna().index
    if len(firelist):
        fp                  = firelist[-1]
        fv                  = df.close.loc[fp]
        if len(df) - fp == 1 or len(df) - fp >= gap:
            return []
        vectlist            = df.where(df.v == 1).dropna().index
        vp                  = vectlist[-1] if len(vectlist) else 0
        maxCls              = df.close[vp] if vp > fp else fv
        minIdx              = df.close[vp+1:].idxmin() if vp > fp else df.close[fp+1:].idxmin()
        minCls              = df.close[vp+1:].min()    if vp > fp else df.close[fp+1:].idxmin()
        #print(fp, fv, maxCls, minIdx, minCls)
        return [fp, fv, maxCls, minCls, minIdx]
    return []
def findDaysTurtle(code, name, testFlag, days, inn, period, num):
    funcEntry               = False
    lens                    = len(days)
    close                   = days.close.tolist()
    #print(days.tail(20))
    result                  = getLastFireUp(days, 20, 7)
    if len(result) == 0:
        return False, code, name
    if testFlag :
        print(code, name, result)
    
    # 日线加速上涨
    change                  = days.change.iloc[-1]
    amount                  = days.amount.iloc[-1]
    fp                      = result[0]    
    fv                      = result[1]
    top                     = result[2]
    bot                     = result[3]
    gap0                    = lens - fp
    gap1                    = lens - result[4]
    dlt                     = round(close[-1] / max(fv, top), 2)
    findFlag                = False
    if dlt < 0.98 or gap0 <= 3:
        return False, code, name
    
    if dlt >= 1 :
        findFlag            = True
        info                = str(num) + ".7 " + "ADJ.OK " + str(change).rjust(5) + "%"
    if close[-1] >= fv and close[-1] < top :
        findFlag            = True
        info                = str(num) + ".6 " + "ADJ.Wl " + str(change).rjust(5) + "%"
    
    cmin                    = days['close'].tail(120).min()
    grow0                   = round(close[-1] / close[fp-1], 2)
    grow1                   = round(close[-1] / cmin, 2)
    if amount < 8 or change < 2 or grow1 >= 2:
        return False, code, name
    
    if findFlag :
        result              = [True, code, name.rjust(4), fv, str(amount)+"亿", close[-1], grow0, gap1, info]
        #print(result)
        return result
    return False, code, name
############################# 上涨中继 ###################################
def findIncrease(code, name, testFlag, days, inn, num):
    funcEntry               = False
    close                   = days['close'].tolist()
    sclose                  = strClose(close[-1])
    amount                  = days['amount'].iloc[-1]
    change                  = days['change'].iloc[-1]
    # 没有1个低点，发生在新股中
    bot                     = days.where(days.v == -1).dropna().index
    if len(bot) == 0 :
        if close[-1] == max(close) and change >= 10 and len(days) >= 20:
            info            = str(num) + ".7 新股新高 " + str(change).rjust(5) + "%"
            minidx          = days['close'].idxmin()
            grow0           = round(close[-1] / close[minidx], 2)
            gaps            = len(days) - minidx
            result          = [True, code, name.rjust(4), sclose, str(amount)+"亿", close[minidx], grow0, gaps, info]
            return result
        return False, code, name
    # 涨停后调整2~3天，后连续上涨(30分钟中枢),低点大于第一个30分钟的高点
    limit                   = days.where(days.grow >= 9.7).dropna().index
    if len(limit) == 0:
        return False, code, name
    lpoint                  = limit[-1]
    gap                     = len(days) - limit[-1]
    if (gap >= 4 and gap <= 7) \
    and days.low.iloc[-1] >= days['high'].iloc[lpoint+1] \
    and close[-1] > close[lpoint] :
        gaps                = len(days) - bot[-1]
        cbot                = close[bot[-1]]
        grow                = round(close[-1] / cbot, 2)
        info                = str(num) + ".6 half " + str(change).rjust(5) + "%"
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", cbot, grow, gaps, info]
        return result
    # 大涨调整后破新高
    limit                   = days.where(days.grow >= 6).dropna().index
    if len(limit) == 0:
        return False, code, name
    lpoint                  = limit[-1]
    gap                     = len(days) - limit[-1]
    if gap == 1 :
        return False, code, name
    high                    = days['high'].tolist()
    maxh                    = max(high[lpoint-1], high[lpoint], high[lpoint+1])
    if gap <= 13 \
    and close[-1] >= maxh \
    and days.tur.iloc[-1] == 1\
    and amount >= 3\
    and change >= 3\
    and True:
        gaps                = len(days) - bot[-1]
        cbot                = close[bot[-1]]
        grow                = round(close[-1] / cbot, 2)
        info                = str(num) + ".6 中继 " + str(change).rjust(5) + "%"
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", cbot, grow, gaps, info]
        return result
    return False, code, name
############################# 点火放量大涨 ###################################
def findFireup(code, name, testFlag, days, inn, num):
    funcEntry               = False
    # 找出最近一次首板涨停
    slist                   = days.where(days.fstS == 1).dropna().index
    if len(slist) == 0:
        return False, code, name
    point                   = slist[-1]
    gaps                    = len(days) - point
    
    top                     = days.iloc[point-1:].where(days.v == 1).dropna().index
    if len(top) == 0 :
        #if gaps > 5 :
        #    print(code, name, point, len(days), gaps, "涨停未开始调整")
        return False, code, name
    # 首板涨停后调整出现低点，则前面已经产生了1个高点
    bot                     = days.iloc[point-1:].where(days.v == -1).dropna().index
    if len(bot) == 0 or len(top) == 0:
        return False, code, name
    # 低点小于前高，防止找到几浪后的低点；股价突破前高    
    tp                      = top[0]     
    bp                      = bot[-1]
    close                   = days['close'].tolist()
    amount                  = days['amount'].iloc[-1]
    change                  = days['change'].iloc[-1]
    grow                    = round(close[-1] / days.close.tail(144).min(), 2)
    gaps                    = len(days) - bp
    #if close[bp] < close[tp] and close[-1] >= close[point] \
    if close[bp] < close[tp] and close[-1] >= close[tp] \
    and close[-1] == max(close[bp:]) and grow < 1.5 \
    and change >= 5 and amount >= 3:
        sclose              = strClose(close[-1])
        info                = str(num) + ".5 调整结束" + str(change).rjust(5) + "%"
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", close[point], grow, gaps, info]
        return result
    return False, code, name
############################# 寻找最近的最大换手率， 且日线大于最大换手率的成本 ##########################
def findDaysJump(code, name, testFlag, df, delta, qos):
    funcEntry               = False
    vlist                   = df.where(df.jump == 1).dropna().index
    point                   = vlist[-1] if len(vlist) else 0
    gaps                    = len(df) - point
    if not (gaps < delta and gaps > 3) : return []
    
    sumch                   = df['change'].iloc[point:].sum()
    cost                    = 0
    for i in range(point, len(df)):
        avrg                = (df['close'].iloc[i] + df['high'].iloc[i] + df['low'].iloc[i]) / 3
        cost                += avrg * (df['change'].iloc[i] / sumch)
        if testFlag: print(i, df['close'].iloc[i], df['change'].iloc[i], sumch, avrg, cost)
    cost                    = round(cost, 2)
    mcls                    = df['close'].iloc[point]
    mlst                    = df['close'].iloc[-1]
    minval                  = df['close'].iloc[point:].min()
    minval1                 = df['close'].tail(60).min()
    grow                    = round(mlst / cost, 2)
    grow1                   = round(mlst / minval1, 2)
    change                  = df.change.iloc[-1]
    amount                  = df.amount.iloc[-1]
    if testFlag:
        print(code, name, point, gaps, delta, mlst, cost, grow)
    if grow >= 1.03 and grow <= 1.2 \
    and ((change >= 5 and amount >= 5) or (change >= 2 and amount >= 8)) \
    and minval > df['close'].iloc[point-1] \
    and gaps <= 9 \
    and grow1 < 1.5 \
    and True :
        sclose              = strClose(mcls)
        info                = str(qos) + str(change).rjust(5) + "%" + "_" + str(cost)
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", mlst, grow, gaps, info]
        return result
    return []
