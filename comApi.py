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
from interval import Interval

warnings.filterwarnings( "ignore" )

maList                  = [5, 10, 20]
hfList                  = [5, 10, 20, 30]

isMacdMode              = 0     #0:short; 1:long
macdFast                = 24 if isMacdMode == 1 else 12
macdSlow                = 52 if isMacdMode == 1 else 26
macdMidd                = 18 if isMacdMode == 1 else 9

daysQos                 = 9
weekQos                 = daysQos - 1
mothQos                 = weekQos - 1

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
    # 日线>20线，half>30线
    flag, start         = washInvalidData(code, name, days, half, testFlag)
    if flag == False    : return flag, code, name
    
    value               = info['流通市值'].iloc[-1].replace('亿', '').replace(' ', '')
    if float(value) < 40 : return False, code, name
    #print(info)
    #print(code, name, info['流通市值'].iloc[-1], value)
    #sys.exit(0)
    
    if findEnab and 1:                   # 周线突破
        result      = findWeekGrow(code[2:], name, testFlag, days, half, "9W.")
        if result : return result
    
    if findEnab and 0:                   # 涨停   
        result      = findSealing1(code[2:], name, testFlag, days, half, start, 8, "8T.")
        if result : return result

    if findEnab and 1:                   # 日线中继
        result      = findDaysGrow(code[2:], name, testFlag, days, half, start, 4, "7D.")
        if result : return result

    if findEnab and 0:                   # half中继
        result      = findBigsVolm(code[2:], name, testFlag, days, half, start, 8, "5B.")
        if result : return result

    return False, code, name
    sys.exit(0)

############################# 日线/half清洗数据 ####################
def washInvalidData(code, name, days, half, testFlag) :
    # 上穿20日线后不破20日线
    if len(days) < maList[-1] : return False, 0
    for ma in maList:
        days['cls'+str(ma)] = days.close.rolling(ma).mean().round(decimals=2)
    if days.close.iloc[-1]  < days['cls10'].iloc[-1] : return False, 0

    
    days['shtcls']          = days.close.shift(1)
    days['sht20']           = days['cls20'].shift(1)
    days['fireup']          = [ 1 if a <= b and c > d else 0 for a, b, c, d in zip(days.shtcls, days.sht20, days.close, days.cls20)]
    del days['shtcls'], days['sht20']
    
    days['gvol']            = [ 1 if a >= 5   else 0 for a in days.amount]
    days['seal']            = [ 1 if a >= 9.5 else 0 for a in days.grow]
    vidx                    = days.fireup.where(days.fireup == 1).dropna().index
    if len(vidx) == 0 : return False, 0
    start                   = vidx[-1]
    gvol                    = days.gvol.iloc[start:].sum()
    seal                    = days.seal.iloc[start:].sum()
    if gvol + seal == 0 : return False, 0
    del days['gvol']
    
    for ma in hfList:
        half['cls'+str(ma)] = half.close.rolling(ma).mean().round(decimals=2)
    
    procTurtleData(days, maList[-1])
    procTurtleData(half, hfList[-1])
    #print(code, name, start, days.index[-1], gvol, seal)
    return True, start
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
############################# 放巨量涨停 ###########################
def findSealing1(code, name, testFlag, df, hf, start, money, qos):
    info                = ""
    df['fireS']         = [ 1 if (a>=9.5 and b>=money) or (a>=5 and b>=10) else 0 for a, b in zip(df.grow, df.amount)]
    if df.fireS.tail(maList[-1]).sum() == 0 : return ""
    
    vidx                = df.fireS.where(df.fireS == 1).dropna().index
    close               = df['close'].tolist()
    change              = df['change'].iloc[-1]
    amount              = df['amount'].iloc[-1]
    minval              = df.close.tail(90).min()
    grow                = round(close[-1] / minval, 2)
    procMacdData(hf)
    
    if len(vidx) == 0 : 
        print(code, name, start, df.index[-1], vidx)
        return ""
    if grow >= 2 : return ""
    #print(code, name, start, df.index[-1], vidx)
    
    point               = vidx[-1]
    gaps                = df.index[-1] - point
    
    if gaps <= 2:
        if hf.dif.iloc[-1] < hf.dea.iloc[-1] : return ""
        info            = str(qos) + "9 " + str(change).rjust(5) + "%"
    else :
        if point >= start and close[-1] >= close[point] and df.tur.iloc[-1] :
            info        = str(qos) + "7 " + str(change).rjust(5) + "%"
    
    if True \
    and info != "" \
    and (grow < 1.5 or (grow < 2 and df.seal.iloc[-1])) \
    and (change > 2 or (df.seal.iloc[-1])) \
    and (amount > 5 or (df.seal.iloc[-1])) \
    and change < 50 \
    and True :
        result          = [True, code, name.rjust(4), close[point], str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""
############################# 获取中枢 ############################
def findZS(df) :
    cpeak               = df.high.rolling(15, center=True).max()
    cpoke               = df.low.rolling (15, center=True).min()
    df['top']           = [ 1 if a == b else 0 for a, b in zip(df.high, cpeak)]
    df['bot']           = [-1 if a == b else 0 for a, b in zip(df.low,  cpoke)]
    
    vidx                = df.bot.where(df.bot == -1).dropna().index
    point               = vidx[-1] if len(vidx) else 0
    inter               = [Interval(a, b) for a, b in zip(df.low, df.high)]
    high                = df.high.tolist()
    low                 = df.low.tolist()
    result              = []
    findNewZs           = True
    
    for i in range(point+2, len(df)):
        if findNewZs:
            temp        = inter[i].overlaps(inter[i-1]) and inter[i].overlaps(inter[i-2])
            if temp:
                zsdd    = min(low[i],  low[i-1],  low[i-2])
                zsgg    = max(high[i], high[i-1], high[i-2])
                azs     = Interval(zsdd, zsgg)
                zsdw    = max(low[i],  low[i-1],  low[i-2])
                zsup    = min(high[i], high[i-1], high[i-2])
                zs      = Interval(zsdw, zsup)
                end     = df.low.where(df.low < zsgg).dropna().index
                rg      = i if len(end) == 0 else end[-1]
                result.append([i-2, rg, zsdw, zsup, zsdd, zsgg, zs, azs])
                findNewZs   = False
                #print("zs idx[%d, %d], range[%.2f, %.2f]" %(i-2, rg, zsdd, zsgg))
        else :
            findNewZs   = True if i - result[-1][1] == 2 else False
    return result
############################# 获取fireup ############################    
def findFireUp(code, name, testFlag, df, ktype):
    global maList, hfList
    # 上穿n日线后不破n日线
    wins                    = hfList if ktype == 'half' else maList
    last                    = wins[-1]
    df['shtcls']            = df.close.shift(1)
    df['shtlast']           = df['cls'+str(last)].shift(1)
    df['fireup']            = [ 1 if a <= b and c > d else 0 for a, b, c, d in zip(df.shtcls, df.shtlast, df.close, df['cls'+str(last)])]
    del df['shtcls'], df['shtlast']
    vidx                    = df.fireup.where(df.fireup == 1).dropna().index
    if len(vidx) == 0 : return 0
    return vidx[-1]
def findDaysCross(code, name, testFlag, df, delta):
    close                   = df.close.tolist()
    uplist                  = df.cross.where(df.cross ==  1).dropna().index
    dwlist                  = df.cross.where(df.cross == -1).dropna().index
    upidx                   = uplist[-1] if len(uplist) else df.close.tail(delta).idxmin()
    dwidx                   = dwlist[-1] if len(dwlist) else df.close.tail(delta).idxmax()
    # grow then adjust : dif > dea
    if dwidx < upidx :
        if df.dif.iloc[-1] < 0 : return []
        if df.dea.iloc[-1] < 0 : return []
        difmax              = df.dif.iloc[upidx:].idxmax()
        gaps                = df.index[-1] - difmax
        if difmax == df.dif.index[-1]  : return [True, upidx, dwidx, gaps]
        macdmax             = df.macd.iloc[upidx:].idxmax()
        macdmin             = df.macd.iloc[macdmax:].idxmin()
        gaps                = df.index[-1] - macdmin
        if macdmin != df.macd.iloc[-1] : 
            return [True, upidx, dwidx, gaps]
        elif close[-1] > df.cls10.iloc[-1]: 
            return [True, upidx, dwidx, gaps]
    # grow then adjust : dif < dea     
    elif upidx < dwidx :
        difmin              = df.dif.iloc[dwidx:].min()
        gaps                = df.index[-1] - df.dif.iloc[dwidx:].idxmin()
        if df.dif.iloc[-1] == difmin : return []
        if df.dif.iloc[-1] < df.dif.iloc[-2] : return []
        if close[-1] < df.close.iloc[dwidx]  : return []
        #if close[-1] < max(df.high.iloc[dwidx-2:dwidx]) : return []
        return [True, upidx, dwidx, gaps]
    return []
############################# 日线脱离中枢 #########################
def findWeekGrow(code, name, testFlag, df, hf, qos):
    info                    = ""
    week                    = getMergData("week", df)
    procMacdData(week)
    close                   = week.close.tolist()
    amount                  = df.amount.iloc[-1]
    change                  = df.change.iloc[-1]
    uplist                  = week.cross.where(week.cross ==  1).dropna().index
    dwlist                  = week.cross.where(week.cross == -1).dropna().index
    upidx                   = uplist[-1] if len(uplist) else week.close.tail(90).idxmin()
    dwidx                   = dwlist[-1] if len(dwlist) else week.close.tail(90).idxmax()
    botcls                  = close[upidx]
    grow                    = round(close[-1] / botcls, 2)
    # grow then adjust : dif > dea
    if dwidx < upidx :
        if week.dif.iloc[-1] < 0 : return ""
        if week.dea.iloc[-1] < 0 : return ""
        difmax              = week.dif.iloc[upidx:].idxmax()
        if week.dif.index[-1] == difmax : return ""
        macdmin             = week.macd.iloc[difmax:].min()
        macdidx             = week.macd.iloc[difmax:].idxmin()
        gaps                = week.index[-1] - macdidx
        if week.macd.iloc[-1] == macdmin : return ""
        if week.macd.iloc[-1] < week.macd.iloc[-2] : return ""
        info                = str(qos) + "9 " + str(change).rjust(5) + "%"
        result              = [True, code, name.rjust(4), botcls, str(amount)+"亿", close[-1], grow, gaps, info]
    # grow then adjust : dif < dea     
    elif upidx < dwidx :
        difmin              = week.dif.iloc[dwidx:].min()
        minidx              = week.dif.iloc[dwidx:].idxmin()
        gaps                = week.index[-1] - minidx
        if week.dif.iloc[-1] == difmin : return ""
        if week.dif.iloc[-1] < week.dif.iloc[-2] : return ""
        if close[-1] != max(close[dwidx:]) : return ""
        if grow >= 2 : return ""
        info                = str(qos) + "8 " + str(change).rjust(5) + "%"
        result              = [True, code, name.rjust(4), botcls, str(amount)+"亿", close[-1], grow, gaps, info]
    if info == "" : return ""
    if True \
    and ((amount >= 5 and change >= 4) or (amount >= 8 and change >= 2) or (df.seal.iloc[-1] and change >= 5)) \
    and True :
        return result
    return ""
def findDaysGrow(code, name, testFlag, df, hf, start, money, qos):
    info                    = ""
    procMacdData(df)
    dfRust                  = findDaysCross(code, name, testFlag, df, 120)
    if len(dfRust) == 0 : return ""
    if df.macd.iloc[-1] < 0 : return ""
    
    close                   = df.close.tolist()
    upidx                   = dfRust[1]
    gaps                    = dfRust[-1]
    botcls                  = close[upidx]
    grow                    = round(close[-1] / botcls, 2)
    amount                  = df.amount.iloc[-1]
    change                  = df.change.iloc[-1]

    procMacdData(hf)
    hfRust                  = findDaysCross(code, name, testFlag, hf, 120)
    if len(hfRust) == 0 : return ""
    
    if grow > 1.5 : return ""
    if not ((amount >= 5 and change >= 5) or (amount >= 8 and change >= 2) or (df.seal.iloc[-1] and change >= 5)) : return ""

    info                    = str(qos) + "9 " + str(change).rjust(5) + "%"
    result                  = [True, code, name.rjust(4), botcls, str(amount)+"亿", close[-1], grow, gaps, info]
    return result
def findHalfGrow(code, name, testFlag, df, hf, start, money, qos):
    info                    = ""
    procMacdData(df)
    dfRust                  = findDaysCross(code, name, testFlag, df, 120)
    if len(dfRust) == 0 : return ""
    if df.macd.iloc[-1] < 0 : return ""
    
    close                   = df.close.tolist()
    upidx                   = dfRust[1]
    gaps                    = dfRust[-1]
    botcls                  = close[upidx]
    grow                    = round(close[-1] / botcls, 2)
    amount                  = df.amount.iloc[-1]
    change                  = df.change.iloc[-1]

    procMacdData(hf)
    hfRust                  = findDaysCross(code, name, testFlag, hf, 120)
    if len(hfRust) == 0 : return ""
    
    if grow > 1.5 : return ""
    if not ((amount >= 5 and change >= 5) or (amount >= 8 and change >= 2) or (df.seal.iloc[-1] and change >= 5)) : return ""

    info                    = str(qos) + "9 " + str(change).rjust(5) + "%"
    result                  = [True, code, name.rjust(4), botcls, str(amount)+"亿", close[-1], grow, gaps, info]
    return result
def findBigsVolm(code, name, testFlag, df, hf, start, money, qos):
    info                    = ""
    delta                   = 20
    close                   = df.close.tolist()
    amount                  = df.amount.iloc[-1]
    change                  = df.change.iloc[-1]
    ammax                   = df.amount.tail(delta).max()
    amidx                   = df.amount.tail(delta).idxmax()
    gaps                    = df.index[-1] - amidx
    grow                    = round(close[-1] / min(close[-90:]), 2)
    
    if not (ammax > 10 and amount > ammax/2) : return ""
    if close[-1] < df.cls20.iloc[-1]  : return ""
    if df.seal.tail(delta).sum() == 0 : return ""
    if not (change >= 5 or (amount >= 10 and change >=2)) : return ""
    if close[-1] < close[amidx] : return ""
    if grow > 1.6 : return ""
    
    procMacdData(df)
    if df.dif.iloc[-1] < 0 : return ""
    if df.dif.iloc[-1] < df.dif.iloc[-2] : return ""
    
    procMacdData(hf)
    if hf.dif.iloc[-1] < 0 : return ""
    if hf.dif.iloc[-1] < hf.dea.iloc[-1] : return ""
    
    info                    = str(qos) + "9 " + str(change).rjust(5) + "%"
    result                  = [True, code, name.rjust(4), close[start], str(amount)+"亿", close[-1], grow, gaps, info]
    return result

    if True \
    and (change >= 5 or (amount >= 10 and change >=2)) \
    and grow < 1.5 and close[-1] < 50 \
    and True :
        info                = str(qos) + "9 " + str(change).rjust(5) + "%"
        result              = [True, code, name.rjust(4), close[start], str(amount)+"亿", close[-1], grow, gaps, info]
        return result
    return ""
###################################################################



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

def findDaysGrow114(code, name, testFlag, df, hf, start, money, qos):
    funcEntry               = False
    info                    = ""
    procMacdData(df)
    close                   = df.close.tolist()
    amount                  = df.amount.iloc[-1]
    change                  = df.change.iloc[-1]
    gaps                    = len(df) - start
    difmax                  = df.dif.tail(90).idxmax()
    grow                    = round(df.close.iloc[-1] / df.close.tail(90).min(), 2)
    topcls                  = df.close.iloc[-90 : difmax].max()
    dw                      = df.cross.iloc[difmax:].where(df.cross == -1).dropna().index
    if len(dw) == 0 :
        if close[-1] >= topcls and grow < 2 :
            if df.dif.iloc[-1] == df.dif.iloc[difmax:].max() \
            and df.tur.iloc[-1] :
                info        = str(qos) + "9 " + str(change).rjust(5) + "%"
            elif df.dif.iloc[-1] > df.dif.iloc[difmax:].min() :
                info        = str(qos) + "8 " + str(change).rjust(5) + "%"
    else:
        fstdw               = dw[0]
        cross               = [1 if a > b else 0 for a, b in zip(df.dif, df.dea)]
        crosscnt            = sum(cross[fstdw:])
        if close[-1] >= topcls \
        and df.dif.iloc[fstdw:].idxmin() > df.macd.iloc[fstdw:].idxmin() \
        and ((df.dif.iloc[-1] > df.dea.iloc[-1] >= 0) or (crosscnt)) \
        and grow < 1.5 \
        and True :
            info            = str(qos) + "7 " + str(change).rjust(5) + "%"
    if info == "" : return ""
    
    if True \
    and ((amount >= 4 and change >= 4) or (amount >= 8 and change >= 2) or (df.seal.iloc[-1] and change >= 5)) \
    and close[-1] < 50 \
    and True :
        result              = [True, code, name.rjust(4), close[start], str(amount)+"亿", close[-1], grow, gaps, info]
        return result
    return ""

def findDaysGrow113(code, name, testFlag, df, hf, start, money, qos):
    global topAmount, topChange, topGrow
    funcEntry               = False
    info                    = ""
    df['con1']              = [ 1 if a >= 9.5 and b >= money     else 0 for a, b in zip(df.grow, df.amount)]
    df['con2']              = [ 1 if a >= 5   and b >= money * 2 else 0 for a, b in zip(df.grow, df.amount)]
    con1                    = df.con1.iloc[start:].sum()
    con2                    = df.con2.iloc[start:].sum()
    minma                   = min(hf.cls5.iloc[-1], hf.cls10.iloc[-1], hf.cls20.iloc[-1], hf.cls30.iloc[-1])
    close                   = df.close.tolist()
    amount                  = df.amount.iloc[-1]
    change                  = df.change.iloc[-1]
    if con1 + con2 == 0 : return ""
    if hf.close.iloc[-1] < minma : return ""
    
    procMacdData(hf)
    if hf.dea.iloc[-1] < 0 : return ""

    grow                    = round(close[-1] / df.close.tail(90).min(), 2)
    hidx                    = df.high.iloc[start:].idxmax()
    dgap                    = df.index[-1] - start
    hgap                    = df.index[-1] - hidx
    if hgap == 0 and df.tur.iloc[-1] :
        hdiff               = np.diff(np.sign(np.diff(df.high)))
        hpeak               = (np.where(hdiff == -2)[0] + 1).tolist()
        if len(hpeak) == 0 : return ""
        point               = hpeak[-1]
        if point <= start : 
            info            = str(qos) + "9 CG" + str(change).rjust(5) + "%"
        else:
            clscnt          = [0 for i in range(len(maList))]
            lowcnt          = [0 for i in range(len(maList))]
            for i in range(len(maList)):
                less        = [1 if a < b else 0 for a,b in zip(df.close, df['cls'+str(maList[i])])]
                clscnt[i]   = sum(less[point:])
                temp        = [1 if a < b else 0 for a,b in zip(df.low, df['cls'+str(maList[i])])]
                lowcnt[i]   = sum(temp[point:])
            
            if clscnt[0] == 0 and lowcnt[1] == 0 : 
                info        = str(qos) + "8 N5(0) " + str(change).rjust(5) + "%"
                #print(code, name, point, df.index[-1], clscnt[0], clscnt[1], clscnt[2], lowcnt[0], lowcnt[1], lowcnt[2], info)
            # cls < 5 && cls > 10 && low > 10
            elif clscnt[0] and clscnt[1] == 0 and lowcnt[1] == 0:
                info        = str(qos) + "8 L5(" + str(clscnt[0]) + ") " + str(change).rjust(5) + "%"
            # cls < 10 && cls > 20 && low > 20
            elif clscnt[0] and clscnt[1] and lowcnt[2] == 0 :
                info            = str(qos) + "8 L10(" + str(clscnt[1]) + ") " + str(change).rjust(5) + "%"
            else : return ""
    elif hgap <= 2 :
        maxma               = max(hf.cls20.iloc[-1], hf.cls30.iloc[-1])
        if hf.close.iloc[-1] > maxma and close[-1] != df.close.iloc[hidx:].min() :
            info            = str(qos) + "7 " + str(change).rjust(5) + "%"
    else:
        if df.tur.iloc[-1] :
            info            = str(qos) + "6 " + str(change).rjust(5) + "%"
    if info == "" : return []
    
    if True \
    and ((amount >= 4 and change >= 4) or (amount >= 8 and change >= 2) or df.seal.iloc[-1]) \
    and (grow < 1.5 or (grow < 2 and df.seal.iloc[-1])) \
    and True :
        result              = [True, code, name.rjust(4), close[start], str(amount)+"亿", close[-1], grow, dgap, info]
        return result
    return ""
def findDaysGrow111(code, name, testFlag, df, half, qos):
    info                    = ""
    # 日线强势
    if df.close.iloc[-1]   <= df.cls5.iloc[-1]   : return []
    if half.close.iloc[-1] <= half.cls30.iloc[-1]: return []
    
    # 日线中枢
    result                  = findZS(df)
    amount                  = df.amount.iloc[-1]
    change                  = df.change.iloc[-1]
    close                   = df['close'].tolist()
    high                    = df['high'].tolist()
    curr                    = df.index[-1]
    grow                    = round(close[-1] / df.close.tail(90).min(), 2)
    gaps                    = 0
    left                    = 0
    info                    = ""
    if testFlag :
        print(code, name, "中枢个数：", len(result))
        if len(result) : print("zs info:", close[-1], high[-1], result[-1][:6])

    if len(result) :
        # inner zs
        start               = result[0][0]
        left                = result[-1][0]
        right               = result[-1][1]
        zsup                = result[-1][3]
        zsgg                = result[-1][5]
        gaps                = curr - right
        if df.grow.iloc[start:].max() < 5 : return []
        if grow >= 1.35 or len(result) > 2 : return []
        
        procTurtleData(df, 29)
        if df.tur.iloc[right:].sum() == 0 : return []
        
        if curr == right :
            # 排除离开中枢又跌回中枢
            if close[-1] != max(close[left:]) : return []
            if high[-1]  != max(high[left:])  : return []

            # 中枢的最后1根K线
            if close[-1] >= zsgg and df.grow.iloc[-1] >= 5:
                info        = str(qos) + "7 >GG[" + str(zsgg) + "] " + str(change).rjust(5) + "%"
            elif close[-1] >= zsup :
                info        = str(qos) + "6 >UP[" + str(zsgg) + "] " + str(change).rjust(5) + "%"
        else:
            if high[-1] == max(high[left:]) :
                info        = str(qos) + "8 Out[" + str(zsgg) + "] " + str(change).rjust(5) + "%"
    else :
        gaps                = 0
        info                = str(qos) + "9 NZS " + str(change).rjust(5) + "%"
    if info == "" : return []
    
    if True \
    and ((amount >= 4 and change >= 5) or (amount >= 8 and change >= 2))\
    and True :
        sclose              = strClose(close[left])
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", close[-1], grow, gaps, info]
        #print(code, name, result, avgm, avgc)
        return result
    return []
def findHalfGrow111(code, name, testFlag, df, half, qos):
    funcEntry               = False
    delta                   = 3
    info                    = ""
    # 日线强势
    if df.tur.tail(delta).sum() == 0 : return []
    if half.close.iloc[-1] <= half.cls30.iloc[-1]: return []
    
    idxmax                  = half.high.tail(delta*8).idxmax()
    gaps                    = half.close.index[-1] - idxmax
    if half.close.iloc[-1] == half.close.iloc[idxmax:].min() : return []
    
    amount                  = df.amount.iloc[-1]
    change                  = df.change.iloc[-1]
    cnt10                   = half.where(half.low < half.cls10).dropna().index
    cnt20                   = half.where(half.low < half.cls20).dropna().index
    cnt30                   = half.where(half.low < half.cls30).dropna().index
    isDw10                  = True if len(cnt10) and cnt10[-1] > idxmax else False
    isDw20                  = True if len(cnt20) and cnt20[-1] > idxmax else False
    isDw30                  = True if len(cnt30) and cnt30[-1] > idxmax else False
    #print(code, name, idxmax, len(half), isDw10, isDw20, isDw30) 
    
    # low不破ma10
    if gaps >= 10 :
        if not isDw10 and not isDw20 and not isDw30:
            if  half.close.iloc[-1] > half.cls10.iloc[-1] :
                info        = str(qos) + "9 P5 " + str(change).rjust(5) + "%"
        elif isDw10 and not isDw20 and not isDw30:
            if  half.close.iloc[-1] > half.cls10.iloc[-1] :
                info        = str(qos) + "8 P10 " + str(change).rjust(5) + "%"
        elif isDw10 and isDw20 and not isDw30:
            if  half.close.iloc[-1] > half.cls20.iloc[-1] :
                info        = str(qos) + "7 P20 " + str(change).rjust(5) + "%"
        elif isDw10 and isDw20 and isDw30:
            if  half.close.iloc[-1] > half.cls20.iloc[-1] :
                info        = str(qos) + "6 P30 " + str(change).rjust(5) + "%" 
    else:
        if half.tur.iloc[-2:].sum() == 0 : return []
        if half.close.iloc[-1] > half.cls10.iloc[-1]:
            info            = str(qos) + "5 Tur " + str(change).rjust(5) + "%"
    if info == '' : return []

    close                   = df['close'].tolist()
    grow                    = round(close[-1] / df.close.tail(90).min(), 2)
    if True \
    and ((amount >= 3 and change >= 3) or (amount >= 8 and change >= 2))\
    and grow < 1.5 \
    and True :
        sclose              = strClose(half.close.iloc[idxmax])
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", close[-1], grow, gaps, info]
        #print(code, name, result, avgm, avgc)
        return result
    return []
#####################################################################
############################# 废弃函数 ###############################
#####################################################################
############################# 股价启动点 ###########################
def findCrossInfo(code, name, testFlag, df, wins):
    # 上穿n日线后不破n日线
    df['delta']             = [1 if a<b else 0 for a,b in zip(df.close, df['cls'+str(wins)])]
    clist                   = list(df.delta.where(df.delta == 1).dropna().index)
    last                    = 0 if len(clist) == 0 or clist[-1] == df.index.values[-1] else clist[-1] + 1
    del df['delta']
    less5                   = [1 if a < b else 0 for a,b in zip(df.close, df.cls5)]
    less10                  = [1 if a < b else 0 for a,b in zip(df.close, df.cls10)]
    less20                  = [1 if a < b else 0 for a,b in zip(df.close, df.cls20)]
    cnt5                    = sum(less5 [last:])
    cnt10                   = sum(less10[last:])
    cnt20                   = sum(less20[last:])
    return last, cnt5, cnt10, cnt20
############################# 周线连续上涨 #########################
def findWeekCross(code, name, testFlag, half, df, qos):
    funcEntry               = False
    info                    = ""
    # half/日线强势有涨停
    if df.close.iloc[-1]   <= df.cls5.iloc[-1]    : return []
    #if half.close.iloc[-1] <= half.cls20.iloc[-1] : return []
    if max(df.seal.tail(5))== 0 : return []
    
    # 上穿20日线后不破20日线
    week                    = getMergData("week", df)
    procMaData(week)
    if week.close.iloc[-1] < week.cls10.iloc[-1] : return []
    last, cnt5, cnt10, cnt20= findCrossInfo(code, name, testFlag, week, 20)
    if last == 0 :  return []

    procTurtleData(week, 20)
    close                   = week['close'].tolist()
    change                  = week['change'].tolist()
    amount                  = week.amount.iloc[-1]
    grow                    = round(close[-1] / close[last], 2)
    gaps                    = week.index.values[-1] - last
    # 线跌破5周线,不破10周线，又重回5周线
    if cnt5 and cnt10 == 0 \
    and close[-2] < week.cls5.iloc[-2] and close[-1] >= week.cls5.iloc[-1] :
        info                = str(qos) + "9 P5 : L(" + str(cnt5) + ") " + str(change[-1]).rjust(5) + "%"
    # 跌破10周线,不破20周线，又重回5周线
    elif cnt10 > 0 and cnt20 == 0 \
    and close[-2] < week.cls10.iloc[-2] and close[-1] >= week.cls10.iloc[-1] :
        info                = str(qos) + "8 P10 : L(" + str(cnt10)+ ") " + str(change[-1]).rjust(5) + "%"
    if info == "" : return []
    if testFlag:
        print("testWeek:", code, name, last, len(week), cnt5, cnt10, cnt20)
        
    if True \
    and (amount >= 5 and change[-1] >= 5) \
    and gaps >= 5 \
    and week.tur.iloc[-1] \
    and True :
        sclose              = strClose(close[-1])
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", close[last], grow, gaps, info]
        #print(code, name, result, avgm, avgc)
        return result
    return []
############################# 日线连续上涨 #########################
def findDyasCross(code, name, testFlag, half, df, qos):
    global mlevel, clevel, glevel, times
    funcEntry               = False
    info                    = ""
    # half/日线强势
    if df.close.iloc[-1]   <= df.cls20.iloc[-1]   : return []
    #if half.close.iloc[-1] <= half.cls20.iloc[-1] : return []
    # 上穿20日线后不破20日线
    last, cnt5, cnt10, cnt20= findCrossInfo(code, name, testFlag, df, 20)
    if last == 0 : return []
    close                   = df['close'].tolist()
    amount                  = df.amount.iloc[-1]
    grow                    = round(close[-1] / df.close[last], 2)
    gaps                    = df.index.values[-1] - last
    avgm                    = round(df.amount.tail(gaps).mean(), 1)
    avgc                    = round(df.change.tail(gaps).mean(), 1)
    if testFlag:
        print(code, name, avgm, avgc, last, len(df), cnt5, cnt10, cnt20)
    
    # 日线不破5日线
    if cnt5 == 0 :
        if  close[-2] > df.cls5.iloc[-2] and df.low.iloc[-1] < df.cls5.iloc[-1] \
        and close[-1] > df.cls5.iloc[-1] and df.tur.iloc[-1] :
        #and df.high.iloc[-1] == df.high.tail(gaps).max() :
            info                = str(qos) + "9 P5 : L(" + str(cnt5) + ") " + str(avgc).rjust(5) + "%"
    # 日线跌破5日线,不破10日线，又重回5日线
    elif cnt5 and cnt10 == 0 :
        if (close[-2] < df.cls5.iloc[-2] and close[-1] >= df.cls5.iloc[-1]) \
        and df.tur.iloc[-1] :
            info            = str(qos) + "8 P5 : L(" + str(cnt5) + ") " + str(avgc).rjust(5) + "%"
        if df.tur.iloc[-2] == 0 and df.tur.iloc[-1] :
            info            = str(qos) + "7 P5 : L(" + str(cnt5) + ") " + str(avgc).rjust(5) + "%"
    # 日线跌破10日线,又重回10日线
    elif cnt10 > 0 :
        if (df.tur.iloc[-2] == 0 or df.tur.iloc[-3] == 0) \
        and df.tur.iloc[-1] :
            info            = str(qos) + "7 P10 : L(" + str(cnt10)+ ") " + str(avgc).rjust(5) + "%"
    
    if info == "" : return []
    procMacdData(df)
    if df.dif.iloc[-1]     < df.dea.iloc[-1] : return []
    if df.dea.iloc[-1]     < 0 : return []
    
    if True \
    and ((avgm >= 2 and avgc >= 2) or max(df.seal.tail(5)) >= 1) \
    and gaps >= 5 \
    and (amount >= 3 or df.change.iloc[-1] >= 10) \
    and grow <= 1.4 \
    and True :
        sclose              = strClose(close[-1])
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", close[last], grow, gaps, info]
        #print(code, name, result, avgm, avgc)
        return result
    return []
