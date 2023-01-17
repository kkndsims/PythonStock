# -*- coding: utf-8 -*-
# -*- coding:gb2312 -*-

"""
Created on Mon Oct 10 22:59:41 2016

@author: Administrator
"""

import os
import sys
import pandas as pd
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
    mothEnab            = 1
    weekEnab            = 1
    daysEnab            = 1
    findEnab            = 1
    testEnab            = 1
    maxch               = len(name) if len(name) > maxch else maxch
    name                = name.rjust(maxch)
    days                = readDataFromFile(code, name, dfile)  
    if len(days) == 0:
        return False, code, name

    #日线：上涨中继， 涨停后缩量调整结束， 海龟， 连板， 反包等处理 
    if daysEnab :
        if findEnab :                       # 连板 + 反包
            result      = findSealing(code[2:], name, testFlag, days, info, daysQos)
            if result[0]:
                return result
        if not procBasicPrice(days, 20):    #去除成交量或者换手率不足的个股
            return False, code, name
        if findEnab :                       #寻找上涨中继
            result      = findIncrease(code[2:], name, testFlag, days, info, daysQos)
            if result[0]:
                return result  
        if findEnab :                       # 涨停点火，继续放量/缩量调整结束
            result      = findFireup(code[2:], name, testFlag, days, info, daysQos)
            if result[0]:
                return result
        if findEnab :                       # 海龟破新高
            result      = findTurtle(code[2:], name, testFlag, days, info, daysQos)
            if result[0]:
                return result
    #周线：29周海龟(7个月建仓时间) or 周线最低点放量，散户清仓
    if weekEnab :
        data            = readDataFromFile(code, name, wfile)
        if len(data):
            result      = findWeekTurtle(code[2:], name, testFlag, data, days, 29, weekQos)
            if result[0]:
                return result 
    #月线：30月海龟(2~3年建仓时间) or 月线最低点放量，散户清仓
    if mothEnab :
        data            = readDataFromFile(code, name, mfile)
        if len(data) :
            result      = findMonthTurtle(code[2:], name, testFlag, data, days, 20, mothQos)
            if result[0]:
                return result
    return False, code, name
    sys.exit(0)
    
############################# 月线海龟 or 月线最低点换手 ###########################
def findMonthTurtle(code, name, testFlag, data, days, period, num):
    funcEntry               = False
    lens                    = len(data)
    if lens < period :
        return False, code, name
    change                  = data.change.iloc[-1]
    amount                  = data.amount.iloc[-1]
    close                   = data.close.tolist()
    sclose                  = strClose(close[-1])
    cmaxidx                 = data.close.idxmax()
    cmax                    = close[cmaxidx]
    cminidx                 = data.close.idxmin()
    cmin                    = close[cminidx]
    down                    = 0 if cmaxidx > cminidx else round(cmin / cmax, 2)
    grow                    = round(close[-1] / cmin, 2)
    gaps                    = lens - cminidx
    # 月线长期横盘，所有人成本一样，和新股一样，没有下跌空间    
    procTurtleData(data, period)    # 计算月线close海龟
    procTurtleData(days, period)    # 计算日线close海龟
    tur0                    = data.tur.iloc[-1]
    tur1                    = days.tur.iloc[-1]
    if tur0 and tur1 :              # 月线日线同时海龟
        procVector(data, 10)
        pbotlist            = data.where(data.v == -1).dropna().index
        pbot                = pbotlist[-1] if len(pbotlist) else 0
        grow                = grow if pbot <= cminidx else round(close[-1] / close[pbot], 2)
        grow1               = grow if cmaxidx < cminidx else cmax/cmin
        if testFlag:
            print(code, name, cmax, cmin, down, cmaxidx, cminidx, "split:", close[cminidx], close[-1], grow, gaps)
            print(data.tail(10))
        if change < 5 or grow >= 1.6 or grow1 > 2 or amount < 4:
            return False, code, name
        # 下跌到最低点，盘整出现高点和低点，close突破前高
        info                = str(num) + ".9 " + "M.Tur " + str(change).rjust(5) + "%"
        #print(code, name, cmax, cmin, down, cmaxidx, cminidx, "split:", close[-1], grow, gaps)
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", cmin, grow, gaps, info]
        return result
    # 月线最低点换手，散户卖出，机构入场
    if gaps >= 5 or gaps < 3 or down > 0.4 or change < 5 or amount < 5 :
        return False, code, name
    if close[-1] != max(close[cminidx:]) :
        return False, code, name
    for i in range(len(mtList)):
        ma                  = mtList[i]
        data[ma]            = data.close.rolling(ma).mean().round(decimals=2)
        if close[-1] < data[ma].iloc[-1] :
            return False, code, name
    if data.volume[cminidx-3:].max() != data.volume.tail(period).max() :
        return False, code, name
    if grow >= 1.1 and grow < 1.5 :
        info                = str(num) + ".8 M.Low " + str(change).rjust(5) + "%"
        #print(code, name, cmax, cmin, down, "split:", close[-1], grow, gaps)
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", cmin, grow, gaps, info]
        return result
    return False, code, name
############################# 周线海龟 or 周线最低点换手 ##########################
def findWeekTurtle(code, name, testFlag, week, days, period, num):
    funcEntry               = False
    lens                    = len(week)
    if lens < period :
        return False, code, name
    change                  = week.change.iloc[-1]
    amount                  = week.amount.iloc[-1]
    close                   = week.close.tolist()
    sclose                  = strClose(close[-1])
    cmaxidx                 = week.close.idxmax()
    cmax                    = close[cmaxidx]
    cminidx                 = week.close[cmaxidx:].idxmin()
    cmin                    = close[cminidx]
    down                    = round(cmin / cmax, 2)
    grow                    = round(close[-1] / cmin, 2)
    gaps                    = lens - cminidx
    procTurtleData(week, period)    # 计算周线close海龟
    procTurtleData(days, period)    # 计算日线close海龟
    tur0                    = week.tur.iloc[-1]
    tur1                    = days.tur.iloc[-1]
    if tur0 and tur1 :              # 周线日线同时海龟
        procVector(week, 20)        # 获取周线vector
        pbotlist            = week.where(week.v == -1).dropna().index
        pbot                = pbotlist[-1] if len(pbotlist) else 0
        if testFlag:
            print(code, name, cmax, cmin, down, close[pbot], close[-1], grow, gaps)
            print(week.tail(20))
        # 下跌腰斩，最低点上涨，close突破前高
        if down <= 0.5 and grow < 1.5 and change >= 5 and amount >= 5:
            info            = str(num) + ".9 " + "W.Tur " + str(change).rjust(5) + "%"
            sclose          = strClose(close[-1])
            #print(code, name, cmax, cmin, down, close[pbot], close[ptop], close[-1], grow, gaps, info)
            result          = [True, code, name.rjust(4), sclose, str(amount)+"亿", close[pbot], grow, gaps, info]
            return result
    # 超跌，最低点放巨量
    if down < 0.35 and gaps <= 5 and close[-1] == max(close[cminidx:]) :
        lleft               = 3*gaps if 3*gaps >= 15 else 15
        vmaxidx             = week.volume.tail(lleft).idxmax()
        #print(code, name, vmaxidx, lens, lens-vmaxidx, close[vmaxidx], close[-1])
        if grow < 1.10 or close[-1] < close[vmaxidx] or change < 2 or amount < 2:
            return False, code, name
        left                = 1 if vmaxidx < cmin else 0
        info                = str(num) + ".8 W.lf " if left else str(num) + ".7 W.rh "
        info                += str(change).rjust(5) + "%"
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", cmin, grow, gaps, info]
        return result
    return False, code, name
############################# 上涨中继 ###################################
def findIncrease(code, name, testFlag, days, inn, num):
    funcEntry               = False
    close                   = days['close'].tolist()
    sclose                  = strClose(close[-1])
    amount                  = days['amount'].iloc[-1]
    change                  = days['change'].iloc[-1]
    # 均线多头发散
    for i in range(len(maList) - 1):
        ma0                 = "ma_" + str(maList[i])
        ma1                 = "ma_" + str(maList[i+1])
        if close[-1] < days[ma0].iloc[-1] or days[ma0].iloc[-1] < days[ma1].iloc[-1]:
            return False, code, name
    # 没有1个低点，发生在新股中
    bot                     = days.where(days.v == -1).dropna().index
    if len(bot) == 0 :
        if close[-1] == max(close) and change >= 10:
            info            = str(num) + ".8 新股新高 " + str(change).rjust(5) + "%"
            minidx          = days['close'].idxmin()
            grow0           = round(close[-1] / close[minidx], 2)
            gaps            = len(days) - minidx
            result          = [True, code, name.rjust(4), sclose, str(amount)+"亿", close[minidx], grow0, gaps, info]
            return result
        return False, code, name
    # 最近的低点处均线发散，有涨停或者破新高
    p1                      = bot[-1]
    gaps                    = len(days) - p1
    if gaps <= 5 and change >= 2 and amount >= 2 :
        top                 = days.where(days.v == 1).dropna().index[-1]
        grow                = round(close[-1] / days.close.tail(144).min(), 2)
        gcnt                = len(days.iloc[p1:].where(days.grow >= 9.7).dropna())
        flag1               = close[-1] == max(close[p1:])
        flag2               = close[-1] >= close[top]
        avrg                = round(days.iloc[p1, [7, 8, 9, 10]].mean(), 2)
        sstd                = round(days.iloc[p1, [7, 8, 9, 10]].std(), 2)
        rate                = str(round(sstd / avrg * 100, 1)).rjust(4) + "%"
        if flag1 and (gcnt or flag2) and grow < 1.5 : 
            info            = str(num) + ".9 多头 " + str(change).rjust(5) + "%"
            result          = [True, code, name.rjust(4), sclose, str(amount)+"亿", rate, grow, gaps, info]
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
        info                = str(num) + ".7 调整结束" + str(change).rjust(5) + "%"
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", close[point], grow, gaps, info]
        return result
    return False, code, name
############################# 海归破新高 ###################################
def findTurtle(code, name, testFlag, days, inn, num):
    funcEntry               = False
    flag                    = dataWashPrice(days, 3, 5)
    procTurtleData(days, 60)
    if not flag or days['tur'].iloc[-1] != 1:
        return False, code, name
    bot                     = days.where(days.v == -1).dropna().index
    if len(bot) < 2:
        return False, code, name
    
    close                   = days['close'].tolist()
    sclose                  = strClose(close[-1])
    amount                  = days['amount'].iloc[-1]
    change                  = days['change'].iloc[-1]
    b2                      = bot[-1]
    b1                      = bot[-2]
    fireup                  = close[b1]
    gaps                    = len(days) - b2
    grow1                   = round(close[-1] / close[b1], 2)
    grow2                   = round(close[-1] / min(close[-60:]), 2)
    if testFlag:
        print(code, name, close[b1], close[b2], gaps, fireup, grow1, grow2)
    if (close[b2] >= close[b1] and grow2 < 1.5) :
        info                = str(num) + ".6 tur " + str(change).rjust(5) + "%"
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", fireup, grow2, gaps, info]
        return result
    return False, code, name
############################# 缩量涨停 ###################################
def findSealing(code, name, testFlag, days, inn, num):
    funcEntry               = False
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
        info                = str(num) + ".5 " + str(scnt) + "连板 " if flag0 else str(num) + ".4 反包 "
        info                += str(change).rjust(5) + "%"
        bot                 = days.where(days.v == -1).dropna().index
        point               = 0 if len(bot) == 0 else bot[-1]
        gaps                = len(days) - point
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", minval, grow1, gaps, info]
        return result
    return False, code, name
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
def strMount(mount):
    return str(mount).rjust(6)+"亿"
############################# 从文件读数据 ###########################
def readDataFromFile(code, name, path):
    if not os.path.exists(path) :
        return empty
    data                    = pd.read_csv(path, parse_dates=[0])
    data.sort_values('date', ascending=True, inplace=True)
    return data
############################# 海龟/均线/MACD/峰值 ####################
def procBasicPrice(df, period) :
    if df.close.iloc[-1] < df['ma_5'].iloc[-1]:
        return 0
    procTurtleData(df, period)
    isa                     = 1 if df['amount'].tail(period).max() >= 2 else 0
    isb                     = 1 if df['change'].tail(period).max() >= 2 else 0 
    isc                     = 1 if df['grow'  ].tail(period).max() >= 5 else 0
    isd                     = 1 if df['tur'   ].tail(period).sum() >= 1 else 0
    flag                    = 1 if (isa + isb + isc + isd) > 0 else 0
    return flag
def dataWashPrice (df, am, ch) :
    amount                  = df['amount'].iloc[-1]
    change                  = df['change'].iloc[-1]
    flag                    = True if (amount >= am and change >= ch) or (amount >= (ch - 1) and change >= ch * 2) else False
    return flag
def procBasicInfo(df, tur, gap) :
    baseFuncEntry           = False
    procTurtleData(df, tur) # 处理海龟
    procMaData(df)          # 处理close均线
    #procPrice (df, gap)     # 处理放量大涨
    procSeal  (df, gap)     # 处理首次涨停
    procMoney (df, gap)     # 找出5天成交额的顶/底
    procVector(df, gap)     # 找出close的顶/底
def procTurtleData(df, period) :
    # apply函数仿真时间是队列操作的3倍
    df['tur']               = df.close == df.close.rolling(period).max()
    df['tur']               = [1 if a else 0 for a in (df.tur)]
def procMaData(df) :
    for ma in maList:
        df['ma_'+str(ma)]   = df.close.rolling(ma).mean().round(decimals=2)
def procMacdData(df) :                         
    df['sema']              = pd.DataFrame.ewm(df.close,span=macdFast).mean().round(decimals=6) 
    df['lema']              = pd.DataFrame.ewm(df.close,span=macdSlow).mean().round(decimals=6)   
    df['dif']               = df.sema.round(decimals=2) - df.lema.round(decimals=6)       
    df['dea']               = pd.DataFrame.ewm(df.dif,span=macdMidd).mean().round(decimals=2)
    df['macd']              = 2*(df.dif.round(decimals=2) - df.dea.round(decimals=2))
    del df['sema'], df['lema']
def procSeal  (df, gap) :
    df['seal']              = [1 if a >= 9.8 else 0 for a in df.grow]
    df['fstS']              = df['seal'].rolling(gap).sum()
    df['fstS']              = [1 if a == 1 and b == True else 0 for a, b in zip(df.seal, df.fstS)]
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
    step                    = int(gap/2)
    index                   = 0 if len(df) < step else len(df) - step + 1
    cpeak                   = df.close.rolling(gap, center=True).max()
    cpoke                   = df.close.rolling(gap, center=True).min()
    for i in range(index, len(df) - 1):
        cpeak[i]            = df.close.iloc[i-step:].max()
        cpoke[i]            = df.close.iloc[i-step:].min()
    df['v']                 = [1 if (a == b) else -1 if (a == c) else 0 for a, b, c in zip(df.close, cpeak, cpoke)]
    df['v1']                = df['v'].shift(-1)
    df['v']                 = [a - 1 if (a == b and a ==  1) else a for a, b in zip(df.v, df.v1)]
    df['v']                 = [a + 1 if (a == b and a == -1) else a for a, b in zip(df.v, df.v1)]
    df['v'].loc[-1]         = 0
    del df['v1']
#####################################################################
############################# 基础函数结束 ###########################
#####################################################################