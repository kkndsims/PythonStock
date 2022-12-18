# -*- coding: utf-8 -*-
# -*- coding:gb2312 -*-

"""
Created on Mon Oct 10 22:59:41 2016

@author: Administrator
"""

import os, sys#, re #, datetime, codecs, chardet
import pandas  as pd
import numpy   as np
import warnings
warnings.filterwarnings( "ignore" )

maList                  = [5, 20, 60, 144] 
amList                  = [5, 10]    
bollList                = [20]
turtleDays              = 20
amountUnit              = 100000000  #1亿

wins                    = 10
isMacdMode              = 0     #0:short; 1:long
macdFast                = 24 if isMacdMode == 1 else 12
macdSlow                = 52 if isMacdMode == 1 else 26
macdMidd                = 18 if isMacdMode == 1 else 9

daysEnab                = 15    #1111 1111
daysEnab                = 15    #1111 1111

maxch                   = 0
limit                   = []
empty                   = pd.DataFrame()

######################################################################
############################# 各种算法处理买点 ########################
######################################################################
def getStockBuy(code, name, baseInfo, testFlag, hfile, dfile, wfile, info):
    global maxch
    funcEntry           = False
    findFlag            = False
    findFlag            = True
    maxch               = len(name) if len(name) > maxch else maxch
    name                = name.rjust(maxch)
    flag, day, half     = readDataFromFile(code, name, dfile, hfile)    
    if not flag :
        return findFlag, code, name
    
    # 数据清洗，去除成交量或者换手率不足的个股
    if not procBasicPrice(day):
        return False, code, name
    # 数据清洗，海龟：60days；顶点：10days    
    procBasicInfo(day, 60, 10)

    # 缩量涨停，一致看多
    if daysEnab & findFlag & 0:
        result          = findSealing(code[2:], name, testFlag, day, half, info)
        if result[0]:
            return result
    # 强势洗盘，巨大振幅，2~3天内收复前高，不破10日均线
    if daysEnab & findFlag & 1:
        result          = findStrWash(code[2:], name, testFlag, day, half, info)
        if result[0]:
            return result
    # 涨停点火，调整后中继创新高
    if daysEnab & findFlag & 0:
        result          = findFstSeal(code[2:], name, testFlag, day, half, info)
        if result[0]:
            return result
    # 成交放量，继续放量/缩量调整
    if daysEnab & findFlag & 1:
        result          = findFireup(code[2:], name, testFlag, day, half, info)
        if result[0]:
            return result
        
    #寻找长期超跌股
    if daysEnab & findFlag & 0:
        result          = findDecrease(code[2:], name, testFlag, day, half, info)
        if result[0]:
            return result    
    #寻找长期超跌后趋势反转
    if daysEnab & findFlag & 0:
        result          = findTrence(code[2:], name, testFlag, day, half, info)
        if result[0]:
            return result

    return False, code, name
    sys.exit(0)

############################# 缩量涨停 ###################################
def findSealing(code, name, testFlag, days, half, inn):
    if inn['封单额'].iloc[-1] != "--  " and inn['涨幅%'].iloc[-1] > "0":
        amount              = days['amount'].iloc[-1]
        change              = days['change'].iloc[-1]
        close               = days['close' ].iloc[-1]
        sclose              = strClose(close)
        if amount and amount < days['amount'].iloc[-2] :
            minval          = days['close'].tail(20).min()
            minidx          = days['close'].tail(20).idxmin()
            grow            = round(close / minval, 2)
            gaps            = len(days) - minidx
            if grow >= 1.4:
                return False, code, name
            info            = "9.9.2缩量涨停:" +str(change).rjust(5) + "%"
            result          = [True, code, name.rjust(4), sclose, str(amount)+"亿", minval, grow, gaps, info]
            return result
    return False, code, name
############################# 强势洗盘，巨幅震荡收复前高 ###################################
def findStrWash(code, name, testFlag, days, half, inn):
    close                   = days['close'].tolist()
    amount                  = days['amount'].iloc[-1]
    change                  = days['change'].iloc[-1]
    days['ampl']            = (days['high'] / days['low']).round(decimals=2)
    days['wash']            = [1 if a >= 1.15 and b < 0 else 0 for a, b in zip(days['ampl'].tolist(), days['grow'].tolist())]
    gtAmpl                  = days.where(days.wash == 1).dropna().index
    point                   = gtAmpl[-1] if len(gtAmpl) else 0
    gaps                    = len(days) - point
    #print(days.tail(10))
    # 去除新股，最近三天没有巨幅震荡的个股，刚巨幅震荡的个股
    if len(days) <= 5 or gaps > 5 or gaps == 1:
        return False, code, name
    
    fireup                  = close[point]
    isTur                   = 1 if close[-1] >= max(close[point-5:]) else 0
    fw0                     = gaps + maList[-1]
    fw1                     = gaps + maList[-2]
    grow0                   = round(close[-1] / days['close'].tail(fw0).min(), 2)
    grow1                   = round(close[-1] / days['close'].tail(fw1).min(), 2)
    sclose                  = strClose(close[-1])
    if testFlag:
        print(code, name, len(days), point, gaps, fireup, grow0, grow1)
    if isTur :
        info                = "9.9.1 巨幅洗盘" + str(change).rjust(5) + "%"
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", fireup, grow1, gaps, info]
        return result
    return False, code, name
############################# 首板后调整中继 ###################################
def findFstSeal(code, name, testFlag, days, half, inn):
    close                   = days['close'].tolist()
    amount                  = days['amount'].iloc[-1]
    change                  = days['change'].iloc[-1]
    fstSeal                 = days.where(days.fstS == 1).dropna().index
    point                   = fstSeal[-1] if len(fstSeal) else 0
    flag                    = dataWashPrice(days, 3, 5)
    # 去除成交量太小的个股, 去除没有涨停的个股
    if not flag or point == 0:
        return False, code, name
    fireup                  = close[point]
    gaps                    = len(days) - point
    # 去除刚开始涨或者调整小于突破价的个股
    if gaps <= 1 or close[-1] < fireup:
        return False, code, name
    botNum                  = days[point:].where(days.v == -1).dropna().index
    # 去除调整出现多次低点的个股
    if len(botNum) > 2:
        return False, code, name
    # 符合条件的个股分类
    isSeal                  = days['seal'].iloc[-1]
    isTur                   = 1 if close[-1] == max(close[point+1:]) else 0
    mincls                  = min(close[point+1:])
    fw0                     = gaps + maList[-1]
    fw1                     = gaps + maList[-2]
    grow0                   = round(close[-1] / days['close'].tail(fw0).min(), 2)
    grow1                   = round(close[-1] / days['close'].tail(fw1).min(), 2)
    sclose                  = strClose(close[-1])
    info                    = ""
    if       isSeal and mincls >= fireup:
        info                = "9.8.4 涨停不破"
    elif     isSeal and mincls  < fireup and close[-1] >= fireup :
        info                = "9.8.3 涨停跌破"
    # 至少3天完成1个30分钟级别的调整，9天完成1个日线级别的调整
    elif not isSeal and mincls >= fireup and isTur and gaps >= 4:
        info                = "9.8.2 海龟不破"
    elif not isSeal and mincls  < fireup and isTur and gaps >= 4 :
        info                = "9.8.1 海龟跌破"
    else:
        info                = ""
    if testFlag:
        print(code, name, len(days), point, gaps, fireup, grow0, grow1)
    if info == "" or grow0 >= 1.8 or grow1 >= 1.4:
        return False, code, name
    info                    += str(change).rjust(5) + "%"
    result                  = [True, code, name.rjust(4), sclose, str(amount)+"亿", fireup, grow1, gaps, info]
    return result
############################# 点火放量大涨 ###################################
def findFireup(code, name, testFlag, days, half, inn):
    funcEntry               = False
    flag                    = dataWashPrice(days, 3, 5)
    if not flag:
        return False, code, name
    # 找出最近一次放量点
    plist                   = days.where(days.ppeak == 1).dropna().index
    point                   = plist[-1] if len(plist) else 0
    lens                    = len(days)
    gaps                    = lens - point
    close                   = days['close'].tolist()
    # 放量后am5最多只有1个高点和1个低点
    mlist                   = days[point:].where(days.m != 0).dropna().index
    mcnt                    = len(mlist)
    if mcnt > 2 or close[-1] < close[point] :
        return False, code, name
    #sys.exit(0)

    info                    = ""
    if mcnt :
        mpoint              = int(mlist[-1])
        tp                  = int(days.m[mpoint])
        gaps                = lens - mpoint
        if tp == -1 and close[-1] != min(close[point:]) and gaps <= 3:
            info            = '9.7.3 缩量结束'
        #if tp == -1 and close[-1] >= max(close[point:]) :
        #    info            = '9.7.2 调整结束'
        if tp ==  1 and close[-1] >= max(close[point:]) and gaps >= 5:
            info            = '9.7.0 缩量上涨'
    #else :
    #    info                = '9.7.1 放量上涨'
        
    amount                  = days['amount'].iloc[-1]
    change                  = days['change'].iloc[-1]
    grow                    = round(close[-1] / close[point], 2)
    grow                    = round(close[-1] / days['close'].tail(60).min(), 2)
    #flag                    = True if (amount >= 3 and change >= 5) or (amount < 3 and change >= 10) else False
    if testFlag:
        print(code, name, lens, point, gaps, close[point], max(close[point:]), close[-1], grow)
        print(mlist, mcnt)
        print(days.tail(20))
    if info == "" or grow >= 1.32:
        return False, code, name
    if True:
        sclose          = strClose(close[-1])
        info            = info + str(change).rjust(5) + "%"
        result          = [True, code, name.rjust(4), sclose, str(amount)+"亿", close[point], grow, gaps, info]
        return result
    return False, code, name

############################# 长期超跌股 ###################################
def findDecrease(code, name, testFlag, days, half, inn):
    mval                    = days['close'].tail(250).min()
    close                   = days['close'].tolist()
    grow                    = days['grow'].tolist()
    if close[-1] < days['ma_20'].iloc[-1]:
        return False, code, name
    # 找到最近的超跌点(跌破所有均线)是1年的最低点或附近(1.05),且马上出现涨停   
    dt                      = procPeakList(close, 5)
    dt                      = dt.where(dt.type == 0).dropna()
    flag                    = False
    idx                     = 0
    gaps                    = 0
    for i in range(len(dt)-1, 0, -1):
        idx                 = int(dt['idx'].iloc[i])
        gaps                = int(len(days) - idx)
        if  gaps       <= 10 \
        and close[idx] < mval * 1.05 \
        and close[idx] < days['ma_60'].iloc[idx] \
        and close[idx] < days['ma_144'].iloc[idx]\
        and ((days['grow'].iloc[idx+1:idx+4] > 9.9).sum() or days['grow'].iloc[idx+1:idx+4].sum() >= 10):
            flag            = True
            break
    if not flag:
        return False, code, name
    
    # 超跌后涨停并持续放量  
    amount                  = days['amount'].iloc[-1]
    change                  = days['change'].iloc[-1]
    grow                    = round(close[-1] / mval, 2)
    if testFlag:
        print(code, name, idx, len(days), gaps, mval, close[idx], close[-1], grow)
    if  change >= 3 \
    and True:
        sclose              = strClose(close[-1])
        info                = "9.9.超跌："  + str(change).rjust(5) + "%"
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", close[idx], grow, gaps, info]
        #print(result, grow)
        return result
    return False, code, name
############################# 超跌吸筹反弹 ################################
def findTrence(code, name, testFlag, days, half, inn):
    close                   = days['close'].tolist()
    for ma in reversed(maList[-2:]):
        if not pd.isna(  days['ma_'+str(ma)].iloc[-1]) \
        and close[-1] <  days['ma_'+str(ma)].iloc[-1] :
            return False, code, name
        days['sf'+str(ma)]  = days['ma_'+str(ma)].shift(1)
    days['cross']           = 0
    # 找出60上穿/下穿144均线
    days['cross']           = days.apply(lambda x : 1 if ((x.sf60 < x.sf144) & (x.ma_60 >= x.ma_144)) else -1 if ((x.sf60 >= x.sf144) & (x.ma_60 < x.ma_144)) else 0, axis=1)
    cross                   = days['cross'].tolist()
    peak                    = np.nonzero(cross)[0]
    if len(peak) == 0:
        return False, code, name
    
    # 找出60长期低于144，且60上穿144的点
    df                      = pd.DataFrame(peak, columns=['idx'])
    df['cross']             = 0
    index                   = 0
    for i in peak:
        df['cross'].iloc[index]     = cross[i]
        index               += 1
    df['gap']               = df['idx'] - df['idx'].shift(1)
    df['flag']              = df.apply(lambda x : 1 if x.cross == 1 and x.gap >= 120 else 0, axis=1)
    df                      = df.where(df.gap > 10).dropna()
    point                   = int(df['idx'].iloc[-1]) if len(df) else 0
    cnt                     = int(df['flag'].tail(1).sum())
    if len(df) == 0 or cnt == 0 or close[-1] < close[point]:
        return False, code, name
    # 上穿后最低点没有跌破均线
    midx                    = days['close'].iloc[point:].idxmin()
    if close[midx] < days['ma_'+str(maList[-1])].iloc[midx] \
    or days['close'].iloc[-1] < days['ma_20'].iloc[-1] \
    or days['ma_20'].iloc[-1] < days['ma_60'].iloc[-1] \
    or days['ma_60'].iloc[-1] < days['ma_144'].iloc[-1]:
        return False, code, name
    if testFlag :
        print(df)
    
    amount                  = days['amount'].iloc[-1]
    change                  = days['change'].iloc[-1]
    grow                    = round(close[-1] / days['close'].iloc[point-20:-1].min(), 2)
    # 找出上穿后最近的1个低点，如果没有低点则选择上穿点
    dt                      = procPeakList(close, 5)
    dt                      = dt.where(dt.type == 0).dropna()
    cnt                     = (dt['idx'] > point).sum()
    if cnt > 1 :
        return False, code, name
    bot                     = int(dt['idx'].iloc[-1]) if cnt == 1 else point
    gaps                    = int(len(days) - bot)
    if testFlag :
        print(dt.tail(5), cnt, point, bot, gaps)   
        
    if cnt <= 1 \
    and gaps <= 15  \
    and (change >= 2 and amount >= 2) \
    and grow < 1.5 \
    and True:
        sclose              = strClose(close[-1])
        info                = "9.8.吸筹：" + str(change).rjust(5) + "%"
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", close[bot], grow, gaps, info]
        #print(result, grow)
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
def readDataFromFile(code, name, dpath, hpath):
    if not os.path.exists(dpath) :
        return False, empty, empty
    ddata                   = pd.read_csv(dpath, parse_dates=[0])
    ddata.sort_values('date', ascending=True, inplace=True)
    if len(ddata) == 0:
        return False, empty, empty
    #ddata                   = procMaData  (ddata)
    #ddata                   = procMacdData(ddata)
    del ddata['open'], ddata['volume']
    return True, ddata, empty
############################# 海龟/均线/MACD/峰值 ####################
def procTurtleData(df, period) :
    # apply函数仿真时间是队列操作的3倍
    df['tur']               = df.close == df.close.rolling(period).max()
    df['tur']               = [1 if a else 0 for a in (df.tur)]
    return df
def procMaData(df) :
    for ma in maList:
        df['ma_'+str(ma)]   = df.close.rolling(ma).mean().round(decimals=2)
    return df
def procMacdData(df) :                         
    df['sema']              = pd.DataFrame.ewm(df.close,span=macdFast).mean().round(decimals=6) 
    df['lema']              = pd.DataFrame.ewm(df.close,span=macdSlow).mean().round(decimals=6)   
    df['dif']               = df.sema.round(decimals=2) - df.lema.round(decimals=6)       
    df['dea']               = pd.DataFrame.ewm(df.dif,span=macdMidd).mean().round(decimals=2)
    df['macd']              = 2*(df.dif.round(decimals=2) - df.dea.round(decimals=2))
    del df['sema'], df['lema']
    return df
def procSealing(df, gap) :
    df['price']             = round(df.grow * df.rate, 1)
    df['seal']              = [1 if a >= 9.8 else 0 for a in (df.grow)]
    df['fstS']              = df.seal.rolling(gap).sum()
    df['fstS']              = [1 if a == 1 and b == True else 0 for a, b in zip(df.seal, df.fstS)]
    return df    
def procBasicPrice(df) :
    isa                     = 1 if df['amount'].tail(30).max() >= 1 else 0
    isb                     = 1 if df['change'].tail(30).max() >= 2 else 0
    flag                    = 1 if (isa + isb) > 0 else 0
    return flag
def dataWashPrice (df, am, ch) :
    amount                  = df['amount'].iloc[-1]
    change                  = df['change'].iloc[-1]
    flag                    = True if (amount >= am and change >= ch) or (amount < ch and change >= ch * 2) else False
    return flag
def procBasicInfo(df, tur, gap) :
    baseFuncEntry           = False
    step                    = int(gap/2)
    index                   = len(df) - step + 1
    # 找出5天均线的顶/底
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
    #df['m'].iloc[-1]         = None
    # 找出海龟
    df['tur']               = df.close == df.close.rolling(tur).max()
    df['tur']               = [1 if a else 0 for a in df.tur]
    # 找出放量大涨、涨停、首次涨停
    df['price']             = (df.grow * df.rate).round(decimals=1)
    df['ppeak']             = df.price == df.price.rolling(gap, center=True).max()
    df['ppeak']             = [1 if a >= 10 and b == 1 else 0 for a, b in zip(df.price, df.ppeak)]   
    #df['ppeak'].iloc[-1]     = None
    df['seal']              = [1 if a >= 9.8 else 0 for a in df.grow]
    df['fstS']              = df['seal'].rolling(gap).sum()
    df['fstS']              = [1 if a == 1 and b == True else 0 for a, b in zip(df.seal, df.fstS)]
    # 找出close的顶/底
    cpeak                   = df.close.rolling(gap, center=True).max()
    cpoke                   = df.close.rolling(gap, center=True).min()
    for i in range(index, len(df) - 1):
        cpeak[i]            = df.close.iloc[i-step:].max()
        cpoke[i]            = df.close.iloc[i-step:].min()
    df['v']                 = [1 if (a == b) else -1 if (a == c) else 0 for a, b, c in zip(df.close, cpeak, cpoke)]
    df['v1']                = df['v'].shift(-1)
    df['v']                 = [a - 1 if (a == b and a ==  1) else a for a, b in zip(df.v, df.v1)]
    df['v']                 = [a + 1 if (a == b and a == -1) else a for a, b in zip(df.v, df.v1)]
    #df['v'].iloc[-1]         = None
    del df['m1'], df['v1']
    return df
############################# 寻找波峰波谷 ############################
def procPeakList(close, gap) :
    lens            = len(close)   
    hdiff           = np.diff(np.sign(np.diff(close)))
    ldiff           = np.diff(np.sign(np.diff(close)))
    hpeak           = (np.where(hdiff == -2)[0] + 1).tolist()
    lpoke           = (np.where(ldiff ==  2)[0] + 1).tolist() 
    top = []; bot = []; tol = []; rst = [];
    for x in hpeak:
        top.append([int(x), 1, close[x]])
    for x in lpoke:
        bot.append([int(x), 0, close[x]])
    tol             = top + bot
    tol.sort(reverse=False)

    for i in range(len(tol)):
        idx         = tol[i][0]
        lf          = idx - gap if idx >= gap else 0
        rh          = idx + gap if idx + gap <= lens else lens
        #print(tol[i], lf, rh, close[lf:rh])
        if tol[i][1] == 0 and close[idx] == min(close[lf:rh]):
            if len(rst) and rst[-1][1] == 0:
                rst.pop()
            rst.append(tol[i])
        if tol[i][1] == 1 and close[idx] == max(close[lf:rh]):
            if len(rst) and rst[-1][1] == 1:
                rst.pop()
            rst.append(tol[i])
    df                      = pd.DataFrame(rst, columns=['idx', 'type', 'close'])
    return df
############################# 寻找波峰波谷 ############################
def procPeak(data, gap) :
    df                      = pd.DataFrame(data.index)
    df.columns              = ['idx']
    df['close']             = data['close']
    half                    = int(gap / 2)
    index                   = max(data['close'].idxmin(), data['close'].idxmax()) - gap
    df                      = df.iloc[index:]
    df['peak']              = df['close'].rolling(gap, center=True).max()
    df['poke']              = df['close'].rolling(gap, center=True).min()
    peak                    = df['peak'].tolist()
    poke                    = df['poke'].tolist()
    for i in range(1, half):
        #print(i, df['close'].iloc[-i-half:].tolist())
        peak[-i]            = df['close'].iloc[-i-half:].max()
        poke[-i]            = df['close'].iloc[-i-half:].min()
    df['peak']              = peak
    df['poke']              = poke
    df['v']                 = df.apply(lambda x: 1 if (x.close == x.peak) else -1 if (x.close == x.poke) else 0, axis=1)
    #print(len(df))
    #print(df.tail(10))
    df                      = df.where(df.v != 0).dropna()
    df['v1']                = df['v'].shift(-1)
    df['v']                 = df.apply(lambda x: x.v - 1 if (x.v == x.v1 and x.v ==  1) else x.v, axis=1)
    df['v']                 = df.apply(lambda x: x.v + 1 if (x.v == x.v1 and x.v == -1) else x.v, axis=1)
    df                      = df.where(df.v != 0).dropna()
    #print(df.tail(20))
    del df['peak'], df['poke'], df['v1']
    return df
#####################################################################
############################# 基础函数结束 ###########################
#####################################################################

         



#####################################################################
############################# 无效函数 ###############################
#####################################################################
    
############################# 巨量换手/振幅 ############################
def findChange(code, name, testFlag, days, half, inn):
    close                   = days['close'].iloc[-1]
    amount                  = days['amount'].iloc[-1]                
    change                  = days['change'].iloc[-1]
    scope                   = str(float(inn['振幅%'].iloc[-1]))+"%"
    grow                    = str(float(inn['涨幅%'].iloc[-1]))+"%"
    sclose                  = strClose(close)
    if change >= 50 and float(inn['涨幅%'].iloc[-1]) > 0 :
        info                = "9.8.巨量换手:" + str(change).rjust(5) + "%"
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", grow, scope, info]
        return result
    return False, code, name
############################# 突破回踩不破均线 ########################
def findMaBreak(code, name, testFlag, days, half, inn):
    close                   = days['close'].tolist()
    change                  = days['change'].iloc[-1]
    lens                    = len(days)
    for ma in reversed(maList):
        maval               = days['ma_'+str(ma)].tolist()
        if  ma >= 60 and not pd.isna(maval[-1]) and close[-1] > maval[-1]:
            cr              = [0 for i in range(lens)]
            for i in range(1, lens):
                if close[i-1] < maval[i-1] and close[i] >= maval[i]:
                    cr[i]   = 1
            days['cross']   = pd.DataFrame(cr)
            procTurtleData(days, 60)
            turcnt          = days['tur'].tail(1).sum()
            hastur          = np.nonzero(cr)[0]
            if len(hastur) == 0:
                return False, code, name
            
            gap             = lens - hastur[-1]    
            minval          = days['close'].tail(gap).min()
            minidx          = days['close'].tail(gap).idxmin()
            amount          = days['amount'].iloc[-1]
            grow            = round(close[-1] / min(close[-60:]), 2)
            if testFlag:
                print(code, name, lens, gap, hastur, minval, minidx)
        
            if  turcnt      \
            and gap         <= 9    \
            and close[-1]   >= days['ma_10'].iloc[-1] \
            and close[-1]   >= days['ma_20'].iloc[-1] \
            and minval      >= maval[minidx]    \
            and change      >= 5                \
            and grow        < 1.35              \
            and amount      >= 3                \
            and True :
                info        = "9.6.ma_" + str(ma) + "中继"
                sclose      = strClose(close[-1])
                info        += str(change).rjust(5) + "%"
                result      = [True, code, name.rjust(4), sclose, amount, grow, str(gap).rjust(2), info]
                return result
    return False, code, name
############################# 涨停分析 ################################
def findLimitup(code, name, testFlag, days, half, inn):
    if float(inn['涨幅%'].iloc[-1]) < 9.8:
        return False, code, name
    close                   = days['close'].tolist()
    for ma in reversed(maList):
        if not pd.isna(  days['ma_'+str(ma)].iloc[-1]) \
        and close[-1] >= days['ma_'+str(ma)].iloc[-1] :
            procTurtleData(days, 60)
            turcnt          = days['tur'].tail(1).sum()
            if turcnt == 0:
                return False, code, name
                
            amount          = days['amount'].iloc[-1]                
            change          = days['change'].iloc[-1]
            minval          = days['close'].iloc[-60:].min()
            minidx          = days['close'].iloc[-60:].idxmin()
            grow            = round(close[-1] / minval, 2)
            gaps            = len(days) - minidx - 1
            #if grow > 1.32 or change < 5:
            if grow > 1.32 or change < 5:
                return False, code, name
            sclose          = strClose(days['close'].iloc[-1])
            info            = "8.9.涨停：" + str(change).rjust(5) + "%"
            result          = [True, code, name.rjust(4), sclose, str(amount)+"亿", grow, gaps, info]
            return result
    return False, code, name

#####################################################################