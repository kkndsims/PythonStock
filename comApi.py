# -*- coding: utf-8 -*-
# -*- coding:gb2312 -*-

"""
Created on Mon Oct 10 22:59:41 2016

@author: Administrator
"""

import os, sys#, re #, datetime, codecs, chardet
import pandas  as pd
import numpy   as np

maList                  = [5, 10, 20, 60, 144] 
maList                  = [5, 10, 20, 60, 144]    
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

######################################################################
############################# 各种算法处理买点 ########################
######################################################################
def getStockBuy(code, name, baseInfo, testFlag, hfile, dfile, wfile, info):
    global maxch
    funcEntry           = False
    findFlag            = False
    findFlag            = True
    testFlag            = False
    maxch               = len(name) if len(name) > maxch else maxch
    name                = name.rjust(maxch)
    flag, day, half     = readDataFromFile(code, name, dfile, hfile)    
    if not flag :
        return findFlag, code, name
    
    #寻找超跌后趋势反转
    if daysEnab & findFlag & 1:
        result          = findTrence(code[2:], name, testFlag, day, half, info)
        if result[0]:
            return result
    # 成交放量
    if daysEnab & findFlag & 1:
        result          = findAmount(code[2:], name, testFlag, day, half, info)
        if result[0]:
            return result
        
    return False, code, name
    #sys.exit(0)

############################# 放量后缩量调整，或者刚放量 ############################
def findAmount(code, name, testFlag, days, half, inn):
    close               = days['close'].tolist()
    if close[-1] < days['ma_5' ].iloc[-1] :
        return False, code, name
    if close[-1] < days['ma_10'].iloc[-1] :
        return False, code, name
    if close[-1] < days['ma_144'].iloc[-1] :
        return False, code, name
    vrate               = days['rate'].tolist()
    nrate               = [a if a >= 2 else 0 for a in vrate]
    haspeak             = np.nonzero(nrate)[0]
    days['vma_5']       = days['amount'].rolling(window= 5).mean().round(decimals=2)
    days['vma_10']      = days['amount'].rolling(window=10).mean().round(decimals=2)
    if len(haspeak) == 0:
        return False, code, name
    procTurtleData(days, 60)
    turcnt              = days['tur'].tail(1).sum()
    if turcnt == 0 :
        return False, code, name
    idxpeak             = haspeak[-1]
    idxend              = 0
    peak                = 0
    avrg                = 0
    info                = "9.5.放量"
    lens                = len(days)
    gap                 = len(days) - idxpeak
    minval              = days['close'].iloc[-90:].min()
    grow                = round(close[-1] / minval,2 )
    change              = days['change'].iloc[-1]
    amount              = days['amount'].iloc[-1]
    growcnt             = (days['grow'] > 7).tail(gap).sum()
    if growcnt == 0:
        return False, code, name        
    if not (days['amount'].iloc[idxpeak] >= 3 or days['change'].iloc[idxpeak] >= 5):
        return False, code, name
    if days['vma_5'].iloc[idxpeak] < days['vma_10'].iloc[idxpeak] :
        return False, code, name
    #print(code, name, idxpeak, lens, gap)
    for i in range(idxpeak, lens):
        #print(close[i])
        if days['vma_5'].iloc[i] < days['vma_10'].iloc[i] :
            #print(idxpeak, idxend, lens, gap)
            idxend      = i 
            peak        = max(close[idxpeak:idxend])
            avrg        = days['amount'].iloc[idxpeak:idxend].mean()
            info        += "_缩量"
            #print("peak start at [%d:%d], peak = %.2f" %(idxpeak, idxend, peak))
            break
    if idxend == 0:     # 量能继续增加
        peak            = days['close'].iloc[idxpeak-20:idxpeak].max()
        avrg            = days['amount'].iloc[idxpeak:].mean()
        if amount < 3 or avrg < 2 or gap < 4:
            return False, code, name
        info            += "_放量"
        #print("peak start at end[%d:%d], peak = %.2f" %(idxpeak, lens, peak))
            
    if close[-1]    >= peak \
    and gap         <= 30   \
    and change      >= 2    \
    and grow        <= 1.5  \
    and True :
        sclose          = strClose(days['close'].iloc[-1])
        info            += str(change).rjust(5) + "%"
        result          = [True, code, name.rjust(4), sclose, str(amount)+"亿", grow, str(gap).rjust(2), info]
        return result
    return False, code, name
############################# 超跌吸筹反弹 ################################
def findTrence(code, name, testFlag, days, half, inn):
    if days['close'].iloc[-1] < days['ma_60' ].iloc[-1] :
        return False, code, name
    if days['close'].iloc[-1] < days['ma_144'].iloc[-1] :
        return False, code, name
    days['sf60']            = days['ma_60' ].shift(1)
    days['sf144']           = days['ma_144'].shift(1)
    days['cross']           = 0
    days['cross']           = days.apply(lambda x : 1 if ((x.sf60 < x.sf144) & (x.ma_60 >= x.ma_144)) else -1 if ((x.sf60 >= x.sf144) & (x.ma_60 < x.ma_144)) else 0, axis=1)
    cross                   = days['cross'].tolist()
    peak                    = np.nonzero(cross)[0]
    lens                    = len(peak)
    if lens == 0:
        return False, code, name
    
    df                      = pd.DataFrame(peak, columns=['idx'])
    df['cross']             = 0
    index                   = 0
    for i in peak:
        df['cross'].iloc[index]     = cross[i]
        index               += 1
    df['gap']               = df['idx'] - df['idx'].shift(1)
    df['flag']              = df.apply(lambda x : 1 if x.cross == 1 and x.gap >= 120 else 0, axis=1)
    df                      = df.where(df.gap > 10).dropna()
    if len(df) == 0:
        return False, code, name
    delta                   = int(df['gap'].iloc[-1])
    point                   = int(df['idx'].iloc[-1])
    cnt                     = int(df['flag'].tail(1).sum())
    amount                  = days['amount'].iloc[-1]
    change                  = days['change'].iloc[-1]
    gaps                    = int(len(days) - df['idx'].iloc[-1])
    grow                    = round(days['close'].iloc[-1] / days['close'].iloc[point-20:-1].min(), 2)
    if  cnt \
    and gaps   <= 5 \
    and change >= 2 \
    and grow   < 1.5\
    and True:
        sclose              = strClose(days['close'].iloc[-1])
        info                = "9.4.超跌_" + str(delta) + "天 " + str(change).rjust(5) + "%"
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", grow, gaps, info]
        return result
    return False, code, name


######################################################################
############################# 基础处理函数 ############################
######################################################################
def strClose(close):
    baseFuncEntry       = False
    return str(close).rjust(6)
def strMount(mount):
    return str(mount).rjust(6)+"亿"
def getGrowFromMin(data):
    grow                = round(data['close'].iloc[-1] / data['close'].iloc[-120:].min(), 2)
    return grow
def getGrowFromStage(data):
    index               = data['close'].iloc[-20:].idxmin()
    grow                = round(data['close'].iloc[-1] / data['close'].iloc[-20:].min(), 2)
    return grow, index
def getSealingNum(data):
    grow                = data['grow'].tolist()
    seal                = [1 if a >= 9.7 else 0 for a in grow]
    data['seal']        = pd.DataFrame(seal)
    data['flag']        = data['seal'].rolling(window=2).sum() == 2
    del data['seal']
############################# 从文件读数据 ###########################
def readDataFromFile(code, name, dpath, hpath):
    empty               = pd.DataFrame()
    if not os.path.exists(dpath) :
        return False, empty, empty
    ddata               = pd.read_csv(dpath, parse_dates=[0])
    ddata.sort_values('date', ascending=True, inplace=True)
    ddata               = procMaData  (ddata)
    #ddata               = procMacdData(ddata)
    if len(ddata) == 0:
        return False, empty, empty
    
    #if not os.path.exists(hpath) :
    #    return False, ddata, empty
    #hdata               = pd.read_csv(hpath, parse_dates=[0])
    #hdata.sort_values('date', ascending=True, inplace=True)
    #hdata               = procMaData  (hdata)
    return True, ddata, empty
############################# 寻找有效日线 ###########################
def getValidDays(code, name, testFlag, data):
    # 有大阳(4%); 有海龟; 有换手率
    df                  = data.tail(wins)
    cntg                = len(df[df.grow   >= 1.04])    #大阳(4%)
    cntt                = len(df[df.tur    >= 1])      #海归
    cntm                = len(df[df.amount >= 10]) \
                        + len(df[df.change >= 30]) \
                        + len(df[(df.amount>=6) & (df.change>= 5)]) \
                        + len(df[(df.amount>=4) & (df.change>=10)]) \
                        + len(df[(df.amount>=2) & (df.change>=20)])
    total               = cntg + cntt + cntm
    return True if total > 0 else False
############################# 寻找放量天数 ###########################
def getMountDays(code, name, testFlag, data, wins):
    df                  = data.tail(wins)
    if testFlag:
        #print(df)
        print("getMountDays:", code, name, wins, len(df[(df.amount>=10)]))
        print("getMountDays:", code, name, wins, len(df[(df.change>=30)]))
        print("getMountDays:", code, name, wins, len(df[(df.amount>=6) & (df.change>= 5)]))
        print("getMountDays:", code, name, wins, len(df[(df.amount>=4) & (df.change>=10)]))
        print("getMountDays:", code, name, wins, len(df[(df.amount>=2) & (df.change>=20)]))
    cntm                = len(df[(df.amount>=10)]) \
                        + len(df[(df.change>=30)]) \
                        + len(df[(df.amount>=6) & (df.change>= 5)]) \
                        + len(df[(df.amount>=4) & (df.change>=10)]) \
                        + len(df[(df.amount>=2) & (df.change>=20)])
    return cntm
############################# 寻找有效half ###########################
def chkLastDays (code, name, testFlag, amount, change):
    cntm                =   amount >= 10 \
                        or  change >= 30 \
                        or (amount >= 6 and change >=  5) \
                        or (amount >= 4 and change >= 10) \
                        or (amount >= 2 and change >= 20)
    return cntm
############################# 海龟/均线/MACD/BOLL ####################
def procTurtleData(df, period) :
    df['tur']               = df['close'] == df['close'].rolling(window=period).max()
    return df
def procMaData(df) :
    for ma in maList:
        col                 = 'ma_' + str(ma)
        df[col]             = df['close'].rolling(window=ma).mean().round(decimals=2)
    return df
def procBollData(df) :
    for ma in bollList:
        df['md_'+str(ma)]   = df['close'].rolling(window=ma).mean().round(decimals=2)
        df['std_'+str(ma)]  = df['close'].rolling(window=ma).std(ddof=0).round(decimals=2)
        df['up_'+str(ma)]   = df['md_'+str(ma)] + 2 * df['std_'+str(ma)]
        df['dw_'+str(ma)]   = df['md_'+str(ma)] - 2 * df['std_'+str(ma)]
    return df
def procMacdData(df) :                         
    df['sema']              = pd.DataFrame.ewm(df['close'],span=macdFast).mean().round(decimals=6) 
    df['lema']              = pd.DataFrame.ewm(df['close'],span=macdSlow).mean().round(decimals=6)   
    df['dif']               = df['sema'].round(decimals=2) - df['lema'].round(decimals=6)       
    df['dea']               = pd.DataFrame.ewm(df['dif'],span=macdMidd).mean().round(decimals=2)
    df['macd']              = 2*(df['dif'].round(decimals=2) - df['dea'].round(decimals=2))
    #df['dmin']              = df['dif'] == df['dif'].rolling(window=5,center=True).min()
    #df['dmin']              = df['dif'] == df['dif'].rolling(window=7).min()
    del df['sema'], df['lema']
    return df
def procMaCross(ma1, ma2, data, close, testFlag) :
    #data['minus']           = round((data['ma_'+str(ma1)] - data['ma_'+str(ma2)]), 4)
    data['minus']           = round((data['close'] - data['ma_'+str(ma2)]), 4)
    data['plus']            = round((data['minus'].shift(1) * data['minus']), 4)
    minus                   = data['minus'].tolist()  
    plus                    = data['plus'].tolist()    
    cross                   = list(map(lambda x, y: 0 if x > 0 else -1 if y < 0 else 1, plus, minus))
    lens                    = len(cross)
    eidx                    = lens - 60 if lens > 60 else 0
    for i in range(lens-1, eidx, -1):
        if cross[i] == 1 and cross[i] == cross[i-1]:
            cross[i]        = 0
    data['cross']           = pd.DataFrame(cross)
    del data['minus'], data['plus']
    if testFlag:
        print(cross)
    
    hdiff                   = np.diff(np.sign(np.diff(cross)))
    peak                    = (np.where(hdiff == -2)[0] + 1).tolist()   #上穿
    poke                    = (np.where(hdiff ==  2)[0] + 1).tolist()   #下穿
    rsut                    = [[x, 1, close[x]] for x in peak] + [[x, -1, close[x]] for x in poke]
    rsut.sort(reverse=False)
    return rsut
############################# 寻找波峰波谷 ############################
def procPeakList(close, gap) :
    lens            = len(close)   
    hdiff           = np.diff(np.sign(np.diff(close)))
    ldiff           = np.diff(np.sign(np.diff(close)))
    hpeak           = (np.where(hdiff == -2)[0] + 1).tolist()
    lpoke           = (np.where(ldiff ==  2)[0] + 1).tolist() 
    top = []; bot = []; tol = []; rst = [];
    for x in hpeak:
        top.append([x, 1, close[x]])
    for x in lpoke:
        bot.append([x, 0, close[x]])
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
    return rst
#####################################################################
############################# 基础函数结束 ###########################
#####################################################################

         







#####################################################################
############################# 无效函数 ###############################
#####################################################################
############################# 封单额/成交额 > 2 ########################
def findSealing(code, name, testFlag, days, half, inn):
    # 内盘：主动卖； 外盘：主动买，涨停时外盘0为封一字; 外盘越少，买方越强势
    if inn['封单额'].iloc[-1] != "--  " and inn['涨幅%'].iloc[-1] > "0":
        amount              = round(float(inn['总金额'].iloc[-1]) / 10000, 2)
        sealing             = round(float(inn['封单额'].iloc[-1]) / 10000, 2)
        rate                = round(sealing / amount, 2)
        grab                = 1 if rate > 0.8  else 0  # 封单 > 成交量，抢筹
        amplitude           = 1 if float(inn['振幅%'].iloc[-1]) == 0 else 0  # 振幅为0，一字板
        change              = days['change'].iloc[-1]
        sclose              = strClose(days['close'].iloc[-1])
        if grab or amplitude:
            info            = "9.9.大单封停:" if grab else "9.9.一字涨停:" if amplitude else ""
            info            += str(change).rjust(5) + "%"
            result          = [True, code, name.rjust(4), sclose, str(amount)+"亿", str(sealing)+"亿", str(rate).rjust(2), info]
            return result
    return False, code, name
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
############################# 寻找海归 ################################
def findTurtle(code, name, testFlag, days, half, inn):
    procTurtleData(days, 60)
    turcnt                  = days['tur'].tail(1).sum()
    if turcnt == 0 :
        return False, code, name
    
    if days['grow'].iloc[-1] >= 5 :
        amount              = days['amount'].iloc[-1]                
        change              = days['change'].iloc[-1]
        minval              = days['close' ].iloc[-20:].min()
        minidx              = days['close' ].iloc[-20:].idxmin()
        grow                = round(days['close'].iloc[-1] / minval, 2)
        gaps                = len(days) - minidx - 1
        if amount < 3 or change < 5 or grow > 1.35:
            return False, code, name
        sclose              = strClose(days['close'].iloc[-1])
        info                = "9.3.海龟：" + str(change).rjust(5) + "%"
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", grow, gaps, info]
        return result
    return False, code, name
############################# 连续涨停后调整 ############################
def findContinue(code, name, testFlag, days, half, inn):
    getSealingNum(days)
    flag                    = days['flag'].tolist()
    haspeak                 = np.nonzero(flag)[0]
    if len(haspeak) == 0:
        return False, code, name
    
    idx                     = haspeak[-1]
    gaps                    = len(days) - idx
    close                   = days['close'].tolist()
    peak                    = close[idx]
    amount                  = days['amount'].iloc[-1]                
    change                  = days['change'].iloc[-1]
    mval                    = days['close'].iloc[idx:].min()
    grow                    = round(close[-1] / min(close[-90:]), 2)
    sclose                  = strClose(close[-1])
    
    procTurtleData(days, 30)
    turcnt              = days['tur'].tail(3).sum()
    if  mval >= peak and close[-1] == max(close[idx:]) \
    and grow <= 1.62\
    and gaps <= 15  \
    and gaps >= 3   \
    and turcnt >= 1 \
    and True :
        info                = "9.7.连涨不调整:" + str(change).rjust(5) + "%"
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", grow, gaps, info]
        return result
    
    if  mval <  peak and close[-1] >= (peak * 0.99) \
    and close[-1] >= days['ma_10'].iloc[-1] \
    and close[-1] >= days['ma_5' ].iloc[-1] \
    and close[-1] >= mval \
    and grow <  1.62\
    and gaps <= 15  \
    and gaps >= 3   \
    and True :
        info                = "9.7.连涨调整:" + str(change).rjust(5) + "%"
        result              = [True, code, name.rjust(4), sclose, str(amount)+"亿", grow, gaps, info]
        return result
    return False, code, name
#####################################################################