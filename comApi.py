# -*- coding: utf-8 -*-
# -*- coding:gb2312 -*-

"""
Created on Mon Oct 10 22:59:41 2016

@author: Administrator
"""

import os, sys#, re #, datetime, codecs, chardet
import pandas  as pd
import numpy   as np

maList                  = [5, 10, 20, 60, 72] 
maList                  = [5, 10, 20, 72]    
bollList                = [20]
turtleDays              = 20
amountUnit              = 100000000  #1亿

wins                    = 10
isMacdMode              = 0     #0:short; 1:long
macdFast                = 24 if isMacdMode == 1 else 12
macdSlow                = 52 if isMacdMode == 1 else 26
macdMidd                = 18 if isMacdMode == 1 else 9

daysEnab                = 15    #1111 1111
daysEnab                =  7    #1111 1111

maxch                   = 0

######################################################################
############################# 各种算法处理买点 ########################
######################################################################
def getStockBuy(code, name, baseInfo, tp, testFlag, hfile, dfile, wfile):
    global maxch
    funcEntryPoint  = False
    findFlag        = False
    maxch           = len(name) if len(name) > maxch else maxch
    name            = name.rjust(maxch)
    flag, data      = readDataFromFile(code, name, dfile)
    if not flag :
        return findFlag, code, name

    if tp == 'days' :        
        # daysEnab bits0.寻找龙头股(三板定龙头,找连续2连板)
        if daysEnab & 1 :
            result          = getLimitGrow(code[2:], name, testFlag, data)
            if result[0]:
                return result
        # daysEnab bits1.寻找中继(两个海归高点间的低点dif>0; 两高间的低点不破5/20线)
        if daysEnab & 2 :
            result          = getContiGrow(code[2:], name, testFlag, data, tp)
            if result[0]:
                return result
        # daysEnab bits2.海归筛选
        if daysEnab & 4 :
            result          = getTurtleGrow(code[2:], name, testFlag, data)
            if result[0]:
                return result
        # daysEnab bits3.低点dif突破0轴,比前一个低点的dif高
        if daysEnab & 8 :
            result          = getMacdDifGrow(code[2:], name, testFlag, data)
            if result[0]:
                return result

        return findFlag, code, name
    
    elif tp == 'half' :
        # 筛出日线不满足
        flag, data  = readDataFromFile(code, name, dfile)
        if not flag :
            return findFlag, code, name
        if data['close'].iloc[-1] < data['ma_20'].iloc[-1]:
            return findFlag, code, name
        # 找出最近放量
        #cntm        = getDaysMount(code, name, testFlag, data, wins)
        #if not cntm :
        #    return findFlag, code, name    
        # 筛出half不满足
        flag, data  = readDataFromFile(code, name, hfile)
        if not flag :
            return findFlag, code, name
        
        # Step2.寻找中继(两个海归高点间的低点dif>0; 两高间的低点不破5/20线)
        result      = getContiGrow(code[2:], name, testFlag, data, tp)
        if result[0]:
            return result
        return findFlag, code, name
    
    elif tp == 'week' :
        return findFlag, code, name
    
    return findFlag, code, name
    sys.exit(0)

############################# B0:日线连板 ############################
def getLimitGrow(code, name, testFlag, data):
    cnt2                = (data['grow'].tail(2) >= 9.7).astype(int).sum()  
    cnt5                = (data['grow'].tail(5) >= 9.7).astype(int).sum()
    if cnt2 == 2 and cnt5 == 2:
        lens            = len(data)
        mgrow           = getGrowFromMin  (data)
        #if mgrow > 2:
        #    return False, code, name
        close           = strClose(data['close'].iloc[-1])
        mount           = strMount(data['amount'].iloc[-1])
        info            = "9.连板"
        if lens < 30 :
            allCnt      = len(data.grow >= 9.7)
            if allCnt == lens:
                info    += "新"+str(lens)
        result          = [True, code, name, close, mount, mgrow, cnt2, info]
        return result
    return False, code, name

############################# B1:调整新高 ############################
def getContiGrow(code, name, testFlag, data, tp):
    findFlag            = False
    info                = "8.中继："
    # 剔除涨幅过大
    mgrow               = getGrowFromMin  (data)
    sgrow, idx          = getGrowFromStage(data)
    if mgrow > 2 or sgrow > 1.62:
        return False, code, name
    
    # 找出两次海龟
    result              = getTurlProcs(code, name, testFlag, data)
    if not result[0]:
        return findFlag, code, name
    p1                  = result[1]
    p0                  = result[2]
    days                = result[3]
    gap                 = result[4]
    close               = strClose(data['close' ].iloc[p0])
    if data['close'].iloc[-1] < data['close'].iloc[p0]:
        return findFlag, code, name
    
    amount              = 0
    change              = 0
    if tp == 'days' :
        amount          = data['amount'].iloc[-1]
        change          = data['change'].iloc[-1]
        flag1           = getValidDays(code, name, testFlag, data)
        # 日线有成交量
        if not flag1 :
            return False, code, name
    elif tp == 'half':
        amount          = round(data['amount'].tail(8).sum(),1)
        change          = round(data['change'].tail(8).sum(),1)
    mount               = strMount(amount)
    # 最后一天有成交量
    flag2               = chkLastDays(code, name, testFlag, amount, change)
    if not flag2:
        return False, code, name
    
    # 调整：不破5/20日线；不破/跌破又站上0轴          
    if  data['close'].iloc[-1] >= data['close'].iloc[p0] \
    and gap <= 5 :
        cntma5          = (data['close'].iloc[p1:p0] >= data['ma_5' ].iloc[p1:p0]).astype(int).sum()
        if cntma5 == days:
            findFlag    = True
            info        += "不破 5日_" + str(gap) + "天_" + str(change).rjust(5) + "%"
            result      = [findFlag, code, name.rjust(4), close, mount, sgrow, str(days).rjust(2), info]
            return result
        
        cntma20         = (data['close'].iloc[p1:p0] >= data['ma_20'].iloc[p1:p0]).astype(int).sum()
        if cntma20 == days:
            findFlag    = True
            info        += "不破20日_" + str(gap) + "天_" + str(change).rjust(5) + "%"
            result      = [findFlag, code, name.rjust(4), close, mount, sgrow, str(days).rjust(2), info]
            return result
        
        cntdiff         = (data['dif'].iloc[p1:p0] >= -0.02).astype(int).sum()
        if cntdiff == days:
            findFlag    = True
            info        += "不破 0轴_" + str(gap) + "天_" + str(change).rjust(5) + "%"
            result      = [findFlag, code, name.rjust(4), close, mount, sgrow, str(days).rjust(2), info]
            return result
        else:
            findFlag    = True
            info        += "跌破 0轴_" + str(gap) + "天_" + str(change).rjust(5) + "%"
            result      = [findFlag, code, name.rjust(4), close, mount, sgrow, str(days).rjust(2), info]
            return result
    return findFlag, code, name
############################# B1:寻找两高 ############################
def getTurlProcs(code, name, testFlag, data):
    # 两高间的低点>5日/20日; dif>0轴
    lens                = len(data)
    tur                 = data['tur'].tolist()
    hastur              = np.nonzero(tur)[0]        # array[0]返回tur非0的索引值
    if len(hastur) < 2 :
        #print(code, name, str(len(hastur)) + "个海龟", lens)
        return [False, code, name]

    # 把海龟list右移相减，找出调整大于5天的时间段
    find                = False
    dltdf               = pd.DataFrame(hastur)
    dltdf.columns       = ['tur']
    dltdf['tur']        = dltdf['tur'] - dltdf['tur'].shift(1)
    dlttur              = dltdf['tur'].tolist()
    for i in range(len(dlttur)-1, 0, -1):
        if dlttur[i] >= 5 :
            p0          = hastur[i]
            p1          = hastur[i-1]
            find        = True
            days        = p0 - p1
            gap         = lens - 1 - p0
            break
    if not find:
        #print(code, name, "没有找到", lens)
        return [False, code, name]
    else:
        dif             = data['dif'].tolist()
        if dif[p0] < dif[p1] \
        or dif[-1] < dif[p1] \
        or data['close'].iloc[-1] < data['close'].iloc[p0]:
            return [False, code, name]
        
        if testFlag :
            print(hastur)
            print(dlttur)
            print(code, name, p0, p1, days, gap, data['close' ].iloc[p1], data['close' ].iloc[p0])
        return [True, p1, p0, days, gap]   

############################# B2:海归筛选 ############################
def getTurtleGrow(code, name, testFlag, data):
    findFlag            = False    
    info                = "7.tur"
    # 获取海归
    data['tur']         = data['close'] == data['close'].rolling(window=180).max()
    data['peak']        = data['close'] == data['close'].rolling(window=20,center=True).max()
    tur                 = data['tur'].tolist()
    turcnt              = (data['tur'].tail(3) == True).astype(int).sum()
    if turcnt == 0:
        return findFlag, code, name 
    
    # 获取最近低点
    lens                = len(data)
    peak                = data['peak' ].tolist()
    close               = data['close'].tolist()        
    haspeak             = np.nonzero(peak)[0]          # array[0]返回非0的索引值
    maxidx              = 0
    minidx              = 0
    if len(haspeak) == 0:
        maxidx          = data['close'].tail(20).idxmax()
        minidx          = data['close'].tail(20).idxmin()
        if maxidx > minidx :
            print(code, name, "has no higher", maxidx, close[maxidx], minidx, close[minidx])
            sys.exit(0)
    else:
        maxidx          = haspeak[-1]
        minidx          = data['close'].iloc[maxidx:].idxmin()
    minval              = close[minidx]
    days                = lens - minidx

    # 获取有效海归
    grow                = round(close[-1] / minval, 2)
    change              = data['change'].iloc[-1]
    amount              = data['amount'].iloc[-1]
    dif                 = data['dif'].tolist()
    mount               = strMount(amount)
    sclose              = strClose(close[-1])
    cntm                = getMountDays(code, name, testFlag, data, days)
    if testFlag:
        print(code, name, maxidx, close[maxidx], lens - maxidx)
        print(code, name, minidx, close[minidx], days)
        print(code, name, cntm)
        
    if  cntm        > 0 \
    and grow        < 1.5 \
    and days        <= 13 \
    and close[-1]   > close[maxidx] \
    and dif[minidx] >= -0.05 \
    and dif[-1]     >= -0.05 \
    and True :
        findFlag        = True
        info            += "海归：" + "change_" + str(change).rjust(5) + "%"
        result          = [findFlag, code, name.rjust(4), sclose, mount, grow, str(days).rjust(2), info]
        return result
    return findFlag, code, name

############################# B2:dif递增 ############################
def getMacdDifGrow(code, name, testFlag, data):
    findFlag            = False    
    info                = "8.dif"
    lens                = len(data)
    dif                 = data['dif' ].round(decimals=2).tolist()
    # 找出dif低点
    dmin                = data['dmin'].tolist()
    close               = data['close'].tolist()
    change              = data['change'].iloc[-1]
    amount              = data['amount'].iloc[-1]
    mount               = strMount(amount)
    sclose              = strClose(close[-1])
    hasdmin             = np.nonzero(dmin)[0]        # array[0]返回tur非0的索引值
    if testFlag:
        print(code, name, dif)
        print(code, name, dmin)
        print(code, name, hasdmin)
        
    if len(hasdmin) == 0 :
        if dif[-1] > 0 : 
            days        = lens
            grow        = round(close[-1] / close[0], 2)
            info        += "新股：" + str(dif[-1]) + str(change).rjust(5) + "%"
            result      = [True, code, name.rjust(4), sclose, mount, grow, str(days).rjust(2), info]
            return result
            print("%s %4s\t %2d天 No dif poke dif = %0.2f" %(code, name, lens, dif[-1]))
        return False, code, name
    
    # 连续多个dif低点只保留一个
    for i in range(len(hasdmin)):
        idx             = hasdmin[i]
        lft             = idx - 5 if idx >= 5 else 0
        rgt             = idx + 5 if idx + 5 < lens else lens - 1
        #print(idx, lft, rgt)
        #print(idx, dif[idx], min(dif[lft:rgt+1]), dif[idx] != min(dif[lft:rgt+1]), dif[lft:rgt+1])
        #sys.exit(0)
        if dif[idx] != min(dif[lft:rgt+1]):
            dmin[idx]   = False
    hasdmin             = np.nonzero(dmin)[0]
    #print(code, name, hasdmin)

    idx0                = hasdmin[-1]
    idx1                = hasdmin[-2] if len(hasdmin) >= 2 else idx0
    dif0                = round(dif[idx0], 2)
    dif1                = round(dif[idx1], 2)
    #print(code, name, len(data), idx1, idx0)
    grow                = round(close[-1] / close[idx0], 2)
    days                = lens - hasdmin[-1]
    
    if  abs(dif0) < 0.05 and dif[-1] > dif0 \
    and True :
        findFlag        = True
        info            = "3.dif"
        info            += "连增：" + str(dif0) + "/" + str(dif[-1]) + str(change).rjust(5) + "%"
        result          = [findFlag, code, name.rjust(4), sclose, mount, grow, str(days).rjust(2), info]
        return result
    return findFlag, code, name

############################# 寻找高换手 ###########################    
def getHighCHange(code, name, testFlag, data):
    findFlag            = False
    # 剔除涨幅过大
    mgrow               = getGrowFromMin  (data)
    sgrow, idx          = getGrowFromStage(data)
    if mgrow > 2 or sgrow > 1.62:
        return False, code, name
    
    # 找出放量
    mount               = strMount(data['amount'].iloc[-1])
    close               = strMount(data['close' ].iloc[-1])
    cntc                = (data['change'].iloc[-wins:] >= 30).astype(int).sum()
    change              = data['change'].iloc[-1]
    grow                = data['grow'  ].iloc[-1]
    gap                 = len(data) - data['close'].tail(20).idxmin()
    days                = gap
    if  cntc:
        if  True:
            findFlag    = True
            info        = "巨量：" + str(change).rjust(5) + "%"
            result      = [findFlag, code, name.rjust(4), close, mount, sgrow, str(days).rjust(2), info]
            return result
        
    return findFlag, code, name

######################################################################
############################# 基础处理函数 ############################
######################################################################
def strClose(close):
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
############################# 从文件读数据 ###########################
def readDataFromFile(code, name, path):
    if not os.path.exists(path) :
        return False, pd.DataFrame()
    data                = pd.read_csv(path, parse_dates=[0])
    data.sort_values('date', ascending=True, inplace=True)
    data                = procMaData  (data)
    data                = procMacdData(data)
    procTurtleData(data, turtleDays)
    if data['close'].iloc[-1] < data['ma_20'].iloc[-1] :
        return False,data 
    else:
        return True, data
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
        print(code, name, wins, len(df[(df.amount>=10)]))
        print(code, name, wins, len(df[(df.change>=30)]))
        print(code, name, wins, len(df[(df.amount>=6) & (df.change>= 5)]))
        print(code, name, wins, len(df[(df.amount>=4) & (df.change>=10)]))
        print(code, name, wins, len(df[(df.amount>=2) & (df.change>=20)]))
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

         
