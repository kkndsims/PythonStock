# -*- coding: utf-8 -*-
# -*- coding:gb2312 -*-

"""
Created on Mon Oct 10 22:59:41 2016

@author: Administrator
"""

import os
import datetime
import sys
import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings("ignore")

minipath    = "C:\\new_tdx\\T0001\\export\\m\\"  # 5分钟原始数据
maList      = [5, 10, 20]
isMacdMode  = 0  # 0:short; 1:long
macdFast    = 24 if isMacdMode == 1 else 12
macdSlow    = 52 if isMacdMode == 1 else 26
macdMidd    = 18 if isMacdMode == 1 else 9

dayAmount   = 5
dayChange   = 3
growLevel   = 1.7
maxch       = 0
empty       = pd.DataFrame()
percnet     = 10
mlevel      = 0
clevel      = 0
glevel      = 0
longTerm    = ['week', 'month']
#longTerm    = ['week']

yearWins    = 250

######################################################################
############################# 板块算法处理买点 ########################
######################################################################
def findPlateBuy(code, name, baseInfo, testFlag, days):
    week    = pd.DataFrame()
    flag    = washPlateData(code, name, days)
    if flag == False : return flag, code, name
    if True:
        # 板块有涨停且周线破新高
        value   = baseInfo.loc[baseInfo['名称'] == name]
        if int(value['涨停数'].iloc[-1]) < 2: return False, code, name
        week    = getMergData("week", days)
        procTurtleData(week, maList[-1])
        if week.tur.iloc[-1] == 0 : return False, code, name

        result  = findPlateDaysMacd(code[2:], name, testFlag, days, week, "9PD.")
        if result : return result
        result  = findPlateWeekMacd(code[2:], name, testFlag, days, week, "8PW.")
        if result : return result
    return False, code, name
######################################################################
############################# 个股算法处理买点 ########################
######################################################################
def getStockBuy(code, name, endDate, baseInfo, testFlag, hfile, dfile, info):
    global maxch
    findEnab        = 1
    maxch           = len(name) if len(name) > maxch else maxch
    name            = name.rjust(maxch)
    days            = readDataFromFile(code, name, dfile)
    # 日线站上5/10/20线
    flag            = washDaysData(code, name, days, info)
    if flag == False : return flag, code, name

    if findEnab and 1 : 
        result      = findRighSideGrow0(code[2:], name, testFlag, days, "8R.")
        if result: return result    # 日线右侧交易
        result      = findLeftSideGrow0(code[2:], name, testFlag, days, "7L.")
        if result : return result   # 日线左侧交易
        
    if findEnab and 0 : # 额外选择
        result      = findRighSideGrow2(code[2:], name, testFlag, days, "5.")
        if result: return result   # week/month右侧交易
        result      = findRighSideGrow1(code[2:], name, testFlag, days, "6H.")
        if result: return result   # half右侧交易
        
    return False, code, name
    sys.exit(0)
############################# 日/周/月线清洗数据 ######################
def washDaysData(code, name, days, info):
    if len(days) < maList[-1]: return False
    for ma in maList:
        days['cls'+str(ma)] = days.close.rolling(ma).mean().round(decimals=2)
    if days.close.iloc[-1] < days['cls5'].iloc[-1] : return False
    if days.close.iloc[-1] < days['cls10'].iloc[-1]: return False
    if days.close.iloc[-1] < days['cls20'].iloc[-1]: return False
    for ma in maList: del days['cls'+str(ma)]

    procTurtleData(days, maList[-1])
    procMacdData(days)
    days['seal']    = [1 if a >= 9.7 else 0 for a in days.grow]
    del days['open'], days['low'], days['volume']
    
    tolAmt0     = float(info['流通市值'].iloc[-1].replace("亿","").replace(" ",""))
    if float(tolAmt0) <=  10 : return False
    tolAmt1     = float(info['流通市值Z'].iloc[-1].replace("亿","").replace(" ",""))
    if float(tolAmt1) >= 500 : return False
    
    if days.high.iloc[-1] / days.close.iloc[-1] > 1.1 : return False
    if days.dif.iloc[-1] < 0 : return ""

    # 成交活跃有量或大阳
    vtype       = 0
    flag1       = days.seal.tail(10).sum()
    flag2       = days.amount.iloc[-1] >= dayAmount and days.change.iloc[-1] >= dayChange
    flag21      = days.amount.tail(10).where(days.amount >= dayAmount).dropna().sum()
    flag3       = days.grow.tail(10).where(days.grow >= 5).dropna().sum()
    flag4       = days.tur.tail(10).sum()
    if vtype:
        if not (flag1 or flag2 or flag3): return False
    else:
        if not (flag1 or flag2 or flag21 or flag3 or flag4): return False
    return True
def washWeekData(code, name, days):
    week        = getMergData("week", days)
    procMacdData(week)
    for ma in maList:
        week['cls'+str(ma)] = week.close.rolling(ma).mean().round(decimals=2)
    #if week.close.iloc[-1] < week['cls5'].iloc[-1]: return False, week
    if week.close.iloc[-1] < week['cls10'].iloc[-1]: return False, week
    if week.close.iloc[-1] < week['cls20'].iloc[-1]: return False, week
    for ma in maList: del week['cls'+str(ma)]
    return True, week
############################# 合并月/周线数据 #########################
def getMergData(otype, data):
    period      = "W" if otype == "week" else "M"
    data['date']= pd.to_datetime(data['date'])  # 把字符串转换成时间信息
    data        = data.set_index('date')
    df2         = data.resample(period, closed="right", label="right").last()
    df2         = df2[~df2['amount'].isin([0])]
    df2         = df2.dropna()
    if 'seal' in df2.columns: del df2['seal']
    if 'tur' in df2.columns : del df2['tur']
    if 'dif' in df2.columns : del df2['dif']
    if 'dea' in df2.columns : del df2['dea']
    if 'delta' in df2.columns:del df2['delta']
    if 'cross' in df2.columns:del df2['cross']
    if 'oepn' in df2.columns:
        df2['open'] = data['open'].resample  (period,closed="right",label="right").first().round(decimals=2)
        del df2['open']
    if 'high' in df2.columns:
        df2['high'] = data['high'].resample(period, closed="right", label="right").max().round(decimals=2)
    if 'low' in df2.columns:
        #df2['low']  = data['low'].resample   (period,closed="right",label="right").min().round(decimals=2)
        del df2['low']
    df2['close']    = data['close'].resample(period, closed="right", label="right").last().round(decimals=2)
    if 'volume' in df2.columns:
        #df2['volume']   = data['volume'].resample(period,closed="right",label="right").sum().round(decimals=1)
        del df2['volume']
    if 'amount' in df2.columns:
        df2['amount'] = data['amount'].resample(period, closed="right", label="right").sum().round(decimals=1)
    if 'change' in df2.columns:
        df2['change'] = data['change'].resample(period, closed="right", label="right").sum().round(decimals=2)
    if 'chgTol' in df2.columns:
        df2['chgTol'] = data['chgTol'].resample(period, closed="right", label="right").sum().round(decimals=2)
    df2['grow'] = ((df2['close'] / df2['close'].shift(1) - 1) * 100).round(decimals=2)
    df2.reset_index(inplace=True)
    procTurtleData(df2, maList[-1])
    return df2
def getMergMinute(otype, data):
    period      = otype
    data['date']= pd.to_datetime(data['date'])  # 把字符串转换成时间信息
    data        = data.set_index('date')
    df2         = data.resample(period, closed="right", label="right").last()
    df2         = df2[~df2['amount'].isin([0])]
    df2         = df2.dropna()
    if 'high' in df2.columns:
        df2['high'] = data['high'].resample(period, closed="right", label="right").max().round(decimals=2)
    if 'low' in df2.columns:
        df2['low'] = data['low'].resample(period, closed="right", label="right").min().round(decimals=2)
    df2['close'] = data['close'].resample(period, closed="right", label="right").last().round(decimals=2)
    if 'volume' in df2.columns:
        df2['volume'] = data['volume'].resample(period, closed="right", label="right").sum().round(decimals=1)
    if 'amount' in df2.columns:
        del df2['amount']
    df2.reset_index(inplace=True)
    return df2
############################# macd区间 ###############################
def findDaysPoke(code, name, testFlag, df, wins):
    halfwins    = wins//2 - 1
    end         = df.index[-1]
    tail        = end - halfwins
    hdiff       = np.diff(np.sign(np.diff(df.close)))
    hpoke       = (np.where(hdiff == 2)[0] + 1).tolist()
    df['bot']   = 0
    for i in hpoke : df.bot.iloc[i] = -1
    df['mcls']  = df.close.rolling(wins, center=True).min()
    for i in range(tail, end+1):
        df['mcls'].iloc[i] = df.close.iloc[i-halfwins:].min()
    df['bot']   = [-1 if a == -1 and b == c else 0 for a,b,c in zip(df.bot, df.close, df.mcls)]
    del df['mcls']
def findDeltaKeep(code, name, testFlag, df, wins):
    halfwins    = wins//2 - 1
    end         = df.index[-1]
    tail        = end - halfwins
    hdiff       = np.diff(np.sign(np.diff(df.delta)))
    hpeak       = (np.where(hdiff == -2)[0] + 1).tolist()
    #print(code, name, hpeak)
    #print(df.delta.tail(60))
    df['top']   = 0
    for i in hpeak : df.top.iloc[i] = -1
    df['mdlt']  = df.delta.rolling(wins, center=True).max()
    for i in range(tail, end+1):
        df['mdlt'].iloc[i] = df.delta.iloc[i-halfwins:].min()
    df['top']   = [1 if a == 1 and b == c else 0 for a,b,c in zip(df.top, df.delta, df.mdlt)]
    del df['mdlt'] 
def findMinClose(code, name, testFlag, df, wins):
    clsMaxIdx   = df.close.tail(wins).idxmax()
    clsMinIdx   = df.close.tail(wins).idxmin()
    difMinIdx   = df.dif.tail(wins).idxmin()
    P0          = max(clsMinIdx, difMinIdx)
    gaps        = df.index[-1] - clsMaxIdx
    if gaps == 0 : return P0, clsMaxIdx
    clsMinIdx   = df.close.tail(gaps).idxmin()
    difMinIdx   = df.dif.tail(gaps).idxmin()
    P1          = max(clsMinIdx, difMinIdx)
    return P0, P1
def findMinDif  (code, name, testFlag, df, wins):
    clsMaxIdx   = df.close.tail(wins).idxmax()
    difMinIdx   = df.dif.tail(wins).idxmin()
    P0          = difMinIdx
    gaps        = df.index[-1] - clsMaxIdx
    if gaps == 0 : return True, P0
    difMinIdx   = df.dif.tail(gaps).idxmin()
    P1          = difMinIdx
    if P0 == P1 : return True, P1
    return False, P0
def washAmount(code, name, testFlag, df):
    amount      = df['amount'].iloc[-1]
    change      = df['change'].iloc[-1]
    if amount < 2 and df.seal.iloc[-1] == 0 : return False
    if change < dayChange and df.grow.iloc[-1] < topGrow : return False
    flag0       = df.seal.iloc[-1]
    flag1       = df.close.tail(20).max() == df.close.iloc[-1]
    flag2       = amount >= dayAmount and change >= dayChange
    if testFlag : print(sys._getframe().f_lineno, code, name, flag0, flag1, flag2)
    if flag0 or flag1 or flag2 : return True
    return False
def washHalfData(code, name, testFlag, df):
    hf          = getMinuteData(code, name, '30min')
    procMacdData(hf)
    adj         = list(hf.cross.where(hf.cross == -1).dropna().index)
    if len(adj) == 0 : return False
    peak        = [hf.close.iloc[i] for i in adj]
    top         = max(peak[-7:])
    P0          = adj[peak.index(top)]
    if hf.index[-1] - P0 < 5 : return False
    
    hmin        = hf.dif.iloc[P0:].min()
    #print(code, name, adj, peak, top)
    #print(P0, hmin, top, hf.index[-1])
    flag0       = 1 if hf.close.iloc[-1] >= hf.close.iloc[P0-1] else 0
    flag1       = 1 if (hmin >= -0.03 \
                  and hf.dif.iloc[-1] != hmin \
                  and hf.dif.iloc[-1] > hf.dif.iloc[-2] \
                  and hf.dif.iloc[-2] > hf.dif.iloc[-3]) else 0
    if testFlag : print(sys._getframe().f_lineno, code, name, flag0, flag1)
    if flag0 or flag1 : return True
    return False
def washLongGrow(code, name, testFlag, df, isweek):
    wk          = getMergData("week", df) if isweek else getMergData("M", df)
    wk['cls5']  = wk.close.rolling(5).mean().round(decimals=2)
    if wk.close.iloc[-1] < wk['cls5'].iloc[-1]: return False, 0, 0
    procMacdData(wk)
    if wk.dif.iloc[-1] < 0 :return False, 0, 0
    if wk.dif.iloc[-1] < wk.dea.iloc[-1] : return False, 0, 0
    
    # 短期调整，dif >= dea, dif > 0
    P0          = wk.delta.tail(36).idxmax()
    cmin        = wk.delta.iloc[P0:].min()
    cminp       = wk.delta.iloc[P0:].idxmin()
    gaps        = wk.index[-1] - cminp
    flag0       = cmin > 0
    flag1       = wk.delta.iloc[-1] > cmin
    flag2       = wk.close.iloc[-1] > wk.close.iloc[cminp]
    flag3       = wk.close.iloc[-1] >= wk.close.iloc[P0-2:P0+1].max()
    if testFlag : print(code, name, P0, wk.index[-1], cmin, flag0, flag1, flag2, flag3)
    if wk.dif.iloc[P0] < 0 : return False, 0, 0
    if flag0 and flag1 and flag2 and flag3 : return True, gaps, "9"
    
    # 中期调整，dif < dea, dif > 0
    adj         = list(wk.cross.where(wk.cross == -1).dropna().index)
    if len(adj) == 0 : return False, 0, 0
    P0          = adj[-1]
    dmin        = wk.dif.iloc[P0:].min()
    dminp       = wk.dif.iloc[P0:].idxmin()
    gaps        = wk.index[-1] - dminp
    flag0       = dmin > 0
    flag1       = wk.dif.iloc[-1] >= wk.dea.iloc[-1]
    flag2       = wk.dif.iloc[-1] > dmin
    flag3       = wk.close.iloc[-1] > wk.close.iloc[P0]
    flag3 = True
    if testFlag : print(code, name, P0, wk.index[-1], dmin, flag0, flag1, flag2, flag3)
    if flag0 and flag1 and flag2 and flag3 : return True, gaps, "8"

    # 长期调整，dif长期小于0轴，突破0轴
    sta         = list(wk.cross.where(wk.cross == 1).dropna().index)
    if len(sta) == 0 : return False, 0, 0
    gaps        = wk.index[-1] - sta[-1]
    cnt         = wk.delta.iloc[sta[-1]:].where(wk.delta >= 0).dropna().count()
    rate        = round(cnt/gaps, 2)
    if testFlag : print(code, name, sta[-1], wk.index[-1], gaps, cnt, rate)
    if cnt >= 20 and rate >= 0.85 : return True, gaps, "7"
    return False, 0, 0
############################# 涨停调整 ################################
def findSealGrow(code, name, testFlag, wk, df, qos):
    info        = ""
    wins        = maList[-1]
    if 'seal' not in df.columns : df['seal'] = [1 if a >= 9.7 else 0 for a in df.grow]
    slist       = list(df.seal.tail(wins).where(df.seal == 1).dropna().index)
    if len(slist) == 0 : return ""

    change      = df['change'].iloc[-1]
    amount      = df['amount'].iloc[-1]
    close       = df['close'].tolist()
    P0          = slist[0]
    gaps        = df.index[-1] - P0
    hgap        = (gaps + 1) * 8
    grow        = round(close[-1] / min(close[-120:]), 2)
    flag        = washHalfData(code, name, testFlag, df, hgap)
    info        = str(qos) + "9 " + str(change).rjust(5) + "%"
    if flag == False : return ""
    if grow > 1.8 : return ""
    if df.dif.iloc[-1] < 0 : return ""
    if amount < dayAmount * 0.8 : return ""
    if True :
        result  = [True, code, name.rjust(4), close[P0], str(amount)+"亿", close[-1], grow, gaps, info]
        return result
    return ""
############################# 日线右侧交易 ############################
def findRighSideGrow0(code, name, testFlag, df, qos):
    info        = ""
    close       = df['close'].tolist()
    change      = df['change'].iloc[-1]
    amount      = df['amount'].iloc[-1]
    P0          = df.close.tail(yearWins).idxmin()
    grow        = round(close[-1] / min(close[P0:]), 2)
    if df.dif.iloc[-1] < 0 : return ""
    if df.dea.iloc[-1] < -0.1 : return ""
    if grow >= 2 : return ""
    if washAmount(code, name, testFlag, df) == False : return ""

    adj         = list(df.cross.iloc[P0:].where(df.cross == -1).dropna().index)
    if len(adj) == 0 : return ""
    idx         = max(adj)
    gaps        = df.index[-1] - idx
    mcls        = df.close.iloc[idx-5:idx].min()
    hmin        = df.dif.iloc[idx:].min()
    flag0       = close[-1] > mcls
    flag1       = hmin > 0 and df.dif.iloc[-1] > hmin
    if not (flag0 or flag1) : return ""
    if df.grow.iloc[adj[-1]:].max() <= 7 :  return ""
    #if df.seal.iloc[adj[-1]:].sum() == 0 :  return ""
    if washHalfData(code, name, testFlag, df) == False : return ""
    
    if True:
        info    = str(qos) + "9  " + str(change).rjust(5) + "%"
        result  = [True, code, name.rjust(4), close[P0], str(amount)+"亿", close[-1], grow, gaps, info]
        return result
    return ""
############################# 日线左侧交易 ############################
def findLeftSideGrow0(code, name, testFlag, df, qos):
    info        = ""
    close       = df['close'].tolist()
    change      = df['change'].iloc[-1]
    amount      = df['amount'].iloc[-1]
    # macd连续保持红柱
    sta         = list(df.cross.where(df.cross == 1).dropna().index)
    gaps        = df.index[-1] - sta[-1]
    cnt         = df.delta.iloc[sta[-1]:].where(df.delta >= 0).dropna().count()
    rate        = round(cnt/gaps, 2)
    if cnt >= 30 and rate >= 0.9 and df.dif.iloc[-1] > 0 :
        if df.dif.iloc[-1] < df.dea.iloc[-1] : return ""
        if change < dayChange and df.grow.iloc[-1] < 5 : return ""
        grow    = round(close[-1] / min(close[sta[-1]:]), 2)
        info    = str(qos) + "9  " + str(change).rjust(5) + "%"
        result  = [True, code, name.rjust(4), close[sta[-1]], str(amount)+"亿", close[-1], grow, gaps, info]
        return result
    
    # dif从min盘整结束: 低点逐步抬高 or dif站稳0轴 
    flag, P0    = findMinDif(code, name, testFlag, df, yearWins)
    if flag == False : return ""
    grow        = round(close[-1] / min(close[P0-5:]), 2)
    gaps        = df.index[-1] - P0
    findDaysPoke(code, name, testFlag, df, 20)
    botlst      = list(df.bot.iloc[P0:].where(df.bot == -1).dropna().index)
    lens        = len(botlst)
    if lens == 0 : return ""
    if df.dif.iloc[P0:].max() < 0 : return ""
    if df.dif.iloc[-1] < 0 : return ""
    if testFlag : print(code, name, P0, df.index[-1], botlst)
    
    maxcls      = df.close.iloc[P0 : botlst[-1]].max()
    if lens == 1 : # 反转：dif反弹后不破0轴, close大于反弹高点
        if close[-1] < maxcls : return ""
        if df.dif.iloc[P0:].min() < -0.1 : return ""
        if change < dayChange and df.grow.iloc[-1] < 5 : return ""
        info    = str(qos) + "8  " + str(change).rjust(5) + "%"
        result  = [True, code, name.rjust(4), close[P0], str(amount)+"亿", close[-1], grow, gaps, info]
        return result
    else :
        idxm    = botlst[-2]
        idxn    = botlst[-1]
        if df.dif.iloc[idxn] > 0 :
            info    = str(qos) + "7  " + str(change).rjust(5) + "%"
        # 低点逐步抬高, dif站稳0轴
        if df.dif.iloc[idxn] >= df.dif.iloc[idxm] and close[idxn] >= close[idxm] :
            info    = str(qos) + "6  " + str(change).rjust(5) + "%"
        # 低点破新低, dif突破0轴
        else :
            if close[-1] < maxcls : return ""
            info    = str(qos) + "5  " + str(change).rjust(5) + "%"
            return ""
    if info == "": return ""
    if washHalfData(code, name, testFlag, df) == False : return ""
    if washAmount(code, name, testFlag, df) == False : return ""
    if grow > growLevel : return ""
    
    if True:
        result  = [True, code, name.rjust(4), close[P0], str(amount)+"亿", close[-1], grow, gaps, info]
        return result
    return ""
############################# half右侧交易 ############################
def findRighSideGrow1(code, name, testFlag, df, qos):
    info        = ""
    close       = df['close'].tolist()
    change      = df['change'].iloc[-1]
    amount      = df['amount'].iloc[-1]
    P0          = df.close.tail(yearWins).idxmin()
    grow        = round(close[-1] / min(close[P0:]), 2)
    gaps        = df.index[-1] - P0
    if df.dif.iloc[-1] < 0 : return ""
    if grow > growLevel: return ""
    if df.grow.tail(10).max() < 7 : return ""
    if washAmount(code, name, testFlag, df) == False : return ""
    if washHalfData(code, name, testFlag, df) == False : return ""
    
    findDeltaKeep(code, name, testFlag, df, 20)
    toplst      = list(df.top.where(df.top == 1).dropna().index)
    lens        = len(toplst)
    if lens :
        P0      = toplst[-1]
        if df.delta.iloc[-1] != df.delta.iloc[P0:].min() \
        and df.close.iloc[-1] >= df.colse.iloc[P0] :
            info    = str(qos) + "9  " + str(change).rjust(5) + "%"
            result  = [True, code, name.rjust(4), close[P0], str(amount)+"亿", close[-1], grow, gaps, info]
            return result
    
    info        = str(qos) + "8  " + str(change).rjust(5) + "%"
    if True:
        result  = [True, code, name.rjust(4), close[P0], str(amount)+"亿", close[-1], grow, gaps, info]
        return result
    return ""
############################# week右侧交易 ############################
def findRighSideGrow2(code, name, testFlag, df, qos):
    info        = ""
    close       = df['close'].tolist()
    change      = df['change'].iloc[-1]
    amount      = df['amount'].iloc[-1]
    P0          = df.close.tail(yearWins).idxmin()
    grow        = round(close[-1] / min(close[P0:]), 2)
    if df.dif.iloc[-1] < 0 : return ""
    if df.grow.tail(10).max() < 7 : return ""
    if df.amount.iloc[-1] < 2 and df.grow.iloc[-1] < 5 : return ""
    
    flag,P0,tp  = washLongGrow(code, name, testFlag, df, True)
    if flag == True : 
        info    = str(qos) + str(tp) + "W " + str(change).rjust(5) + "%"
    else :
        flag,P0,tp  = washLongGrow(code, name, testFlag, df, False)
        if flag == True : 
            info    = str(qos) + str(tp) + "M " + str(change).rjust(5) + "%"
    if info == "" : return ""
    if grow > 2.5 : return ""
    if True:
        result  = [True, code, name.rjust(4), close[P0], str(amount)+"亿", close[-1], grow, P0, info]
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
    df = pd.read_csv(baseFile, sep='\t', encoding='gbk')
    df.drop(df.tail(1).index, inplace=True)             # drop last rows
    df['amount'] = round(df['总金额'] / 10000, 2)
    df['change'] = [0 if a == '--  ' else float(a) for a in df['换手%']]
    df['grow'] = [0 if a == '--  ' else float(a) for a in df['涨幅%']]
    summ = df['amount'].sum()
    mlevel = np.percentile(df.amount, 100 - percnet)
    clevel = np.percentile(df.change, 100 - percnet)
    glevel = np.percentile(df.grow,   100 - percnet)
    topAmount = mlevel
    topChange = clevel
    topGrow = glevel
    print("\n%s :: line %3d : ############### 大盘 : "
          % ("comDef", sys._getframe().f_lineno))
    print("成交总额(%.1f)亿, Top%d%% 成交量(%.1f)亿, 换手率（%.1f%%）,涨幅(%.1f%%)" %
          (summ, percnet, mlevel, clevel, glevel))
    grow10 = len(df[df['grow'] >= 9.7])
    grow05 = len(df[df['grow'] >= 5])
    down10 = len(df[df['grow'] <= -9.7])
    down05 = len(df[df['grow'] <= -5])
    print("涨停个数(%3d), >=5个数(%3d)" % (grow10, grow05))
    print("跌停个数(%3d), <=5个数(%3d)" % (down10, down05))
def getPlatReplay(endDate, bs):
    bs = bs[bs['涨跌数'] != '--  ']
    bs['总金额'] = (bs['总金额'] / 10000).round(decimals=1)
    bs['total'] = [int(a.split(',')[0]) + int(a.split(',')[1])
                   for a in bs['涨跌数']]
    bs['snum'] = [int(a) for a in bs['涨停数']]
    bs['srate'] = (bs['snum'] / bs['total']).round(decimals=3)
    bs['sel'] = [1 if ((a >= 0.1 and c >= 2) or (a >= 0.05 and c >= 3))
                 and float(b) > 0 else 0
                 for a, b, c in zip(bs.srate, bs['涨幅%'], bs.snum)]
    sr = bs[bs['sel'] == 1]
    sr.sort_values('snum', ascending=False, inplace=True)
    #print(sr[['代码', '名称', '涨跌数', 'total', '涨停数', 'snum', 'srate']])
    print(sr[['代码', '名称', 'total', 'snum', 'srate', '总金额', '换手Z']])
    print("\n%s :: line %3d : 板块涨停率 >=5%%个数(%3d)\n" %
          ("comDef", sys._getframe().f_lineno, len(sr)))
def washPlateData(code, name, days):
    for ma in maList:
        days['cls'+str(ma)] = days.close.rolling(ma).mean().round(decimals=2)
    if days.close.iloc[-1] < days['cls5'].iloc[-1]:
        return False
    if days.close.iloc[-1] < days['cls10'].iloc[-1]:
        return False
    if days.close.iloc[-1] < days['cls20'].iloc[-1]:
        return False
    for ma in maList:
        del days['cls'+str(ma)]
    procTurtleData(days, maList[-1])
    if days.tur.iloc[-1] == 0:
        return False
    return True
def findPlateDaysMacd(code, name, testFlag, df, wk, qos):
    info = ""
    P0, P1, P2 = findMacdCross(code, name, testFlag, df, "days")
    if P0 == 0:
        return ""
    if df.amount.iloc[-1] < 30:
        return ""
    if df.delta.iloc[-1] < df.delta.iloc[-2]:
        return ""
    if df.delta.iloc[-2] < df.delta.iloc[-3]:
        return ""

    gaps = df.index[-1] - P0
    close = df.close.tolist()
    amount = df['amount'].iloc[-1]
    procMacdData(wk)
    if wk.delta.iloc[-1] < wk.delta.iloc[-2]:
        return ""
    if wk.delta.iloc[-1] < 0:
        return ""
    if wk.dea.iloc[-1] < 0:
        return ""
    if (P1 > 0 and P2 > 0):
        gaps = df.index[-1] - P2 if P2 else gaps
        info = str(qos) + "9  "
    result = [True, code, name.rjust(4), close[P0], str(
        amount)+"亿", close[-1], df.grow.iloc[-1], gaps, info]
    return result
def findPlateWeekMacd(code, name, testFlag, df, wk, qos):
    info = ""
    P0, P1, P2 = findMacdCross(code, name, testFlag, wk, "week")
    if P0 == 0:
        return ""
    if df.grow.iloc[-1] < 1:
        return ""
    if wk.grow.iloc[-1] < 1:
        return ""
    if df.amount.iloc[-1] < 100:
        return ""
    if wk.tur.iloc[-1] == 0:
        return ""

    close = wk.close.tolist()
    amount = wk['amount'].iloc[-1]
    if wk.delta.iloc[-1] < wk.delta.iloc[-2]:
        return ""
    if P2 > 0:
        gaps = wk.index[-1] - P2
        info = str(qos) + "9  "
    if info == "":
        return ""
    result = [True, code, name.rjust(4), close[P0], str(
        amount)+"亿", close[-1], wk.grow.iloc[-1], gaps, info]
    return result
######################################################################
############################# 板块处理函数 ############################
######################################################################

######################################################################
############################# 个股处理函数 ############################
######################################################################
def strClose(close):
    return str(close).rjust(6)
def getMinuteData(code, name, tp):
    loca    = "SH" if code[0] == "6" else "SZ"
    ifile   = minipath + loca + code
    if not os.path.exists(ifile):
        print(ifile, "not find")
        sys.exit(0)
    # 删除前两行和最后一行无效内容
    data         = pd.read_csv(ifile, encoding='gbk', header=2)
    data.columns = ['date', 'time', 'open','high', 'low', 'close', 'volume', 'amount']
    data         = data.iloc[:-1]
    data['time'] = [str(a).rjust(6, '0').replace('.', ':') for a in data.time]
    data['time'] = [a[0:2]+':'+a[2:] for a in data.time]
    data['date'] = [a + " " + b for a, b in zip(data.date, data.time)]
    data['time'] = [datetime.datetime.strptime(str(a), '%Y-%m-%d %H:%M:%S') for a in data.date]
    del data['time'], data['open']
    df      = getMergMinute(tp, data)
    return df
def readDataFromFile(code, name, path):
    if not os.path.exists(path):
        #print(code, name, path)
        return empty
    data    = pd.read_csv(path, parse_dates=[0])
    data.sort_values('date', ascending=True, inplace=True)
    return data
def procTurtleData(df, period):
    # apply函数仿真时间是队列操作的3倍
    df['tur'] = df.close == df.close.rolling(period).max()
    df['tur'] = [1 if a else 0 for a in (df.tur)]
def procMaData(df):
    for ma in maList:
        df['cls'+str(ma)] = df.close.rolling(ma).mean().round(decimals=2)
def delMaData(df):
    for ma in maList:
        del df['cls'+str(ma)]
def procMacdData(df):
    df['sema']  = pd.DataFrame.ewm(df.close, span=macdFast).mean().round(decimals=6)
    df['lema']  = pd.DataFrame.ewm(df.close, span=macdSlow).mean().round(decimals=6)
    df['dif']   = (df.sema - df.lema).round(decimals=3)
    df['dea']   = pd.DataFrame.ewm(df.dif, span=macdMidd).mean().round(decimals=3)
    df['macd']  = 2*(df.dif - df.dea).round(decimals=2)
    del df['sema'], df['lema'], df['macd']
    df['sdif']  = df.dif.shift(1)
    df['sdea']  = df.dea.shift(1)
    #df['dgld']  = [1 if (a < b) else 0 for a,b in zip(df.dif, df.dea)]
    #df['dglz']  = [1 if (a < 0) else 0 for a,b in zip(df.dif, df.dea)]
    df['delta'] = df.dif - df.dea
    df['cross'] = [1 if (a < b and c >= d) else -1 if (a >= b and c < d) else 0
                   for a, b, c, d in zip(df.sdif, df.sdea, df.dif, df.dea)]
    df['cross'] = [2 if (a == 1 and b >= 0) else 1 if(a == 1 and b < 0) else a
                   for a, b in zip(df.cross, df.dif)]
    del df['sdif'], df['sdea']
######################################################################
############################# 个股处理函数 ############################
######################################################################
