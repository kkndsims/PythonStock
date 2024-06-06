# -*- coding: utf-8 -*-
# -*- coding:gb2312 -*-

"""
Created on Mon Oct 10 22:59:41 2016

@author: Administrator
"""

import os, datetime
import sys
import pandas as pd
import numpy  as np
import warnings

warnings.filterwarnings( "ignore" )

minipath                = "C:\\new_tdx\\T0001\\export\\m\\"         #5分钟原始数据

vlList                  = [5, 10]
maList                  = [5, 10, 20]
hfList                  = [5, 10, 20, 30]

isMacdMode              = 0     #0:short; 1:long
macdFast                = 24 if isMacdMode == 1 else 12
macdSlow                = 52 if isMacdMode == 1 else 26
macdMidd                = 18 if isMacdMode == 1 else 9

dayAmount               = 5
dayChange               = 3
dyaLevel                = 0.95
dltLevel                = 0.35
growLevel               = 1.65
dayClose                = 66

weekOfYear              = 52 * 4
daysOfYear              = 250 * 1.2
longProcType            = ['week']
shortProcType           = ['days']
    
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
def findPlateBuy(code, name, baseInfo, testFlag, days):
    week                = pd.DataFrame()
    # 日线站上5/10/20线
    flag                = washPlateData(code, name, days)
    if  flag == False : return flag, code, name
    
    if True :
        # 板块有涨停且周线破新高
        value           = baseInfo.loc[ baseInfo['名称'] == name] 
        if int(value['涨停数'].iloc[-1]) < 2 : return False, code, name
        week            = getMergData("week", days)
        procTurtleData(week, maList[-1])
        if week.tur.iloc[-1] == 0 : return False, code, name
        
        result          = findPlateDaysMacd(code[2:], name, testFlag, days, week, "9PD.")
        if result : return result
    
        result          = findPlateWeekMacd(code[2:], name, testFlag, days, week, "8PW.")
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
    #isSeal              = info['封单额'].iloc[-1] != '--  ' or days.grow.iloc[-1] > 9.5
    #tolAmount           = float(info['流通市值'].iloc[-1].replace("亿","").replace(" ",""))
    # 日线站上5/10/20线
    flag                = washDaysData(code, name, days)
    if  flag == False : return flag, code, name
    
    #flag, week          = washWeekData(code, name, days)
    #if  flag == False : return flag, code, name
    
    if findEnab and 1 :
        result          = findSealGrow(code[2:], name, testFlag, days, days, "9S.")
        if result : return result

    if findEnab and 1 :
        # 日线dif调整后上涨中继
        result          = findDaysGrow(code[2:], name, testFlag, days, days, "8D.")
        if result : return result
        
        result          = findHalfGrow(code[2:], name, testFlag, days, days, "7H.")
        if result : return result

    return False, code, name
    sys.exit(0)

############################# 日/周/月线清洗数据 ####################
def washDaysData(code, name, days) :
    if len(days) < maList[-1] : return False
    if days.close.iloc[-1] > dayClose : return False
    # 日线站上5/10/20均线
    for ma in maList:
        days['cls'+str(ma)] = days.close.rolling(ma).mean().round(decimals=2)
    if days.close.iloc[-1]  < days['cls5' ].iloc[-1] : return False
    if days.close.iloc[-1]  < days['cls10'].iloc[-1] : return False
    if days.close.iloc[-1]  < days['cls20'].iloc[-1] : return False
    for ma in maList:
        del days['cls'+str(ma)]
        
    procTurtleData(days, maList[-1])
    procMacdData(days)
    if days.dif.iloc[-1] < -0.1 : return ""
    days['seal']            = [ 1 if a >= 9.7 else 0 for a in days.grow]
    del days['open'], days['low'], days['volume']

    # 成交活跃有量或大阳
    vtype       = 0
    flag1       = days.seal.tail(10).sum()
    flag2       = days.amount.iloc[-1] >= dayAmount and days.change.iloc[-1] >= dayChange
    flag21      = days.amount.tail(10).where(days.amount >= dayAmount).dropna().sum()
    flag3       = days.grow.tail(10).where(days.grow >= 5).dropna().sum()
    flag4       = days.tur.tail(10).sum()
    if vtype :
        if not (flag1 or flag2 or flag3) : return False
    else :
        if not (flag1 or flag2 or flag21 or flag3 or flag4) : return False
    return True
def washWeekData(code, name, days) :
    week        = getMergData("week", days)
    procMacdData(week)
    for ma in maList[:2]:
        week['cls'+str(ma)] = week.close.rolling(ma).mean().round(decimals=2)
    if week.close.iloc[-1]  < week['cls5' ].iloc[-1] : return False, week
    if week.close.iloc[-1]  < week['cls10'].iloc[-1] : return False, week
    for ma in maList[:2]:
        del week['cls'+str(ma)]
    return True, week
############################# 合并月/周线数据 ######################
def getMergData(otype, data) :
    period              = "W" if otype == "week" else "M"
    data['date']        = pd.to_datetime(data['date']) #把字符串转换成时间信息    
    data                = data.set_index('date')
    df2                 = data.resample          (period,closed="right",label="right").last()
    df2                 = df2[~df2['amount'].isin([0])]
    df2                 = df2.dropna()
    if 'seal' in df2.columns : del df2['seal']
    if 'tur'  in df2.columns : del df2['tur']
    if 'dif'  in df2.columns : del df2['dif']
    if 'dea'  in df2.columns : del df2['dea']
    if 'delta'in df2.columns : del df2['delta']
    if 'cross'in df2.columns : del df2['cross']
    if 'oepn' in df2.columns:
        #df2['open']     = data['open'].resample  (period,closed="right",label="right").first().round(decimals=2)
        del df2['open']
    if 'high' in df2.columns:
        df2['high']     = data['high'].resample  (period,closed="right",label="right").max().round(decimals=2)
    if 'low' in df2.columns:
        #df2['low']      = data['low'].resample   (period,closed="right",label="right").min().round(decimals=2) 
        del df2['low']
    df2['close']        = data['close' ].resample(period,closed="right",label="right").last().round(decimals=2)    
    if 'volume' in df2.columns:
        #df2['volume']   = data['volume'].resample(period,closed="right",label="right").sum().round(decimals=1)  
        del df2['volume']
    if 'amount' in df2.columns:
        df2['amount']   = data['amount'].resample(period,closed="right",label="right").sum().round(decimals=1)
    if 'change' in df2.columns:
        df2['change']   = data['change'].resample(period,closed="right",label="right").sum().round(decimals=2)  
    if 'chgTol' in df2.columns:
        df2['chgTol']   = data['chgTol'].resample(period,closed="right",label="right").sum().round(decimals=2)
    df2['grow']         = ((df2['close']  / df2['close'].shift(1) - 1) * 100).round(decimals=2)
    df2.reset_index(inplace=True)
    procTurtleData(df2, maList[-1])
    return df2
def getMergMinute(otype, data) :
    period              = otype
    data['date']        = pd.to_datetime(data['date']) #把字符串转换成时间信息    
    data                = data.set_index('date')
    df2                 = data.resample(period,closed="right",label="right").last()
    df2                 = df2[~df2['amount'].isin([0])]
    df2                 = df2.dropna()
    if 'high' in df2.columns:
        df2['high']     = data['high'].resample  (period,closed="right",label="right").max().round(decimals=2)
    if 'low' in df2.columns:
        df2['low']      = data['low'].resample   (period,closed="right",label="right").min().round(decimals=2) 
    df2['close']        = data['close' ].resample(period,closed="right",label="right").last().round(decimals=2)    
    if 'volume' in df2.columns:
        df2['volume']   = data['volume'].resample(period,closed="right",label="right").sum().round(decimals=1)  
    if 'amount' in df2.columns: del df2['amount']
    df2.reset_index(inplace=True)
    return df2
############################# macd区间 ##########################
def findMacdCross(code, name, testFlag, df, otype):
    procMacdData(df)
    sta             = list(df.cross.where(df.cross == 1).dropna().index)
    if len(sta) == 0 : return [0, 0, 0]
    P0              = sta[-1]
    adj             = df.cross.iloc[P0:].where(df.cross == -1).dropna().index
    ctn             = df.cross.iloc[P0:].where(df.cross == 2).dropna().index
    P1              = adj[-1] if len(adj) else 0
    P2              = ctn[-1] if len(ctn) else 0
    if testFlag :
        print("findMacdCross", code, name, [P0, P1, P2])
    return [P0, P1, P2]
def findDaysPoke(code, name, testFlag, df, wins):
    halfwins        = wins//2 + 1
    df['poke']      = df.delta.rolling(wins, center=True).min()
    df.poke.iloc[-halfwins:] = df.delta.tail(halfwins).min()
    df['poke']      = [1 if a == b and c < 0 and d < 0 else 0 for a,b,c,d in zip(df.delta, df.poke, df.dif, df.dea)]
    df['adj']       = [-1 if a == -1 and b > 0 and c > 0 else 0 for a,b,c in zip(df.cross, df.dif, df.dea)]
############################# 日线/周线放量区间 ##########################
def findMacdCtniu(code, name, testFlag, df, wins, otype) :
    df['abc']       = abs(df.delta)
    hdiff           = np.diff(np.sign(np.diff(df.abc)))
    hpeak           = (np.where(hdiff == -2)[0] + 1).tolist()
    hpoke           = (np.where(hdiff ==  2)[0] + 1).tolist()
    df['pk']        = 0
    for i in hpoke : df.pk.iloc[i] = 1
    #df['mdlt']      = df.abc.rolling(wins, center=False).min()
    #df['pk']        = [1 if (a and b == c) else 0 for a,b,c in zip(df.pk, df.abc, df.mdlt)]
    
    sta             = list(df.cross.where(df.cross == 1).dropna().index)
    if len(sta) == 0 : return [0, 0]
    P0              = sta[-1]
    
    ctn             = list(df.pk.where(df.pk == 1).dropna().index)
    if len(ctn) == 0 : return [0, 0]
    if ctn[-1] < P0  : return [0, 0]
    P1              = ctn[-1]
    
    if df.dif.iloc[P1] < 0 : return [0, 0]
    if df.dif.iloc[P1] < df.dea.iloc[P1] : return [0, 0]
    del df['abc'], df['pk']
    return [P0, P1]
def findDifPoke(code, name, testFlag, df) :
    df['abc']       = abs(df.delta)
    hdiff           = np.diff(np.sign(np.diff(df.abc)))
    hpeak           = (np.where(hdiff == -2)[0] + 1).tolist()
    hpoke           = (np.where(hdiff ==  2)[0] + 1).tolist()
    del df['abc']
    return hpoke[-1] if len(hpoke) else 0
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
def findSealGrow(code, name, testFlag, wk, df, qos):
    info            = ""
    wins            = maList[-1]
    if 'seal' not in df.columns : df['seal'] = [ 1 if a >= 9.7 else 0 for a in df.grow]
    slist           = list(df.seal.tail(wins).where(df.seal == 1).dropna().index)
    if len(slist) == 0 : return ""
    
    change          = df['change'].iloc[-1]
    amount          = df['amount'].iloc[-1]
    close           = df['close'].tolist()
    P0              = slist[0]
    gaps            = df.index[-1] - P0
    hgap            = (gaps + 1) * 8
    qgap            = (gaps + 1) * 16
    grow            = round(close[-1] / min(close[-120 :]), 2)
    if grow > 1.8 : return ""
    if amount < topAmount : return ""
    if change < dayChange : return ""
    if df.grow.iloc[-1] < -5 : return ""
    
    hf              = getMinuteData(code, name, '30min')
    procMacdData(hf)
    if hf.dif.iloc[-1] < 0 and hf.dea.iloc[-1] < 0 : return ""
    if hf.dif.iloc[-1] < 0 : return ""
    if hf.dif.iloc[-1] < hf.dea.iloc[-1] :
        if hf.delta.iloc[-1] < hf.delta.iloc[-2] : return ""
    
    qt              = getMinuteData(code, name, '15min')
    procMacdData(qt)
    findDaysPoke(code, name, testFlag, qt, wins)
    if qt.dif.iloc[-1] < 0 and qt.dea.iloc[-1] < 0 : return ""
    
    #mxvol           = hf.volume.tail(hgap).idxmax()
    #if hf.close.iloc[-1] < hf.close.iloc[mxvol] * 0.98 : return ""
    
    findDaysPoke(code, name, testFlag, hf, wins)
    adj             = list(hf.adj.tail(hgap).where(hf.adj == -1).dropna().index)
    if len(adj) > 0 :
        info        = str(qos) + "8 " + str(change).rjust(5) + "%"
        if hf.dea.iloc[-1] < 0 : return ""
        if hf.dif.iloc[-1] < hf.dea.iloc[-1] :
            info    = str(qos) + "9 " + str(change).rjust(5) + "%"
            mxcls   = hf.close.tail(hgap).max()
            if hf.close.iloc[-1] < mxcls * 0.97 : return ""
    else :
        info        = str(qos) + "7 " + str(change).rjust(5) + "%"
        adj         = list(qt.adj.tail(qgap).where(qt.adj == -1).dropna().index)
        if len(adj) == 0 : return ""
        if len(adj) == 1 and qt.index[-1] - adj[-1] <= 8 : return ""
        if qt.dif.iloc[-1] < qt.dea.iloc[-1] :
            mxcls   = qt.close.tail(qgap).max()
            if qt.close.iloc[-1] < mxcls * 0.98 : return ""
            #print(code, name, len(adj))
            #if qt.dif.iloc[-1] != qt.dif.iloc[point:].min() : 
            #    if qt.close.iloc[-1] != qt.close.iloc[adj[-1]:].min() : return ""
    
    if True \
    and True :
        result          = [True, code, name.rjust(4), close[P0], str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""
############################# 日线放量后连涨或回调破新高 #######################
def findDaysGrow(code, name, testFlag, wk, df, qos):
    info            = ""
    wins            = maList[-1]
    findDaysPoke(code, name, testFlag, df, wins)
    bot             = list(df.poke.where(df.poke == 1).dropna().index)
    adj             = list(df.adj. where(df.adj == -1).dropna().index)
    if df.grow.iloc[-1] < -5 : return ""
    if df.dif.iloc[-1] < 0 and df.dea.iloc[-1] < 0 : return ""
    if df.dif.iloc[-1] < -0.1 : return ""
    if df.dea.iloc[-1] < -0.1 : return ""
    if df.dif.iloc[-1] < df.dea.iloc[-1] and df.delta.iloc[-1] < df.delta.iloc[-2] : return ""
    if len(bot) == 0 : 
        print(code, name)
        return ""

    B0              = bot[-1]
    B1              = 0 if len(bot) == 1 else bot[-2]
    A0              = 0 if len(adj) == 0 else adj[-1]
    close           = df['close'].tolist()
    change          = df['change'].iloc[-1]
    amount          = df['amount'].iloc[-1]
    gaps            = df.index[-1] - B0
    hgap            = (gaps + 1) * 8
    grow            = round(close[-1] / min(close[-120 :]), 2)
    if df.grow.tail(gaps).where(df.grow > 7).dropna().count() == 0 : return ""
    if not (amount >= dayAmount * 0.9 and change >= dayChange) : return ""
    if change >= 50 : return ""
    if grow > growLevel : return ""
    if df.seal.iloc[-1] : return ""
    # bot --> adjust start --> tur
    if A0 > B0 :
        mxcls       = df.close.iloc[B0 : A0].max()
        # dif > dea
        if df.dif.iloc[-1] > df.dea.iloc[-1] :
            info    = str(qos) + "9  " + str(change).rjust(5) + "%"
            if close[-1] < mxcls : return ""
        # dif <= dea and nearby 0轴
        else :
            info    = str(qos) + "8  " + str(change).rjust(5) + "%"
            if close[-1] < mxcls * 0.98 : return "" 
    # bot --> adjust few days --> continue grow
    else :
        info        = str(qos) + "7  " + str(change).rjust(5) + "%"
        mxcls       = df.close.iloc[B1 : B0].max()
        if close[-1] < mxcls : return ""
        mxcls       = df.close.iloc[B0 : ].max()
        if close[-1] < mxcls * 0.98 : return "" 

    if 1 :
        hf          = getMinuteData(code, name, '30min')
        procMacdData(hf)
        if hf.dif.iloc[-1] < 0 and hf.dea.iloc[-1] < 0 : return ""
        if hf.dif.iloc[-1] < 0 : return ""
        
        qt          = getMinuteData(code, name, '15min')
        procMacdData(qt)
        if qt.dif.iloc[-1] < 0 and qt.dea.iloc[-1] < 0 : return ""
        
        #if hf.dif.iloc[-1] < hf.dea.iloc[-1] :
        #    findDaysPoke(code, name, testFlag, hf, wins)
        #    hfadj   = list(hf.adj. where(hf.adj == -1).dropna().index)
        #    if len(hfadj) > 0 :
        #        mxcls   = hf.close.iloc[-hgap:hfadj[-1]].max()
        #        if hf.close.iloc[-1] < mxcls * 0.97 : return ""
            
        #if hf.dif.iloc[-1] < 0 : return ""
        #if hf.dif.iloc[-1] < hf.dea.iloc[-1] and hf.delta.iloc[-1] < hf.delta.iloc[-2] : return ""
        #if hf.dif.iloc[-1] < hf.dea.iloc[-1] :
        #    findDaysPoke(code, name, testFlag, hf, wins)
        #    hfadj   = list(hf.adj. where(hf.adj == -1).dropna().index)
        #    if hf.delta.iloc[-1] == hf.delta.iloc[hfadj[-1] : ].min() : return ""
    
    if info == "" : return ""
    if True \
    and True :
        result          = [True, code, name.rjust(4), close[B0], str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""       
    print(code, name, info)
    sys.exit(0)
############################# 日线放量后连涨或回调破新高 #######################
def findHalfGrow(code, name, testFlag, wk, df, qos):
    info            = ""
    wins            = maList[-1]
    halfwins        = wins//2
    df['sta']       = df.dif.rolling(wins, center=True).min()
    df.sta.iloc[-halfwins:] = df.dif.tail(halfwins).min()
    df['sta']       = [1 if a == b else 0 for a,b in zip(df.dif, df.sta)]
    sta             = list(df.sta.where(df.sta == 1).dropna().index)
    if df.seal.iloc[-1] : return ""
    if df.dif.iloc[-1] < -0.1 : return ""
    if df.dea.iloc[-1] < -0.1 : return ""
    if df.dif.iloc[-1] < df.dea.iloc[-1] : return ""
    if len(sta) == 0 : 
        print(code, name)
        return ""

    close           = df['close'].tolist()
    change          = df['change'].iloc[-1]
    amount          = df['amount'].iloc[-1]
    grow            = round(close[-1] / df.close.tail(120).min(), 2)
    if grow > growLevel : return ""
    P0              = sta[-1]
    gaps            = df.index[-1] - P0
    
    hf              = getMinuteData(code, name, '30min')
    procMacdData(hf)
    if hf.dif.iloc[-1] < 0 and hf.dea.iloc[-1] < 0 : return ""
    if hf.dif.iloc[-1] < 0 : return ""
    if hf.dif.iloc[-1] < hf.dea.iloc[-1] :
        if hf.delta.iloc[-1] < hf.delta.iloc[-2] : return ""
    
    qt              = getMinuteData(code, name, '15min')
    procMacdData(qt)
    if qt.dif.iloc[-1] < 0 and qt.dea.iloc[-1] < 0 : return ""    

    findDaysPoke(code, name, testFlag, hf, wins)
    hfgaps          = (gaps + 1) * 8
    hfadj           = list(hf.adj.tail(hfgaps).where(hf.adj == -1).dropna().index)
    info            = str(qos) + "9  " + str(change).rjust(5) + "%"
    #if len(hfadj) == 0 : return ""
    #if len(hfadj) >  2 : return ""
    #if hf.dif.iloc[-1] < hf.dea.iloc[-1] : 
    #    if hf.dif.iloc[-1] < hf.dea.iloc[-2] : return ""
    #    if hf.dif.iloc[-2] < hf.dea.iloc[-3] : return ""
    if not (amount >= dayAmount * 0.9 and change >= dayChange) : return ""

    if True \
    and True :
        result          = [True, code, name.rjust(4), close[P0], str(amount)+"亿", close[-1], grow, gaps, info]
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
def getPlatReplay(endDate, bs):
    bs                  = bs[bs['涨跌数'] != '--  ']
    bs['总金额']         = (bs['总金额'] / 10000).round(decimals=1)
    bs['total']         = [int(a.split(',')[0]) + int(a.split(',')[1]) for a in bs['涨跌数']]
    bs['snum']          = [int(a) for a in bs['涨停数']]
    bs['srate']         = (bs['snum']  / bs['total']).round(decimals=3)
    bs['sel']           = [1 if ((a >= 0.1 and c >= 2) or (a >= 0.05 and c >= 3)) \
                           and float(b) > 0 else 0 \
                           for a,b,c in zip(bs.srate, bs['涨幅%'], bs.snum)]
    sr = bs[bs['sel'] == 1]
    sr.sort_values('snum', ascending=False, inplace=True)
    #print(sr[['代码', '名称', '涨跌数', 'total', '涨停数', 'snum', 'srate']])
    print(sr[['代码', '名称', 'total', 'snum', 'srate', '总金额', '换手Z']])
    print("\n%s :: line %3d : 板块涨停率 >=5%%个数(%3d)\n" %("comDef", sys._getframe().f_lineno, len(sr)))
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
    if df.delta.iloc[-1]  < df.delta.iloc[-2] : return ""
    if df.delta.iloc[-2]  < df.delta.iloc[-3] : return ""
    
    gaps                = df.index[-1] - P0
    close               = df.close.tolist()
    amount              = df['amount'].iloc[-1]
    procMacdData(wk)
    if wk.delta.iloc[-1] < wk.delta.iloc[-2] : return ""
    if wk.delta.iloc[-1] < 0 : return ""
    if wk.dea.iloc[-1] < 0 : return ""
    if (P1 > 0 and P2 > 0) :
        gaps            = df.index[-1] - P2 if P2 else gaps
        info            = str(qos) + "9  "
    result              = [True, code, name.rjust(4), close[P0], str(amount)+"亿", close[-1], df.grow.iloc[-1], gaps, info]
    return result
def findPlateWeekMacd(code, name, testFlag, df, wk, qos) :
    info                = ""
    P0, P1, P2          = findMacdCross(code, name, testFlag, wk, "week")
    if P0 == 0 : return ""
    if df.grow.iloc[-1] < 1 : return ""
    if wk.grow.iloc[-1] < 1 : return ""
    if df.amount.iloc[-1]< 100 : return "" 
    if wk.tur.iloc[-1] == 0 : return ""

    close               = wk.close.tolist()
    amount              = wk['amount'].iloc[-1]
    if wk.delta.iloc[-1] < wk.delta.iloc[-2] : return ""
    if P2 > 0 :
        gaps            = wk.index[-1] - P2
        info            = str(qos) + "9  "
    if info == "" : return ""
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
def getMinuteData(code, name, tp) :
    loca            = "SH" if code[0] == "6" else "SZ" 
    ifile           = minipath + loca + code
    if not os.path.exists(ifile) : 
        print(ifile, "not find")
        sys.exit(0)
    # 删除前两行和最后一行无效内容
    data            = pd.read_csv(ifile,encoding='gbk',header=2)
    data.columns    = ['date','time','open','high','low','close','volume','amount']
    data            = data.iloc[:-1]
    data['time']    = [str(a).rjust(6, '0').replace('.', ':') for a in data.time]
    data['time']    = [a[0:2]+':'+a[2:] for a in data.time]
    data['date']    = [a + " " + b for a, b in zip(data.date, data.time)]
    data['time']    = [datetime.datetime.strptime(str(a), '%Y-%m-%d %H:%M:%S') for a in data.date]
    del data['time'], data['open']
    df              = getMergMinute(tp, data)
    return df
def readDataFromFile(code, name, path):
    if not os.path.exists(path) :
        #print(code, name, path)
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


