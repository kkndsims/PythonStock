# -*- coding: utf-8 -*-
"""
Created on Tue Mar 21 22:53:53 2017

@author: Administrator
"""

import os, sys
import pandas            as pd
import numpy             as np
import datetime          as dt
import matplotlib.pyplot as plt

from matplotlib import dates  as mdates
from matplotlib import ticker as mticker
#from matplotlib.finance import candlestick_ohlc
#from matplotlib.dates   import DateFormatter, WeekdayLocator, DayLocator, MONDAY, YEARLY
#from matplotlib.dates   import MonthLocator, MONTHLY

diffLevel           = 0.3
diffMax             = 9.0

macdLevel           = 0.3
macdMax             = 9.0

daysLevel           = 5

growRate            = 1.2
growWind            = 5
growGap             = 1.05
growGap             = 1.1

drwaFlag            = False

######################################################################
################## set global debug level here #######################
######################################################################
def setDebugEn(flag):
    global drwaFlag
    drwaFlag        = flag
    
######################################################################
################## calc data's ZHONGSHU buy point ####################
######################################################################
def ZSBuyPoint(code, name, data, testFlag, zhongshu):
    findFlag        = False
    close           = list(map(lambda x : round(x, 2), data['close'].tolist()))
    if len(zhongshu) < 6:
        return findFlag, code, name
    
    #取6个中枢点+close[-1]
    cls1 = zhongshu[-1][2]; cls2 = zhongshu[-2][2]; 
    cls3 = zhongshu[-3][2]; cls4 = zhongshu[-4][2]; 
    cls5 = zhongshu[-5][2]; cls6 = zhongshu[-5][2];     
    cls0 = close[-1];       idx  = zhongshu[-1][0];      
    cls2 = max(cls2, max(close[idx-20:idx]))    
    
    days        = len(data) - zhongshu[-1][0]
    grow0       = round(cls2 / cls3, 2)
    grow1       = round(cls0 / cls3, 2)
    
    if zhongshu[-1][1] == 0: #最近是一个低点
        #第三类买点，低点大于中枢高点
        if  cls1 >= cls4 and cls0 > cls2 and cls0 > cls1\
        and cls0 < 30\
        and cls0 >= max(cls2, cls4, cls6)\
        and grow1 > 1.1 and grow1 < 1.5\
        and thdPoint :
            findFlag    = True
            result      = [findFlag, code, name, cls4, cls2, cls1, cls0, grow0, days, grow1, "3th buy"]
            return result
        if  cls0 >= max(cls2, cls4, cls6) \
        and cls0 < 30\
        and grow1 > 1.1 and grow1 < 1.5\
        and secPoint : #第二类买点，低点出现在中枢下面或者里面
            findFlag    = True
            result      = [findFlag, code, name, cls4, cls2, cls1, cls0, grow0, days, grow1, "2th buy"]
            return result
    
    return findFlag, code, name            






def getBottenBuy(code, name, data, testFlag):
    findFlag        = False
    data            = procMacdData(data)
    data            = procVolmData(data)
    data            = procMaData(data)
    close           = list(map(lambda x : round(x, 2), data['close'].tolist()))
    dif             = list(map(lambda x : round(x, 3), data['dif' ].tolist()))
    clsm5           = list(map(lambda x : round(x, 3), data['ma_5'].tolist()))    
    clsm10          = list(map(lambda x : round(x, 3), data['ma_10'].tolist())) 
    clsm20          = list(map(lambda x : round(x, 3), data['ma_20'].tolist()))  
    macd            = list(map(lambda x : round(x, 3), data['macd'].tolist()))
    dlt             = list(map(lambda x : round(x, 3), (data['dif'] - data['dea']).tolist()))
    hdiff           = np.diff(np.sign(np.diff(close)))
    ldiff           = np.diff(np.sign(np.diff(close)))
    peak            = (np.where(hdiff == -2)[0] + 1).tolist()   #峰值
    poke            = (np.where(ldiff ==  2)[0] + 1).tolist()   #波谷
    top             = [[x, 1, close[x]] for x in peak]
    bot             = [[x, 0, close[x]] for x in poke]
    tol             = top + bot
    tol.sort(reverse=False)
    
    lastMinVal      = -1
    lastMinIdx      = -1
    lastBotFlag     = True
    lastBotIdx      = -1
    lastBotVal      = -1
    for i in range(len(bot)-1, -1, -1):
        idx         = bot[i][0]
        if lastBotFlag and data['close'].iloc[idx] != data['close'].iloc[idx-5:idx+5].min():
            lastBotFlag = False
            lastBotVal  = close[idx]
            lastBotIdx  = idx
            return findFlag, code, name
        if data['close'].iloc[idx] == data['close'].iloc[idx-5:idx+5].min():
            lastMinVal  = close[idx]
            lastMinIdx  = idx
            break
        
    lastMaxVal      = data['close'].iloc[lastMinIdx-20:lastMinIdx].max()
    lastMaxIdx      = data['close'].iloc[lastMinIdx-20:lastMinIdx].idxmax()
    grow            = round(close[-1] / lastMinVal, 2)
    days            = len(data) - lastMinIdx
    daysAvg         = round((grow-1)/days, 2)
    mindif          = min(dif[lastMaxIdx:lastMinIdx])
    mindlt          = min(dlt[lastMaxIdx:lastMinIdx])
    
    if testFlag:
        print (lastMaxIdx, lastMaxVal, lastMinIdx, lastMinVal, lastBotIdx, lastBotVal, days, grow)
        print ("cond1 :", close[-1] > lastMaxVal and grow > 1.05)
        print ("cond2 :", days >= 2)
        print ("cond3 :", mindif > 0 and mindlt > -0.03)
        print ("cond4 :", close[-1] < 45)
        print ("cond5 :", close[-1] > clsm5[-1] and  data['low'].iloc[-1] > clsm5[-1])
        print ("cond6 :", clsm5[-1] > clsm10[-1] and clsm5[-1] > clsm20[-1])     
        print ("cond7 :", daysAvg >= 0.03) 
    
    if  close[-1] > lastMaxVal and grow > 1.05\
    and days >= 2 \
    and mindif > 0 and mindlt > -0.03 \
    and close[-1] < 45\
    and close[-1] > clsm5[-1] and  data['low'].iloc[-1] > clsm5[-1]\
    and clsm5[-1] > clsm10[-1] and clsm5[-1] > clsm20[-1]\
    and daysAvg >= 0.03\
    and True:
        findFlag    = True
        result      = [findFlag, code, name, lastMaxVal, lastMinVal, close[-1], macd[-1], daysAvg, days, grow, str(days)+"天涨幅"]
        return result
    return findFlag, code, name


def getGrowConts(code, name, data, testFlag):
    findFlag        = False
    lens            = len(data)
    data            = procMacdData(data)
    data            = procVolmData(data)
    data            = procMaData(data)
    close           = list(map(lambda x : round(x, 2), data['close'].tolist()))
    clsm5           = list(map(lambda x : round(x, 3), data['ma_5'].tolist())) 
    clsm10          = list(map(lambda x : round(x, 3), data['ma_10'].tolist()))
    clsm20          = list(map(lambda x : round(x, 3), data['ma_20'].tolist()))
    dif             = list(map(lambda x : round(x, 3), data['dif' ].tolist()))
    dlt             = list(map(lambda x : round(x, 3), data['dlt' ].tolist()))
    high            = list(map(lambda x : round(x, 2), data['high'].tolist()))           
    macd            = list(map(lambda x : round(x, 3), data['macd'].tolist()))
    hdiff           = np.diff(np.sign(np.diff(close)))
    ldiff           = np.diff(np.sign(np.diff(close)))
    peak            = (np.where(hdiff == -2)[0] + 1).tolist()   #峰值
    poke            = (np.where(ldiff ==  2)[0] + 1).tolist()   #波谷
    top             = [[x, 1, dlt[x]] for x in peak]
    bot             = [[x, 0, dlt[x]] for x in poke]
    tol             = top + bot
    tol.sort(reverse=False)
    
    idx             = tol[-1][0]
    isbot           = tol[-1][1]
    days            = lens - idx - 1
    lastTop         = close[peak[-1]]
    if isbot == 0 : #last is a botton
        grow        = round(close[-1] / close[idx], 2)
        #print (close[idx], clsm5[idx], clsm10[idx], close[idx+1],data['close'].iloc[idx-10:idx+1].max())
        #print (close[idx] > clsm5[idx] and clsm5[idx] > clsm10[idx])
        if (close[idx] >= clsm5[idx] or close[idx+1] > clsm5[idx+1])\
        and clsm5[idx] >= clsm10[idx] and clsm10[idx] >= clsm20[idx]\
        and close[idx+1] == data['close'].iloc[idx-20:idx+2].max()\
        and high[idx+1]  == data['high'].iloc[idx-20:idx+2].max()\
        and close[-1] > lastTop\
        and grow >= 1.07\
        and days >= 2\
        and close[-1] < 25 \
        and True :
            findFlag    = getWeekPoint(code, name, data, testFlag)
            #findFlag    = True
            result      = [findFlag, code, name, close[idx], close[-1], dlt[-1], dif[-1], macd[-1], days, grow, str(days)+"天涨幅"]
            #if testFlag :
            #    print("\n%s :: line %3d : debug with lastclose = %0.2f, idx = %0.2f, close = %0.2f, dasy = %d"\
            #    %("comApi", sys._getframe().f_lineno, close[-1], idx, close[-1], grow))            
            return result
        else:    
            return findFlag, code, name
    return findFlag, code, name





######################################################################
################## draw a stock image of grawfast ####################
######################################################################
def getFirstBuy(code, name, data, testFlag, np):
    findFlag        = False   
    zs              = np[3]
    pro             = np[4]
    istop           = pro[-1][1]
    val             = pro[-1][3] if pro[-1][1] else pro[-1][2]
    idx             = pro[-1][0] if pro        else 0
    days            = len(data) - idx
    df              = pd.DataFrame(zs) 
    df.columns      = ['zsl', 'zsh', 'sidx', 'eidx']
    high            = max(data['high'].iloc[idx-1], data['high'].iloc[idx], data['high'].iloc[idx+1])
    close           = data['close'].iloc[-1]
    if not zs:
        return findFlag, code, name
    if testFlag :
        print("%s :: line %3d : debug with lastmin = %0.2f, zs = %s, pro = %s, lens = %d, idx = %d, days = %d"\
        %("comApi", sys._getframe().f_lineno, val, zs, pro, len(data), idx, days))
    
    zsl = zs[-1][0]; zsh = zs[-1][1]; sidx = zs[-1][2];      
    if istop == 0 and df['zsl'].iloc[-1] == df['zsl'].min() : #低点小于中枢
        if val < zsl and days < 8 and close > high :
            findFlag    = True
            bstart      = data['close'].iloc[sidx]
            down        = round(val/bstart, 2)
            if testFlag :
                print("%s :: line %3d : debug with zsl = %0.2f, zsh = %0.2f, bstart = %0.2f, lastmin = %0.2f, down = %0.2f, days = %d"\
                %("comApi", sys._getframe().f_lineno, zsl, zsh, bstart, val, down, days))
            result      = [findFlag, code, name, zsl, zsh, bstart, val, down, days, '形成1买']
            return result
        
    if istop == 0 :
        if val > zsh and days < 8 and close > high :
            findFlag    = True
            down        = round(close/val, 2)
            if testFlag :
                print("%s :: line %3d : debug with zsl = %0.2f, zsh = %0.2f, close = %0.2f, lastmin = %0.2f, down = %0.2f, days = %d"\
                %("comApi", sys._getframe().f_lineno, zsl, zsh, close, val, down, days))
            result      = [findFlag, code, name, zsl, zsh, close, val, down, days, '形成3买']
            return result
    return findFlag, code, name

def getDaysBuy1(code, name, data, testFlag, np):
    findFlag        = False   
    zs              = np[3]
    pro             = np[4]
    istop           = pro[-1][1]
    val             = pro[-1][3] if pro[-1][1] else pro[-1][2]
    idx             = pro[-1][0] if pro        else 0
    days            = len(data) - idx
    df              = pd.DataFrame(zs) 
    df.columns      = ['zsl', 'zsh', 'sidx', 'eidx']   
    pokecls         = max(data['close'].iloc[idx+1], data['close'].iloc[idx+2]) if days > 2 else data['close'].iloc[idx+1]
    pokehigh        = max(data['high'].iloc[idx-1], data['high'].iloc[idx])
    close           = data['close'].iloc[-1]
    if not zs:
        return findFlag, code, name
    if testFlag :
        print("\n%s :: line %3d : debug with lastmin = %0.2f, zs = %s, pro = %s, lens = %d, idx = %d, days = %d"\
        %("comApi", sys._getframe().f_lineno, val, zs, pro, len(data), idx, days))
    
    zsl = zs[-1][0]; zsh = zs[-1][1]; sidx = zs[-1][2]; 
    if istop == 0 and pokecls > pokehigh : 
        if df['zsl'].iloc[-1] == df['zsl'].min() and val < zsl and days <= 3 :
            findFlag    = True
            bstart      = data['close'].iloc[sidx]
            down        = round(val/bstart, 2)
            if testFlag :
                print("%s :: line %3d : debug with zsl = %0.2f, zsh = %0.2f, close = %0.2f, lastmin = %0.2f, down = %0.2f, days = %d"\
                %("comApi", sys._getframe().f_lineno, zsl, zsh, bstart, val, down, days))
            result      = [findFlag, code, name, zsl, zsh, bstart, val, down, days, '日线1买']
            return result        
        
        if val > zsh and days < 5 :
            findFlag    = True
            down        = round(close/val, 2)
            result      = [findFlag, code, name, zsl, zsh, close, val, down, days, '日线3买']
            return result
    return findFlag, code, name

######################################################################
################## draw a stock image of grawfast ####################
######################################################################
    


######################################################################
################## calc data's ZHONGSHU buy point ####################
######################################################################
def getDaysBuy1(code, name, testFlag):
    findFlag        = False
    validFlag       = False
    ifile           = dayspath + code 
    if not os.path.exists(ifile) :
        return findFlag, code, name, 'stock data is not exits'
    
    data            = pd.read_csv(ifile, parse_dates=[0])
    data.columns    = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']
    data            = data[~data['volume'].isin([0])]
    data.sort_values('date', ascending=True, inplace=True)
    data['grow']    = ((data['close'] / data['close'].shift(1) - 1) * 100)
    volmCnt         = (data['amount'].tail(3) >= 200000000).astype(int).sum()
    data            = procMaData(data)
    close           = list(map(lambda x : round(x, 2), data['close'].tolist()))
    zhongshu        = getZhongShuList(code, name, data, testFlag)
    
    if testFlag:
        print (data['close'].iloc[-1], data['close'].iloc[-2], data['close'].iloc[-21:].max(), data['close'].iloc[-22:-1].max())
        
    if  data['close'].iloc[-1] == data['close'].iloc[-21:].max() \
    and data['close'].iloc[-2] == data['close'].iloc[-22:-1].max():
        if  (data['amount'].iloc[-1] >= 400000000 or data['grow'].iloc[-1] >= 9)\
        and (data['amount'].iloc[-2] >= 400000000 or data['grow'].iloc[-2] >= 9):
            if testFlag:
                print (data['grow'].iloc[-1], data['amount'].iloc[-1], zhongshu[-1][1], zhongshu[-1][2])
            validFlag   = True

    #if  data['close'].iloc[-2] == data['close'].iloc[-22:-2].max():
    #    if (data['grow'].iloc[-1] >= 1.05 and data['amount'].iloc[-1] >= 200000000)\
    #    or  data['grow'].iloc[-1] >= 1.09 :
    #        validFlag   = True
            
    if  validFlag \
    and close[-1] >= data['ma_5'].iloc[-1]\
    and data['ma_5'].iloc[-1] >= data['ma_10'].iloc[-1]\
    and data['ma_10'].iloc[-1] >= data['ma_20'].iloc[-1]\
    and close[-1] < 25\
    and True:
        findFlag= True
        result      = [findFlag, code, name, close[-1], close[-1], close[-1], close[-1], close[-1], close[-1], close[-1], "3th buy"]
        return result
    return findFlag, code, name



####################################################################
#####################old and uesless code here######################
####################################################################
def moveDuplicate(tol):    
    pro = []; pro.append(tol[0])
    for i in range(1, len(tol)) :
        idx0 = pro[-1][0]; istop0 = pro[-1][1]; low0 = pro[-1][2]; high0 = pro[-1][3]
        idx1 = tol[i][0];  istop1 = tol[i][1];  low1 = tol[i][2];  high1 = tol[i][3]
        if istop0 == istop1 :
            pro.pop()
            pro.append(tol[i])
        else:
            if idx1 - idx0 >= 3:
                #if (istop0 == 0 and low1 > high0) or (istop0 == 1 and high1 < low0):
                pro.append(tol[i])
    return pro
    
def getPeak(code, name, df, testFlag):
    findFlag        = False   
    close           = list(map(lambda x : round(x, 2), df['close'].tolist()))
    high            = list(map(lambda x : round(x, 2), df['high' ].tolist()))  
    low             = list(map(lambda x : round(x, 2), df['low'  ].tolist()))       
    hdiff           = np.diff(np.sign(np.diff(high)))
    ldiff           = np.diff(np.sign(np.diff(low)))
    peak            = (np.where(hdiff == -2)[0] + 1).tolist()
    poke            = (np.where(ldiff ==  2)[0] + 1).tolist() 
    top = []; bot = []; tol = []; pro = []; px = []; py = []
    for x in peak:
        lmin        = getListValue(x, True,  low, high)
        top.append([x, 1, lmin, high[x]])
    for x in poke:
        hmax        = getListValue(x, False, low, high)
        bot.append([x, 0, low[x], hmax])
    tol             = top + bot
    tol.sort(reverse=False) 
    if not tol:
        print (code, name, close)
        return findFlag, code, name
    
    pro             = moveDuplicate(tol)
    dt              = pd.DataFrame(pro)
    dt.columns      = ['idx', 'ispeak', 'low', 'high']
    #print (dt)
    for x,y,k,j in pro:
        px.append(x)
        if y == 0:
            py.append(k)
        else:
            py.append(j)
    
    if testFlag:
        print("%s :: line %3d : debug with code = %s, lens = %d peak = %d, poke = %d"\
        %("drawIm", sys._getframe().f_lineno, code, len(df), len(peak), len(poke)))  
        #print ("peak = ", peak)
        #print ("poke = ", poke)
        print("%s :: line %3d : debug with code = %s, pro = %s"\
        %("drawIm", sys._getframe().f_lineno, code, pro)) 
    
    #中枢开始：连续3条次级别走势重叠
    #中枢破坏：次级别走势离开中枢后，其后的次级别走势不回中枢
    zs = []; lens = len(pro); i = 3; findzs = True; result = []
    while i < lens :
        val0 = pro[i-3][3] if pro[i-3][1] else pro[i-3][2]
        val1 = pro[i-2][3] if pro[i-3][1] else pro[i-2][2]
        val2 = pro[i-1][3] if pro[i-3][1] else pro[i-1][2]
        val3 = pro[i-0][3] if pro[i-3][1] else pro[i-0][2]
        idx0 = pro[i-3][0]
        idx3 = pro[i-0][0]
        if findzs: 
            if (val2 >= val0 and val2 <= val1) or (val3 >= val0 and val3 <= val1) \
            or (val2 <= val0 and val2 <= val1 and  val3 >= val0 and val3 >= val1) : #上涨中枢
                zsh = min(val1, val3); zsl = max(val0, val2); findzs = False
                zs.append([zsl, zsh, idx0, idx3])
                #print ("test10 i = %d, %s, %s make a zhongshu %s" %(i, [i-3, i-2, i-1, i-0], [val0, val1, val2, val3], [zsl, zsh]))
            elif (val2 <= val0 and val2 >= val1) or (val3 <= val0 and val3 >= val1) \
            or   (val2 >= val0 and val2 >= val1 and  val3 <= val0 and val3 <= val1) : #下跌中枢
                zsh = min(val0, val2); zsl = max(val1, val3); findzs = False
                zs.append([zsl, zsh, idx0, idx3])
                #print ("test11 idx = %d, %s, %s make a zhongshu %s" %(i, [i-3, i-2, i-1, i-0], [val0, val1, val2, val3], [zsl, zsh]))
            else:
                result  = []
                #print ("test12 : i = %d, %s, %s not zs" %(i, [i-3, i-2, i-1, i-0], [val0, val1, val2, val3]))
            i           = i + 1
        else:
            val3 = pro[i-1][2]; 
            val4 = pro[i-0][2]; istop = pro[i-0][1]; idx = pro[i-0][0]
            #val5 = pro[i+1][2] if i < lens - 1 else close[-1]
            if val4 >= zs[-1][1] and istop == 0: #低点高于中枢，第3类买点
                #grow        = round(val5/val4, 2)
                findzs      = True
                i           = i + 2
                #print ("test20 : i = %d, 3th buy = %0.2f, %s" %(i, val4, [val3, val4, val5], zs[-1]))
            elif val4 < zs[-1][0] and istop == 1: #高点低于中枢，第3类卖点
                #val6        = pro[i+2][2] if i < lens - 2 else close[-1] 
                #grow        = round(val6/val5, 2)
                findzs      = True
                i           = i + 3
                #print ("test21 : i = %d, 3th sell = %0.2f, %s" %(i, val4, [val3, val4, val5], zs[-1]))
            else: #低点或高点在中枢内，中枢扩展 
                zs[-1][3]   = idx
                i           = i + 1 
                #print ("test21 : i = %d, %0.2f, %s 中枢扩展" %(i, val4, [val3, val4, val5], zs[-1]))
    if testFlag and zs:
        print("%s :: line %3d : debug with code = %s, last peak/poke = %s, %s, last zs = %s, lens = %d, close = %0.2f"\
        %("drawIm", sys._getframe().f_lineno, code, pro[-2], pro[-1], zs[-1], len(df), close[-1])) 
                
    plotFlag                = 1
    plotEn                  = True if plotFlag else False
    if plotEn and testFlag :
        plt.figure(figsize = (15, 10))
        ax1                 = plt.subplot(211)
        ax2                 = plt.subplot(212)
        plt.sca(ax1)
        plt.plot(px, py)
        #for x, y in thdBuy:
        #    plt.text(x,y,str(y)+str("_3th"),fontsize=10,verticalalignment="bottom",horizontalalignment="center")
        #for x, y in fstBuy:
        #    plt.text(x,y,str(y)+str("_1st"),fontsize=10,verticalalignment="bottom",horizontalalignment="center")
        for x,y,k,j in pro:
            if y == 0:
                plt.text(x,k,k,fontsize=10,verticalalignment="bottom",horizontalalignment="center")
            else:
                plt.text(x,j,j,fontsize=10,verticalalignment="bottom",horizontalalignment="center")
        
        plt.sca(ax2)
        plt.plot(close)
        for y0, y1, x0, x1 in zs:
            plt.text(x0,y0,str(y0),fontsize=10,verticalalignment="bottom",horizontalalignment="center")
            plt.text(x0,y1,str(y1),fontsize=10,verticalalignment="bottom",horizontalalignment="center")
            plt.plot([x0, x1], [y0, y0], color='r', linewidth=0.5)
            plt.plot([x0, x1], [y1, y1], color='r', linewidth=0.5)
            plt.plot([x0, x0], [y0, y1], color='r', linewidth=0.5)
            plt.plot([x1, x1], [y0, y1], color='r', linewidth=0.5)    
            
    if zs :
        findFlag    = True
        return findFlag, code, name, zs, pro
    else:
        return findFlag, code, name       


######################################################################
################## draw a stock image of grawfast ####################
######################################################################
def getThirdBuy(code, name, df, testFlag):
    findFlag        = False   
    close           = list(map(lambda x : round(x, 2), df['close'].tolist()))
    high            = list(map(lambda x : round(x, 2), df['high' ].tolist()))  
    low             = list(map(lambda x : round(x, 2), df['low'  ].tolist()))       
    hdiff           = np.diff(np.sign(np.diff(high)))
    ldiff           = np.diff(np.sign(np.diff(low)))
    peak            = (np.where(hdiff == -2)[0] + 1).tolist()
    poke            = (np.where(ldiff ==  2)[0] + 1).tolist()
    if not peak or not poke:
        return findFlag, code, name
    
    top = []; bot = []; tol = []; pro = []; px = []; py = []
    for x in peak:
        top.append([x, 1, low[x], high[x]])
    for x in poke:
        bot.append([x, 0, low[x], high[x]])
    tol             = top + bot
    tol.sort(reverse=False)
    pro.append(tol[0])
    for i in range(1, len(tol)) :
        idx0 = pro[-1][0]; istop0 = pro[-1][1]; low0 = pro[-1][2]; high0 = pro[-1][3]
        idx1 = tol[i][0];  istop1 = tol[i][1];  low1 = tol[i][2];  high1 = tol[i][3]
        if istop0 != istop1 :
            if idx1 - idx0 >= 3:
                #print (istop0, low0, high0, low1, high1)
                if (istop0 == 0 and low1 > high0) or (istop0 == 1 and high1 < low0):
                    pro.append(tol[i])
        else:
            if (istop0 == 0 and low1 <= low0) or (istop0 == 1 and high1 >= high0):
                pro.pop()
                pro.append(tol[i])
    for x,y,k,j in pro:
        px.append(x)
        if y == 0:
            py.append(k)
        else:
            py.append(j)
    
    if testFlag:
        print(code, len(df), len(peak), len(poke))
        #print ("peak = ", peak)
        #print ("poke = ", poke)
        #print (tol)
        print (pro)
    
    #中枢开始：连续3条次级别走势重叠
    #中枢破坏：次级别走势离开中枢后，其后的次级别走势不回中枢
    zs = []; lens = len(pro); i = 3; findzs = True; result = []
    while i < lens :
        val0 = pro[i-3][3] if pro[i-3][1] else pro[i-3][2]
        val1 = pro[i-2][3] if pro[i-3][1] else pro[i-2][2]
        val2 = pro[i-1][3] if pro[i-3][1] else pro[i-1][2]
        val3 = pro[i-0][3] if pro[i-3][1] else pro[i-0][2]
        idx0 = pro[i-3][0]
        idx3 = pro[i-0][0]
        if findzs: 
            if (val2 >= val0 and val2 <= val1) or (val3 >= val0 and val3 <= val1) \
            or (val2 <= val0 and val2 <= val1 and  val3 >= val0 and val3 >= val1) : #上涨中枢
                zsh = min(val1, val3); zsl = max(val0, val2); findzs = False
                zs.append([zsl, zsh, idx0, idx3])
                #print ("test10 i = %d, %s, %s make a zhongshu %s" %(i, [i-3, i-2, i-1, i-0], [val0, val1, val2, val3], [zsl, zsh]))
            elif (val2 <= val0 and val2 >= val1) or (val3 <= val0 and val3 >= val1) \
            or   (val2 >= val0 and val2 >= val1 and  val3 <= val0 and val3 <= val1) : #下跌中枢
                zsh = min(val0, val2); zsl = max(val1, val3); findzs = False
                zs.append([zsl, zsh, idx0, idx3])
                #print ("test11 idx = %d, %s, %s make a zhongshu %s" %(i, [i-3, i-2, i-1, i-0], [val0, val1, val2, val3], [zsl, zsh]))
            else:
                result  = []
                #print ("test12 : i = %d, %s, %s not zs" %(i, [i-3, i-2, i-1, i-0], [val0, val1, val2, val3]))
            i           = i + 1
        else:
            val3 = pro[i-1][2]; 
            val4 = pro[i-0][2]; istop = pro[i-0][1]; idx = pro[i-0][0]
            #val5 = pro[i+1][2] if i < lens - 1 else close[-1]
            if val4 >= zs[-1][1] and istop == 0: #低点高于中枢，第3类买点
                #grow        = round(val5/val4, 2)
                findzs      = True
                i           = i + 2
                #print ("test20 : i = %d, 3th buy = %0.2f, %s" %(i, val4, [val3, val4, val5], zs[-1]))
            elif val4 < zs[-1][0] and istop == 1: #高点低于中枢，第3类卖点
                #val6        = pro[i+2][2] if i < lens - 2 else close[-1] 
                #grow        = round(val6/val5, 2)
                findzs      = True
                i           = i + 3
                #print ("test21 : i = %d, 3th sell = %0.2f, %s" %(i, val4, [val3, val4, val5], zs[-1]))
            else: #低点或高点在中枢内，中枢扩展 
                zs[-1][3]   = idx
                i           = i + 1 
                #print ("test21 : i = %d, %0.2f, %s 中枢扩展" %(i, val4, [val3, val4, val5], zs[-1]))
    if testFlag:
        print ("last peak/poke = %s, %s, last zs = %s, lens = %d, close = %0.2f" %(pro[-2], pro[-1], zs[-1], len(df), close[-1]))
    
    
    plotFlag                = 1
    plotEn                  = True if plotFlag else False
    if plotEn and testFlag :
        plt.figure(figsize = (6, 4))
        ax1                 = plt.subplot(211)
        ax2                 = plt.subplot(212)
        plt.sca(ax1)
        plt.plot(px, py)
        #for x, y in thdBuy:
        #    plt.text(x,y,str(y)+str("_3th"),fontsize=10,verticalalignment="bottom",horizontalalignment="center")
        #for x, y in fstBuy:
        #    plt.text(x,y,str(y)+str("_1st"),fontsize=10,verticalalignment="bottom",horizontalalignment="center")
        for x,y,k,j in pro:
            if y == 0:
                plt.text(x,k,k,fontsize=10,verticalalignment="bottom",horizontalalignment="center")
            else:
                plt.text(x,j,j,fontsize=10,verticalalignment="bottom",horizontalalignment="center")
        
        plt.sca(ax2)
        plt.plot(close)
        for y0, y1, x0, x1 in zs:
            plt.text(x0,y0,str(y0),fontsize=10,verticalalignment="bottom",horizontalalignment="center")
            plt.text(x0,y1,str(y1),fontsize=10,verticalalignment="bottom",horizontalalignment="center")
            plt.plot([x0, x1], [y0, y0], color='r', linewidth=0.5)
            plt.plot([x0, x1], [y1, y1], color='r', linewidth=0.5)
            plt.plot([x0, x0], [y0, y1], color='r', linewidth=0.5)
            plt.plot([x1, x1], [y0, y1], color='r', linewidth=0.5)
    
    
    if not zs or not pro :   
        return findFlag, code, name 
    
    zsdf                = pd.DataFrame(zs)
    zsdf.columns        = ['zsl', 'zsh', 'sta', 'end']
    zshigh              = list(map(lambda x : round(x, 2), zsdf['zsh'].tolist()))  
    zslow               = list(map(lambda x : round(x, 2), zsdf['zsl'].tolist()))       
    hdiff               = np.diff(np.sign(np.diff(zshigh)))
    ldiff               = np.diff(np.sign(np.diff(zslow)))
    hpeak               = (np.where(hdiff == -2)[0] + 1).tolist()
    hpoke               = (np.where(ldiff ==  2)[0] + 1).tolist()
    hval = []; lval = []; init = []
    for x in hpeak:
        hval.append([x, 1, zshigh[x]])
    for x in hpoke:
        lval.append([x, 0, zslow[x]])
    init                = hval + lval
    init.sort(reverse=False)
    #print (init)
    #print (zsdf)

    for i in range(2, len(zs)) :
        if i >= 3:
             zsh0 = zs[i-2][1]; zsh1 = zs[i-1][1]; zsh2 = zs[i-0][1];
             zsh3 = zs[i+1][1] if i < len(zs)-1 else close[-1]
             minzsh     = min(zsh0, zsh1, zsh2)
             if minzsh == zsh1 and i == len(zs) - 1 and zsh2 >= zsh0 and close[-1] >= zsh2:
                 #findFlag   = True
                 #print (i-1, [zsh0, zsh1, zsh2], minzsh, zsh3)
                 return findFlag, code, name, zsh0, zsh1, zsh2, minzsh, zsh3, close[-1], '中枢低点'
    #return findFlag, code, name
    
    if pro and zs and pro[-1][1] == 1 :
        idx = pro[-1][0]; low = pro[-1][2]; high = pro[-1][3]; zsl = zs[-1][0]; zsh = zs[-1][1]
        if (close[-2] <= high and close[-1] >= high and df['low'].iloc[idx:].min()/zsh >= 0.99):
            findFlag    = True
            days        = len(df) - idx
            grow        = round(close[-1]/high, 2)
            #zslow, zslow, last peak, last close, grow, days
            result      = [findFlag, code, name, zsl, zsh, high, close[-1], grow, days, '日线上涨']           
        elif (close[-1] <= high and close[-1]/zsh >= 0.99):
            #findFlag    = True
            days        = len(df) - idx
            grow        = round(close[-1]/high, 2)
            #zslow, zslow, last peak, last close, grow, days
            result      = [findFlag, code, name, zsl, zsh, high, close[-1], grow, days, '等待3买']  
    
    if pro and zs and pro[-1][1] == 0 :
        idx = pro[-1][0]; low = pro[-1][2]; high = pro[-1][3]; zsl = zs[-1][0]; zsh = zs[-1][1]
        if (low/zsh >= 0.99 and close[-1] >= low) :
            #findFlag    = True
            days        = len(df) - idx
            grow        = round(close[-1]/low, 2)
            #zslow, zslow, last peak, last close, grow, days
            result      = [findFlag, code, name, zsl, zsh, low, close[-1], grow, days, '形成3买']   
            
    if findFlag:
        return result
    else:
        return findFlag, code, name       



######################################################################
################## draw a stock image of grawfast ####################
######################################################################
def getZhongShu(code, name, df, testFlag):
    findFlag        = False   
    close           = list(map(lambda x : round(x, 2), df['close'].tolist()))
    high            = list(map(lambda x : round(x, 2), df['high' ].tolist()))  
    low             = list(map(lambda x : round(x, 2), df['low'  ].tolist()))       
    hdiff           = np.diff(np.sign(np.diff(high)))
    ldiff           = np.diff(np.sign(np.diff(low)))
    peak            = (np.where(hdiff == -2)[0] + 1).tolist()
    poke            = (np.where(ldiff ==  2)[0] + 1).tolist()
    if not peak or not poke:
        return findFlag, code, name
    
    top = []; bot = []; tol = []; pro = []; px = []; py = []
    for x in peak:
        top.append([x, 1, low[x], high[x]])
    for x in poke:
        bot.append([x, 0, low[x], high[x]])
    tol             = top + bot
    tol.sort(reverse=False)
    pro.append(tol[0])
    for i in range(1, len(tol)) :
        idx0 = pro[-1][0]; istop0 = pro[-1][1]; low0 = pro[-1][2]; high0 = pro[-1][3]
        idx1 = tol[i][0];  istop1 = tol[i][1];  low1 = tol[i][2];  high1 = tol[i][3]
        if istop0 != istop1 :
            if idx1 - idx0 >= 3:
                #print (istop0, low0, high0, low1, high1)
                if (istop0 == 0 and low1 > high0) or (istop0 == 1 and high1 < low0):
                    pro.append(tol[i])
        else:
            if (istop0 == 0 and low1 <= low0) or (istop0 == 1 and high1 >= high0):
                pro.pop()
                pro.append(tol[i])
    for x,y,k,j in pro:
        px.append(x)
        if y == 0:
            py.append(k)
        else:
            py.append(j)
    
    if testFlag:
        print("%s :: line %3d : debug with code = %s, lens = %d peak = %d, poke = %d"\
        %("drawIm", sys._getframe().f_lineno, code, len(df), len(peak), len(poke)))  
        #print ("peak = ", peak)
        #print ("poke = ", poke)
        print("%s :: line %3d : debug with code = %s, pro = %s"\
        %("drawIm", sys._getframe().f_lineno, code, pro)) 
    
    #中枢开始：连续3条次级别走势重叠
    #中枢破坏：次级别走势离开中枢后，其后的次级别走势不回中枢
    zs = []; lens = len(pro); i = 3; findzs = True; result = []
    while i < lens :
        val0 = pro[i-3][3] if pro[i-3][1] else pro[i-3][2]
        val1 = pro[i-2][3] if pro[i-3][1] else pro[i-2][2]
        val2 = pro[i-1][3] if pro[i-3][1] else pro[i-1][2]
        val3 = pro[i-0][3] if pro[i-3][1] else pro[i-0][2]
        idx0 = pro[i-3][0]
        idx3 = pro[i-0][0]
        if findzs: 
            if (val2 >= val0 and val2 <= val1) or (val3 >= val0 and val3 <= val1) \
            or (val2 <= val0 and val2 <= val1 and  val3 >= val0 and val3 >= val1) : #上涨中枢
                zsh = min(val1, val3); zsl = max(val0, val2); findzs = False
                zs.append([zsl, zsh, idx0, idx3])
                #print ("test10 i = %d, %s, %s make a zhongshu %s" %(i, [i-3, i-2, i-1, i-0], [val0, val1, val2, val3], [zsl, zsh]))
            elif (val2 <= val0 and val2 >= val1) or (val3 <= val0 and val3 >= val1) \
            or   (val2 >= val0 and val2 >= val1 and  val3 <= val0 and val3 <= val1) : #下跌中枢
                zsh = min(val0, val2); zsl = max(val1, val3); findzs = False
                zs.append([zsl, zsh, idx0, idx3])
                #print ("test11 idx = %d, %s, %s make a zhongshu %s" %(i, [i-3, i-2, i-1, i-0], [val0, val1, val2, val3], [zsl, zsh]))
            else:
                result  = []
                #print ("test12 : i = %d, %s, %s not zs" %(i, [i-3, i-2, i-1, i-0], [val0, val1, val2, val3]))
            i           = i + 1
        else:
            val3 = pro[i-1][2]; 
            val4 = pro[i-0][2]; istop = pro[i-0][1]; idx = pro[i-0][0]
            #val5 = pro[i+1][2] if i < lens - 1 else close[-1]
            if val4 >= zs[-1][1] and istop == 0: #低点高于中枢，第3类买点
                #grow        = round(val5/val4, 2)
                findzs      = True
                i           = i + 2
                #print ("test20 : i = %d, 3th buy = %0.2f, %s" %(i, val4, [val3, val4, val5], zs[-1]))
            elif val4 < zs[-1][0] and istop == 1: #高点低于中枢，第3类卖点
                #val6        = pro[i+2][2] if i < lens - 2 else close[-1] 
                #grow        = round(val6/val5, 2)
                findzs      = True
                i           = i + 3
                #print ("test21 : i = %d, 3th sell = %0.2f, %s" %(i, val4, [val3, val4, val5], zs[-1]))
            else: #低点或高点在中枢内，中枢扩展 
                zs[-1][3]   = idx
                i           = i + 1 
                #print ("test21 : i = %d, %0.2f, %s 中枢扩展" %(i, val4, [val3, val4, val5], zs[-1]))
    if testFlag and zs:
        print("%s :: line %3d : debug with code = %s, last peak/poke = %s, %s, last zs = %s, lens = %d, close = %0.2f"\
        %("drawIm", sys._getframe().f_lineno, code, pro[-2], pro[-1], zs[-1], len(df), close[-1])) 
                
    plotFlag                = 1
    plotEn                  = True if plotFlag else False
    if plotEn and testFlag :
        plt.figure(figsize = (15, 10))
        ax1                 = plt.subplot(211)
        ax2                 = plt.subplot(212)
        plt.sca(ax1)
        plt.plot(px, py)
        #for x, y in thdBuy:
        #    plt.text(x,y,str(y)+str("_3th"),fontsize=10,verticalalignment="bottom",horizontalalignment="center")
        #for x, y in fstBuy:
        #    plt.text(x,y,str(y)+str("_1st"),fontsize=10,verticalalignment="bottom",horizontalalignment="center")
        for x,y,k,j in pro:
            if y == 0:
                plt.text(x,k,k,fontsize=10,verticalalignment="bottom",horizontalalignment="center")
            else:
                plt.text(x,j,j,fontsize=10,verticalalignment="bottom",horizontalalignment="center")
        
        plt.sca(ax2)
        plt.plot(close)
        for y0, y1, x0, x1 in zs:
            plt.text(x0,y0,str(y0),fontsize=10,verticalalignment="bottom",horizontalalignment="center")
            plt.text(x0,y1,str(y1),fontsize=10,verticalalignment="bottom",horizontalalignment="center")
            plt.plot([x0, x1], [y0, y0], color='r', linewidth=0.5)
            plt.plot([x0, x1], [y1, y1], color='r', linewidth=0.5)
            plt.plot([x0, x0], [y0, y1], color='r', linewidth=0.5)
            plt.plot([x1, x1], [y0, y1], color='r', linewidth=0.5)    
            
    if zs :
        findFlag    = True
        return findFlag, code, name, zs, pro
    else:
        return findFlag, code, name       

######################################################################
################## draw a stock image of weekcenter ##################
######################################################################
def findWeekCenter(code, name, ddf, wdf, np, testFlag) :
    findFlag        = False
    dayslen         = len(ddf)
    weeklen         = len(wdf)
    
    if True:       
        if testFlag:
            print("%s :: line : %d : dayslen = %d, weeklen = %d" \
            %(sys._getframe().f_code.co_name,  sys._getframe().f_lineno, dayslen, weeklen))
            #print (wdf)
        
        results             = ""
        for i in range(weeklen-2, weeklen):
            w0high          = wdf['high'].iloc[i-2]
            w1high          = wdf['high'].iloc[i-1]
            #w2high          = wdf['high'].iloc[i-0]
            #w0low           = wdf['low' ].iloc[i-2] 
            #w1low           = wdf['low' ].iloc[i-1] 
            w2low           = wdf['low' ].iloc[i-0] 
            zshigh          = min(wdf['high'].iloc[i-2:i+1].tolist())
            zslow           = max(wdf['low' ].iloc[i-2:i+1].tolist())
            close           = wdf['close'].iloc[i]
            gap             = round(float(close)/float(zshigh), 2)
            dif             = round(float(wdf['dif'].iloc[i]), 2)
            dea             = round(wdf['dea'].iloc[i], 2)
            macd            = round(ddf['macd'].iloc[i],2)
            if testFlag :
                print("%s :: line : %d : i = %d, highlist = %s" \
                %(sys._getframe().f_code.co_name,  sys._getframe().f_lineno, i, wdf['high'].iloc[i-2:i+1].tolist()))
                print("%s :: line : %d : i = %d, highlist = %s" \
                %(sys._getframe().f_code.co_name,  sys._getframe().f_lineno, i, wdf['low' ].iloc[i-2:i+1].tolist()))
                print("%s :: line : %d : wlow = %0.2f, whigh = %0.2f, w2cls = %.2f, gap = %0.2f, dif = %0.2f" \
                %(sys._getframe().f_code.co_name,  sys._getframe().f_lineno, zslow, zshigh, close, gap, dif))
            
            #最近3周没有形成周中枢
            if zslow > zshigh :
                #case0 : 3根周线都不重叠
                if w2low > w0high and w2low > w1high :
                    findFlag    = True
                    results     = [findFlag, code, name, zshigh, close, gap, dif, dea, macd, 'case0']
                #case1 : 第一和第三周重叠，周二和周三不重叠
                elif w2low > w1high :
                    findFlag    = True
                    results     = [findFlag, code, name, zshigh, close, gap, dif, dea, macd, 'case1']
                #case2 : 第二和第三周重叠，周一和周三不重叠
                elif w2low > w0high :
                    findFlag    = True
                    results     = [findFlag, code, name, zshigh, close, gap, dif, dea, macd, 'case2']            
                if testFlag :
                    print("%s :: line : %d : findFlag = %d, result = %s" \
                    %(sys._getframe().f_code.co_name,  sys._getframe().f_lineno, findFlag, results))
                
                #找最近的日中枢并突破日中枢
                if findFlag :
                    for j in range(dayslen, dayslen-5, -1):                 
                        dayhigh         = min(ddf['high'].iloc[j-3:j+1].tolist())
                        daylow          = max(ddf['low' ].iloc[j-3:j+1].tolist())
                        dayclose        = ddf['close'].iloc[-1]
                        grow            = round(dayclose/dayhigh, 2)
                        if testFlag:      
                            print (j, ddf.iloc[j-3:j])
                            print (j, dayhigh, ddf['high'].iloc[j-3:j+1].tolist())
                            print (j, daylow,  ddf['low' ].iloc[j-3:j+1].tolist())
                            print (daylow, dayhigh, dayclose)
                        if daylow <= dayhigh :
                            if grow > growGap :
                                results[3]      = dayhigh
                                results[4]      = dayclose
                                results[5]      = grow
                                return results
                            else :
                                #print (dayclose, "close低于日中枢", daylow, dayhigh, results)
                                return False, code, name
                    #print (dayclose, "最近5天没有日中枢，继续关注", daylow, dayhigh, results)
                    return results
            #最近3周形成中枢
            else :
                #突破周中枢后回调不踩中枢
                if gap > growGap :
                    for j in range(dayslen-5, dayslen):
                        if ddf['close'].iloc[j-1] < zshigh and ddf['close'].iloc[j] >= zshigh:
                            #print (ddf.iloc[j-1:])
                            #print (code,  ddf['close'].iloc[j:].min(), zslow, zshigh)
                            #print ("low", ddf['low'  ].iloc[j:].tolist())
                            #print ("cls", ddf['close'].iloc[j:].tolist())                            
                            #minlow      = ddf['low'  ].iloc[j:].min()
                            mincls      = ddf['close'].iloc[j:].min()
                            minhigh     = ddf['high' ].iloc[j:].min()
                            endcls      = ddf['close'].iloc[-1]
                            grow        = round(endcls/zshigh, 2)
                            #print ("minlow", minlow,"mincls", mincls,"minhigh", minhigh,"endcls", endcls,) 
                            #sys.exit(0)
                            if  mincls >= zshigh and endcls >= minhigh :
                                #findFlag    = True
                                results     = [findFlag, code, name, zshigh, endcls, grow, dif, dea, macd, 'case3']
                                return results
                                #print ("min = %0.2f, close = %0.2f, gap = %0.2f" %(ddf['close'].iloc[j:].min(), ddf['close'].iloc[-1], grow))
                                                         
    return False, code, name
    

    
def getFirstBuy (code, data, np, testFlag) :
    findFlag        = False
    lens            = len(data)
    
    df              = data.tail(np)
    df.reset_index(inplace=True)
    minIdx          = df['low'].idxmin()
    minVal          = df['low'].min()
    lastCls         = 99999 
    gap             = 0
    
    if False:        
        #data['plus']  = data['dif'].round(decimals=2) - data['dea'].round(decimals=2)
        data['plus']  = data['dif'].round(decimals=2) - 0
        data['sum']   = data['plus'].round(decimals=2) + data['macd'].round(decimals=2)
        data['rst']   = (data['plus'] - data['sum']) ** 2 + (data['macd'] - data['sum']) ** 2
        data['rst']   = data['rst'] * 10000
        if  data['rst'].iloc[-1] > 0 \
        and data['close'].iloc[-1] >= data['ma_5'].iloc[-1]  \
        and data['ma_10'].iloc[-1] >= data['ma_20'].iloc[-1] \
        and data['ma_20'].iloc[-1] >= data['ma_60'].iloc[-1] \
        and data['plus'].iloc[-2] >= -0.05 \
        and data['plus'].iloc[-2] <= 0.05 \
        and data['macd'].iloc[-2] >= -0.05 \
        and data['macd'].iloc[-2] <= 0.05 \
        and data['dlt'].iloc[-1]  > 0:
            bef         = data['rst'].iloc[-2]
            now         = data['rst'].iloc[-1]
            if testFlag:
                del data['open'], data['high'], data['low']
                del data['ma_5'], data['ma_10'], data['ma_20'], data['ma_60'], data['ma_250']
                del data['md_20'], data['up_20'], data['dw_20']
                del data['sema'], data['lema'], data['std_20']
                pd.set_option('display.max_colwidth',1000)
                drawExample(code, data)
                print(data.tail(10))
            findFlag    = True
            return findFlag, bef, now, bef, now, bef, now
    
    if False:
        for i in range(lens-5, lens-1):
            if testFlag:
                print(i, data['dif'].iloc[i], data['dea'].iloc[i], data['macd'].iloc[i])
            if  data['dif'].iloc[i]   >= data['dea'].iloc[i] \
            and data['macd'].iloc[i]  >= -0.1  \
            and data['macd'].iloc[i]  <=  0.1  \
            and data['dif'].iloc[i]   >= -0.07 \
            and data['close'].iloc[-1]>  data['ma_250'].iloc[-1] \
            and data['macd'].iloc[-1] >  0.05   :            
            #and data['dif'].iloc[i]   >= data['dea'].iloc[i]   \
            #if  data['dif'].iloc[i-1] <= data['dea'].iloc[i-1] \
            #and data['dif'].iloc[i]   >= data['dea'].iloc[i]   \
            #and data['dif'].iloc[i]   > 0 \
            #and data['dea'].iloc[i]   > 0 :
                close       = data['close'].iloc[i]
                gap         = round(data['close'].iloc[-1]/data['close'].iloc[i], 2)
                findFlag    = True
                return findFlag, i, close, lens-i, data['close'].iloc[-1], gap, data['macd'].iloc[i]
                      
    if False:    
        for i in range(lens-5, lens-1):
            #print(i, lens, data['low'].iloc[i], data['close'].iloc[i])
            if  data['high'].iloc[i-1] > data['high'].iloc[i] \
            and data['low'].iloc[i-1]  > data['low'].iloc[i]  \
            and data['high'].iloc[i+1] > data['high'].iloc[i] \
            and data['low'].iloc[i+1]  > data['low'].iloc[i]  \
            and data['close'].iloc[-1] >= data['high'].iloc[i-1]\
            and data['dif'].iloc[i+1]  >= diffLevel \
            and data['dif'].iloc[i+1]  <= diffMax   \
            and data['macd'].iloc[i+1] >= macdLevel \
            and data['macd'].iloc[i+1] <= macdMax   :
                findFlag    = True
                return findFlag, i, data['low'].iloc[i], lens-i, round(data['close'].iloc[-1]/data['close'].iloc[i], 2), data['dif'].iloc[i+1], data['macd'].iloc[i+1]
                    
        #print (minVal, curMinVal, curMinVal/minVal, lastCls, df['close'].iloc[-1])
        if curMinVal < minVal or curMinVal / minVal < 1.05 :
            if  df['close'].iloc[-1] >= lastCls \
            and df['macd'].iloc[-1]  >= -0.15 \
            and df['dif'].iloc[-1]   >= df['dea'].iloc[-1] \
            and df['dif'].iloc[-1]   >= -0.15 :
                findFlag    = True
                return findFlag, minIdx, minVal, curMinVal, round(curMinVal/minVal, 2), lastCls, df['close'].iloc[-1]
    return findFlag, code


######################################################################
################## draw a stock image and save #######################
######################################################################
def drawExample(code, data):        
    plt.figure(figsize = (12, 9))
    ax1                 = plt.subplot(211)
    ax2                 = plt.subplot(212)
    
    plt.sca(ax1)
    plt.plot(data['close'].tolist(), 'r-')
    plt.sca(ax2)
    plt.plot(data['dlt'].tolist(), 'b-')    
    plt.show()
    plt.close('all')        

def draw(code, name, output, data, opType):      
    codename            = name.decode('utf-8')
    fileName            = str(code) + '_' + codename + '.png'
    ofile               = os.path.join(output, fileName)    
    MA1                 = 20
    MA2                 = 250 if len(data) >= 250 else 60  
    isDetailImage       = True
    
    if opType == 'growFast' :
        isDetailImage   = False
        plt.figure(figsize = (12, 10))
        plt.plot(data['close'].tolist(), 'r-')
        plt.plot(data['ma_5'].tolist(), 'g-') 
        plt.plot(data['ma_'+str(MA2)].tolist(), 'y-') 
        plt.savefig(ofile, dpi=75)
        plt.cla()
        plt.close('all')

    if isDetailImage:
        # drop the date index from the dateframe & make a copy
        daysreshape     = data.reset_index()
        # convert the datetime64 column in the dataframe to 'float days'
        daysreshape['DateTime'] = mdates.date2num(daysreshape['date'].astype(dt.date))
        # clean day data for candle view 
        #daysreshape.drop('Volume', axis=1, inplace = True)
        daysreshape             = daysreshape.reindex(columns=['DateTime','open','high','low','close'])  
    
        Av1             = data['ma_'+str(MA1)].tolist()
        Av2             = data['ma_'+str(MA2)].tolist()
        SP              = len(daysreshape.DateTime.values[0:])
                
        plt.figure(facecolor='#07000d',figsize=(15,10))    
        ax1             = plt.subplot2grid((6,4), (1,0), rowspan=4, colspan=4, axisbg='#07000d')
        candlestick_ohlc(ax1, daysreshape.values[-SP:], width=.6, colorup='#ff1717', colordown='#53c156')             
        Label1          = str(MA1)+' SMA'
        Label2          = str(MA2)+' SMA'
    
        ax1.plot(daysreshape.DateTime.values[-SP:],Av1[-SP:],'#e1edf9',label=Label1, linewidth=1.5)
        ax1.plot(daysreshape.DateTime.values[-SP:],Av2[-SP:],'#4ee6fd',label=Label2, linewidth=1.5)
        ax1.grid(True, color='w')
        ax1.xaxis.set_major_locator(mticker.MaxNLocator(10))
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax1.yaxis.label.set_color("w")
        ax1.spines['bottom'].set_color("#5998ff")
        ax1.spines['top'].set_color("#5998ff")
        ax1.spines['left'].set_color("#5998ff")
        ax1.spines['right'].set_color("#5998ff")
        ax1.tick_params(axis='y', colors='w')
        plt.gca().yaxis.set_major_locator(mticker.MaxNLocator(prune='upper'))
        ax1.tick_params(axis='x', colors='w')
        plt.ylabel('Stock price and Volume') 
                
        volumeMin       = 0
        ax1v            = ax1.twinx()
        ax1v.fill_between(daysreshape.DateTime.values[-SP:],volumeMin, data.volume.values[-SP:], facecolor='#00ffe8', alpha=.4)
        ax1v.axes.yaxis.set_ticklabels([])
        ax1v.grid(False)
        ###Edit this to 3, so it's a bit larger
        ax1v.set_ylim(0, 3*data.volume.values.max())
        ax1v.spines['bottom'].set_color("#5998ff")
        ax1v.spines['top'].set_color("#5998ff")
        ax1v.spines['left'].set_color("#5998ff")
        ax1v.spines['right'].set_color("#5998ff")
        ax1v.tick_params(axis='x', colors='w')
        ax1v.tick_params(axis='y', colors='w')
                
        ax2             = plt.subplot2grid((6,4), (5,0), sharex=ax1, rowspan=1, colspan=4, axisbg='#07000d')
        fillcolor       = '#00ffe8'
        #emaslow         = data['lema']
        #emafast         = data['sema']
        macd            = data['data_macd']
        ema9            = data['data_dea']
        ax2.plot(daysreshape.DateTime.values[-SP:], macd[-SP:], color='#4ee6fd', lw=2)
        ax2.plot(daysreshape.DateTime.values[-SP:], ema9[-SP:], color='#e1edf9', lw=1)
        ax2.fill_between(daysreshape.DateTime.values[-SP:], macd[-SP:]-ema9[-SP:], 0, alpha=0.5, facecolor=fillcolor, edgecolor=fillcolor)
        plt.gca().yaxis.set_major_locator(mticker.MaxNLocator(prune='upper'))
        ax2.spines['bottom'].set_color("#5998ff")
        ax2.spines['top'].set_color("#5998ff")
        ax2.spines['left'].set_color("#5998ff")
        ax2.spines['right'].set_color("#5998ff")
        ax2.tick_params(axis='x', colors='w')
        ax2.tick_params(axis='y', colors='w')
        plt.ylabel('MACD', color='w')
        ax2.yaxis.set_major_locator(mticker.MaxNLocator(nbins=5, prune='upper'))
        for label in ax2.xaxis.get_ticklabels():
            label.set_rotation(45)   
              
        if name == '':
            plt.show()
        else:
            #print ofile
            plt.savefig(ofile, dpi=150)
        plt.cla()
        plt.close('all')
######################################################################
        
        
        
        
        
        
        
        
        
def checkSimpleTurtle (code, df, ma) :
    findFlag    = False 
    length      = len(df)
    lastHigh    = df['close'].iloc[-2-20 : -2].max()
    lastHighIdx = df['close'].iloc[-2-20 : -2].idxmax()
    lowrLevel   = df['close'].iloc[lastHighIdx - 20 : lastHighIdx].min()
    #middLevel   = (lastHigh - lowrLevel)/2 + lowrLevel         
    #print length, lastHigh, lastHighIdx, lowrLevel

    if df['close'].iloc[-1] >= lastHigh \
   and df['close'].iloc[-2] <= lastHigh \
   and df['close'].iloc[-1] >= df['close'].iloc[-35 :-2].max() \
   and df['close'].iloc[-1] / df['close'].iloc[-2] > 1.03 \
   and df['close'].iloc[lastHighIdx] >= df['ma_'+str(ma)].iloc[lastHighIdx] \
   and length - lastHighIdx >= 5 \
   and df['close'].iloc[lastHighIdx : -2].min() > lowrLevel:
       #print "high =",lastHigh, "low =",lowrLevel, "close =",df['close'].iloc[-1], code, "\n"
       if df['ma_'+str(ma)].iloc[-1] > 0 and df['close'].iloc[-1] / df['ma_'+str(ma)].iloc[-1] < 1.15 :
           findFlag    = True
    #else:
        #print df['close'].iloc[-1] >= lastHigh        
        #print df['close'].iloc[-2] <= lastHigh
        #print df['close'].iloc[-1] >= df['close'].iloc[-35 :-2].max()         
        #print df['close'].iloc[-1] / df['close'].iloc[-2] > 1.03
        #print df['close'].iloc[lastHighIdx] >= df['ma_'+str(ma)].iloc[lastHighIdx]
        #print length - lastHighIdx >= 5
        #print df['close'].iloc[lastHighIdx : -2].min() > lowrLevel
        #print df['close'].iloc[-1] / df['ma_250'].iloc[-1] < 1.15
    return findFlag

def lastSimpleTurtle (code, df, ma) :
    findFlag    = False 
    length      = len(df)
    #print code, length
    for i in range (length - 11, length) :
        lastHigh    = df['close'].iloc[i-20 : i-1].max()
        lastHighIdx = df['close'].iloc[i-20 : i-1].idxmax()
        lowrLevel   = df['close'].iloc[lastHighIdx - 20 : lastHighIdx].min()
        #middLevel   = (lastHigh - lowrLevel)/2 + lowrLevel         
        #print i, length, lastHigh, lastHighIdx, lowrLevel

        if df['close'].iloc[i-0] >= lastHigh \
       and df['close'].iloc[i-1] <= lastHigh \
       and df['close'].iloc[i-0] >= df['close'].iloc[i-1-35 :i-1].max() \
       and df['close'].iloc[i-0] / df['close'].iloc[i-1] > 1.03 \
       and df['close'].iloc[lastHighIdx] >= df['ma_'+str(ma)].iloc[lastHighIdx] \
       and length - lastHighIdx >= 5 \
       and df['close'].iloc[lastHighIdx : i-1].min() > lowrLevel:
           #print "high =",lastHigh, "low =",lowrLevel, "close =",df['close'].iloc[-1], code, "\n"
           if df['ma_'+str(ma)].iloc[i] > 0 \
          and df['close'].iloc[i] / df['ma_'+str(ma)].iloc[i] < 1.15 \
          and df['volRate'].iloc[i] >= 5 \
          and length - i == 3\
          and df['close'].iloc[-1] > df['close'].iloc[i] :
               findFlag    = True
               return findFlag, length - i
        #else:
            #print df['close'].iloc[i-0] >= lastHigh        
            #print df['close'].iloc[i-1] <= lastHigh
            #print df['close'].iloc[i-0] >= df['close'].iloc[i-1-35 :i-1].max()      
            #print df['close'].iloc[i-0] / df['close'].iloc[i-1] > 1.03
            #print df['close'].iloc[lastHighIdx] >= df['ma_'+str(ma)].iloc[lastHighIdx]
            #print length - lastHighIdx >= 5
            #print df['close'].iloc[lastHighIdx : i-1].min() > lowrLevel
            #print df['close'].iloc[i] / df['ma_250'].iloc[i] < 1.15
    return findFlag, 99

def lastBreakTop (code, df, ma) :
    findFlag    = False 
    length      = len(df)
    #print code, length
    lastHigh    = df['close'].iloc[-5-20 : -5].max()
    lastHighIdx = df['close'].iloc[-5-20 : -5].idxmax()
    #print "loop0 : last highidx =",lastHighIdx,  lastHigh,  df['close'].iloc[-1]
       
    for i in range (lastHighIdx, length) :
        #print "loop1 : ", i, df['close'].iloc[i]
        if df['close'].iloc[i-1] <= lastHigh \
       and df['close'].iloc[i-0] >  lastHigh \
       and df['close'].iloc[i-0] / df['close'].iloc[i-1] > 1.03\
       and i != length - 1\
       and df['close'].iloc[i+1] / df['close'].iloc[i] > 1.03 \
       and df['close'].iloc[-1]  >= df['close'].iloc[-2] \
       and i - lastHighIdx >= 3 \
       and length-1-i <= 3:
            #print "loop2 : new highidx =",i, df['close'].iloc[i]            
            flag            = True 
            for j in range (lastHighIdx+1, i) :   
                #print "loop3 : ", j, df['close'].iloc[j]
                if df['close'].iloc[j] < df['ma_5'].iloc[j]:
                    flag    = False
                    break                        
            if flag:
                findFlag    = True
                #print "loop4 : ", lastHighIdx, lastHigh, i, df['close'].iloc[i], length-1-i, df['close'].iloc[-1]
                return findFlag,  lastHighIdx, lastHigh, i, df['close'].iloc[i], length-1-i, df['close'].iloc[-1]
    return findFlag, 99

def catchZhangTing (code, df, ma) :
    findFlag    = False 
    length      = len(df)
       
    for i in range (length-10, length-3) :
        if df['close'].iloc[i] / df['close'].iloc[i-1]  >= 1.07 \
       and df['close'].iloc[i+3] / df['close'].iloc[i+2] > 1.03 \
       and df['low'].iloc[i+1:i+3].min() <= df['high'].iloc[i]  \
       and df['close'].iloc[i+1:i+3].max() / df['close'].iloc[i] < 1.05 \
       and df['close'].iloc[i+3] ==  df['close'].iloc[i:i+4].max() \
       and df['close'].iloc[-1] >= df['close'].iloc[-2] \
       and length-i-4 == 2 :
            #print df['close'].iloc[i:i+4]
            findFlag    = True
            #print "loop4 : ", i, df['close'].iloc[i], i+3, df['close'].iloc[i+3], length-i-4, df['close'].iloc[-1]
            return findFlag,  i, df['close'].iloc[i], i+3, df['close'].iloc[i+3], length-i-4, df['close'].iloc[-1]
    return findFlag, 99
    
def continusUp (code, df, ma) :
    findFlag    = False 
    length      = len(df)
       
    for i in range (length-10, length) :
        if df['close'].iloc[i-2] / df['close'].iloc[i-3]  >= 1.05 \
       and df['close'].iloc[i-1] / df['close'].iloc[i-2]  <= 1.05 \
       and df['close'].iloc[i-2] / df['ma_250'].iloc[i-2] <= 1.25 \
       and df['close'].iloc[i-0] / df['close'].iloc[i-1]  >= 1.05 \
       and length-i-1 == 0:  
            findFlag    = True
            print("loop4 : ", i-2, df['close'].iloc[i-2], i, df['close'].iloc[i], length-i-1, df['close'].iloc[-1]/df['close'].iloc[i])
            return findFlag,  i-2, df['close'].iloc[i-2], i, df['close'].iloc[i], length-i-1, df['close'].iloc[-1]/df['close'].iloc[i]
    return findFlag, 99
    
def checkTurtle(df) :
    findFlag    = False 
    # -1 is the max; -2 is not the max and not a big up; -3 is not the max
    # -1 is a big up
    if  df['close'].iloc[-2] <  df['high'].iloc[-2-40 : -2].max() \
    and df['close'].iloc[-2] /  df['close'].iloc[-3] < 1.05 \
    and df['close'].iloc[-1] >= df['close'].iloc[-1-41 : -1].max() \
    and df['close'].iloc[-3] != df['close'].iloc[-1-41 : -2].max() \
    and df['close'].iloc[-1] /  df['close'].iloc[-2] > 1.09 :
    #and df['close'].iloc[-1] /  df['close'].min() < 1.3 :
        findFlag= True
    #print df['close'].iloc[-2] <  df['high'].iloc[-2-40 : -2].max()
    #print df['close'].iloc[-2] /  df['close'].iloc[-3]
    #print df['close'].iloc[-1], df['high'].iloc[-1-41 : -2].max()
    #print df['close'].iloc[-3] != df['close'].iloc[-1-41 : -2].max()
    #print df['close'].iloc[-1] /  df['close'].iloc[-2] > 1.09
    return findFlag  
    
def bigShot1(df) :
    findFlag    = False
    if  df['close'].iloc[-2]  <= df['ma_5'].iloc[-2]\
    and df['close'].iloc[-2]  <= df['ma_10'].iloc[-2]\
    and df['close'].iloc[-2]  <= df['ma_20'].iloc[-2]\
    and df['close'].iloc[-2]  <= df['ma_60'].iloc[-2]\
    and df['close'].iloc[-1]  >  df['ma_5'].iloc[-1]\
    and df['close'].iloc[-1]  >  df['ma_10'].iloc[-1]\
    and df['close'].iloc[-1]  >  df['ma_20'].iloc[-1]\
    and df['close'].iloc[-1]  >  df['ma_60'].iloc[-1]\
    and df['close'].iloc[-1]  /  df['close'].iloc[-2] > 1.03 \
    and df['close'].iloc[-1]  >= df['high'].iloc[-42:-2].max() :
        findFlag= True       
    return findFlag   

def bigShot2(df) :
    findFlag    = False
    #newdata     = []
    #nd          = [df['ma_20'].iloc[-2], df['ma_60'].iloc[-2], df['ma_250'].iloc[-2]]
    #newdata.append(nd) 
    #nd          = [df['ma_20'].iloc[-1], df['ma_60'].iloc[-1], df['ma_250'].iloc[-1]]
    #newdata.append(nd)        
    #df1         = pd.DataFrame(newdata)
    #df1.columns = ['ma20', 'ma60', 'ma250'] 
    acnt        = 0
    acnt        = acnt + 1 if df['low'].iloc[-2] <= df['ma_20'].iloc[-2]  else acnt + 0
    acnt        = acnt + 1 if df['low'].iloc[-2] <= df['ma_60'].iloc[-2]  else acnt + 0
    acnt        = acnt + 1 if df['low'].iloc[-2] <= df['ma_250'].iloc[-2] else acnt + 0    
    bcnt        = 0
    bcnt        = bcnt + 1 if df['close'].iloc[-1] >= df['ma_20'].iloc[-2]  else bcnt + 0
    bcnt        = bcnt + 1 if df['close'].iloc[-1] >= df['ma_60'].iloc[-2]  else bcnt + 0
    bcnt        = bcnt + 1 if df['close'].iloc[-1] >= df['ma_250'].iloc[-2] else bcnt + 0
        
    if  df['low'].iloc[-2]  <= df['ma_5'].iloc[-2]\
    and df['low'].iloc[-2]  <= df['ma_10'].iloc[-2]\
    and acnt                  <= 2\
    and df['close'].iloc[-1]  >  df['ma_5'].iloc[-1]\
    and df['close'].iloc[-1]  >  df['ma_10'].iloc[-1]\
    and bcnt                  >= 2\
    and df['close'].iloc[-1]  /  df['close'].iloc[-2] > 1.03 \
    and df['close'].iloc[-1]  >= df['high'].iloc[-22:-2].max() :
        findFlag= True       
    return findFlag   
    
######################################################################
################## proc week data and funcs ##########################
######################################################################      
def getWeekTurtle (code, name, data, wdf, testFlag) :
    findFlag        = False            
    wdf['max']      = wdf['close'].rolling(40).max()
    wdf['max'].fillna(method='bfill', inplace=True)
    close0          = wdf['close'].iloc[-3]
    maxcls0         = wdf['max'].iloc[-3]
    close1          = wdf['close'].iloc[-2]
    maxcls1         = wdf['max'].iloc[-2]
    close2          = wdf['close'].iloc[-1]
    maxcls2         = wdf['max'].iloc[-1]
    
    if maxcls0 != close0 and maxcls1 == close1 and maxcls2 == close2 :
        findFlag    = True
        return findFlag, code, name, maxcls0, close0, maxcls1, close1, maxcls2, close2, 'max 20w'
    else:
        return findFlag, code, name
   

def getContinueGrow (code, df) :
    findFlag    = False  
    
    if len(df) < 60:
        return findFlag
    
    print (df['close'].iloc[-20-2:-2].max(), df['close'].iloc[-2], df['close'].iloc[-1])
    
    if  df['close'].iloc[-2]  < df['close'].iloc[-40-2:-2].max() \
    and df['close'].iloc[-1]  > df['close'].iloc[-40-2:-2].max() \
    and df['close'].iloc[-1]  / df['close'].iloc[-2] > 1.03 : 
        findFlag    = True
        return findFlag
    return findFlag
    
def getBotton (code, df) :
    findFlag    = False  
    #print df  
    
    for i in range (1, 6) :
        if  df['open'].iloc[-i]  / df['low'].iloc[-i] > 1.05 \
        and df['close'].iloc[-i] / df['low'].iloc[-i] > 1.05 \
        and df['low'].iloc[-i]   == df['low'].iloc[-i-20:-i].min() :
            findFlag    = True
            return findFlag
    return findFlag
        
def checkWeekTrend (code, name) :
    findFlag    = False 
    ifile       = getWeekPath()  + code + '.csv'    
    if not os.path.exists(ifile) :
        return 'False', code, 'stock data is not exits'
    else :
        df      = pd.read_csv(ifile, parse_dates=[1])
        df.sort_values('date', ascending=True, inplace=True) 
        
    currLen     = len(df)
    if currLen < 30:
        return False, code, 'week num < 30', name
    
    try:
        cls3    = checkMaxValue(df['ma_5'].iloc[-3], df['ma_10'].iloc[-3], df['ma_20'].iloc[-3])
        cls2    = checkMaxValue(df['ma_5'].iloc[-2], df['ma_10'].iloc[-2], df['ma_20'].iloc[-2])
        cls1    = checkMaxValue(df['ma_5'].iloc[-1], df['ma_10'].iloc[-1], df['ma_20'].iloc[-1])
        if  df['close'].iloc[-3] <= cls3\
        and df['close'].iloc[-2] >= cls2\
        and df['ma_5' ].iloc[-1] >= df['ma_10'].iloc[-1]\
        and df['ma_10'].iloc[-1] >= df['ma_20'].iloc[-1]\
        and df['low'  ].iloc[-1] >  cls1:
            findFlag    = True
            return findFlag
    except:
        return findFlag
        
def checkWeek120 (code, name) :
    findFlag    = False 
    ifile       = getWeekPath()  + code + '.csv'    
    if not os.path.exists(ifile) :
        return 'False', code, 'stock data is not exits'
    else :
        df      = pd.read_csv(ifile, parse_dates=[1])
        df.sort_values('date', ascending=True, inplace=True) 
        
    currLen     = len(df)
    if currLen < 250:
        return False, code, 'week num < 120', name
    
    try:
        if  df['close'].iloc[-2] <= df['ma_250'].iloc[-2]\
        and df['close'].iloc[-2] <= df['ma_20'].iloc[-2]\
        and df['close'].iloc[-1] >  df['ma_250'].iloc[-1]\
        and df['close'].iloc[-1] >  df['ma_5'].iloc[-1]\
        and df['low'  ].iloc[-1] >  df['ma_20'].iloc[-1]:
            findFlag    = True
            return findFlag
    except:
        return findFlag
        

def checkYearLine (code, df) :
    findFlag    = False
    try:
        if  df['close'].iloc[-3] <  df['ma_250'].iloc[-3]\
        and df['close'].iloc[-2] >= df['ma_250'].iloc[-2]\
        and df['close'].iloc[-1] >  df['close'].iloc[-2]:
            findFlag    = True
            return findFlag
    except:
        return findFlag
        
def checkYearLineBack (code, df) :
    findFlag    = False
    length      = len(df)
    for i in range (length - 21, length - 1) :
        if  df['close'].iloc[i-1] <  df['ma_250'].iloc[i-1]\
        and df['close'].iloc[i]   >  df['ma_250'].iloc[i]:
            idxmin      = df['close'].iloc[i+1:].idxmin()
            print (i, i+1, idxmin, code)
            if i+1 == idxmin:
                continue
            idxmax      = df['close'].iloc[i+1:idxmin].idxmax()
            valmax      = df['close'].iloc[i+1:idxmin].max()
            if idxmin - idxmax > 4 \
           and df['close'].iloc[-2]  <  valmax \
           and df['close'].iloc[-1]  >= valmax :
                findFlag    = True
                return findFlag
        #and (df['close'].iloc[i+1:].min() == df['close'].iloc[-2] or df['low'].iloc[i+1:].min() == df['low'].iloc[-2])\
                

def checkWeekLine (code, df, ma) :
    findFlag    = False
    df.fillna(999)
    if df['close'].iloc[-2] <= df['ma_'+str(ma)].iloc[-2] \
   and df['close'].iloc[-2] <= df['close'].iloc[-22 :-2].max() \
   and df['close'].iloc[-1] >= df['ma_'+str(ma)].iloc[-1] \
   and df['close'].iloc[-1] >= df['close'].iloc[-22 :-2].max() :
       print ("max = ", df['close'].iloc[-22 :-2].max(), "close = ", df['close'].iloc[-1], "ma = ", str(ma), df['ma_'+str(ma)].iloc[-1],"\n")
       findFlag    = True
    return findFlag

def setDebugLevel (level) :
    global      debugLevel
    debugLevel  = level 
    
def findStandBoolen (code, df, ma) :
    findFlag    = False 
    length      = len(df)   
    
    findType    = True
    findType    = False
    if findType :
        for i in range (length-10, length) :
            if  df['close'].iloc[i-1] < df['up_'+str(ma)].iloc[i-1]  \
            and df['close'].iloc[i]   > df['up_'+str(ma)].iloc[i]  :
                firstCls    = df['close'].iloc[i]
                contNum     = 0
                growNum     = 0
                days        = 4
                level       = days - 1
                end         = length if i+1+days > length else i+1+days
                for j in range (i+1, end) :
                    if df['close'].iloc[j] > df['up_'+str(ma)].iloc[j] :
                        contNum     += 1
                    if df['close'].iloc[j] / df['close'].iloc[j-1] > 1.05:
                        growNum     += 1                    
                if contNum >= level and growNum >= level \
                and df['close'].iloc[-1] > df['ma_5'].iloc[-1] :
                #and df['close'].iloc[-1] == df['close'].iloc[length-10:].max():
                    findFlag    = True
                    print  ("mostGrow", i, firstCls, contNum, length-i-1, df['close'].iloc[-1], round(df['close'].iloc[-1]/firstCls,2))
                    return "mostGrow", i, firstCls, contNum, length-i-1, df['close'].iloc[-1], round(df['close'].iloc[-1]/firstCls,2)
                    
    findType    = True
    #findType    = False
    if findType :   
        for i in range (length-10, length-2) :
            if  df['close'].iloc[i-1] < df['up_'+str(ma)].iloc[i-1]  \
            and df['close'].iloc[i]   > df['up_'+str(ma)].iloc[i]  :
                firstCls    = df['close'].iloc[i]
                minidx      = df['close'].iloc[i+1:].idxmin()
                minval      = df['close'].iloc[i+1:].min()
                maxidx      = df['close'].iloc[i+1:].idxmax()
                maxval      = df['close'].iloc[i+1:].max()
                
                if  minval < firstCls :
                    for j in range (i+1, length-1) :
                        if  df['close'].iloc[j-1] < df['up_'+str(ma)].iloc[j-1] \
                        and df['close'].iloc[j]   > df['up_'+str(ma)].iloc[j]   \
                        and df['close'].iloc[j+1] > df['up_'+str(ma)].iloc[j+1] \
                        and df['close'].iloc[j]   / df['close'].iloc[j-1] > 1.07\
                        and df['close'].iloc[-1]  / df['close'].iloc[-2] > 1.03 \
                        and j - i >= 3 \
                        and df['close'].iloc[-1] == df['close'].iloc[length-10:].max():
                            #findFlag    = True
                            print  ("reGrow", i, firstCls, df['close'].iloc[j], length-j-1, df['close'].iloc[-1], round(df['close'].iloc[-1]/df['close'].iloc[j],2))
                            #return "reGrow", i, firstCls, df['close'].iloc[j], length-j-1, df['close'].iloc[-1], round(df['close'].iloc[-1]/df['close'].iloc[j],2)
                else :
                    print (i+1, length)
                    for j in range (i+2, length) :
                        endidx      = j + 1
                        print (i, j, endidx, )
                        print (df['close'].iloc[i:endidx])
                        print (df['close'].iloc[j-1], df['close'].iloc[i:endidx-1].max())
                        print (df['close'].iloc[j],   df['close'].iloc[i:endidx].max())
                        if  df['close'].iloc[j-1] <  df['close'].iloc[i:endidx-1].max() \
                        and df['close'].iloc[j]   == df['close'].iloc[i:endidx].max() \
                        and df['close'].iloc[j]   / df['close'].iloc[j-1] > 1.05\
                        and df['close'].iloc[-1]  >  df['ma_5'].iloc[-1] \
                        and df['close'].iloc[-1] == df['close'].iloc[length-10:].max():
                            #findFlag    = True
                            print  ("contGrow", i, firstCls, j, length-j-1, df['close'].iloc[j], round(df['close'].iloc[-1]/df['close'].iloc[j],2))
                            return "contGrow", i, firstCls, j, length-j-1, df['close'].iloc[j], round(df['close'].iloc[-1]/df['close'].iloc[j],2)
    
    return findFlag, 99
    
    
def findStandBoolen1 (code, df, ma) :
    findFlag    = False 
    length      = len(df)    
    df.reset_index(inplace=True) 
    
    findType    = True
    findType    = False
    if findType :
        if df['close'].iloc[-1] < df['ma_5'].iloc[-1] \
        or df['close'].iloc[-1] < df['ma_10'].iloc[-1]\
        or df['close'].iloc[-1] < df['ma_20'].iloc[-1]:
            return findFlag, 99
            
        minval              = df['close'].min()
        maxval              = df['close'].max() 
        maxidx              = df['close'].idxmax() 
        for i in range (length-15, length) :
            if  df['close'].iloc[i-1] < df['up_'+str(ma)].iloc[i-1]\
            and df['close'].iloc[i]   > df['up_'+str(ma)].iloc[i]  \
            and maxidx > i :
                print (i, length)
                firstCls    = df['close'].iloc[i-1]
                contNum     = 0
                growRate    = round(maxval/firstCls, 2)
                days        = 3
                level       = 1.3
                end         = length if i+1+days > length else i+1+days
                for j in range (i+1, end) :
                    if df['close'].iloc[j] > df['up_'+str(ma)].iloc[j] :
                        contNum     += 1 

                print (contNum, firstCls, maxval, growRate, level)
                lastMaxidx  = df['close'].iloc[-length:-1].idxmax()
                lastMaxval  = df['close'].iloc[-length:-1].max()
                print (lastMaxidx, lastMaxval, length - 2 - lastMaxidx)
                #when length = 20, -1 idx = 19; -2 idx = 18
                
                if contNum >= days and growRate >= level \
                and df['close'].iloc[-1] > df['close'].iloc[-2]:
                #and length - 2 - lastMaxidx >= 3:
                    findFlag    = True
                    print ( "mostGrow", i, df['close'].iloc[i], maxidx, maxval, df['close'].iloc[-1], round(df['close'].iloc[-1]/firstCls,2))
                    return "mostGrow", i, df['close'].iloc[i], maxidx, maxval, df['close'].iloc[-1], round(df['close'].iloc[-1]/firstCls,2)
                    
    findType    = True
    #findType    = False
    if findType :
        lastFlag            = False
        lastIdx             = 0
        firstCls            = 0
        for i in range (length-12, length-3) :
            if  df['close'].iloc[i-1] < df['up_'+str(ma)].iloc[i-1]\
            and df['close'].iloc[i]   > df['up_'+str(ma)].iloc[i]  :
                lastFlag    = True
                lastIdx     = i
                firstCls    = df['close'].iloc[i]
                print (lastIdx, firstCls)
                
        if  df['close'].iloc[-4]   < df['up_'+str(ma)].iloc[-4]  \
        and df['close'].iloc[-3]   > df['up_'+str(ma)].iloc[-3]  \
        and df['close'].iloc[-3]   / df['close'].iloc[-4] > 1.04 \
        and df['close'].iloc[-3]   >= firstCls \
        and df['close'].iloc[-2]   > df['ma_5'].iloc[-2]         \
        and df['close'].iloc[-1]   > df['ma_5'].iloc[-1]         \
        and df['close'].iloc[-1]   / df['close'].iloc[-2] > 1.03 \
        and df['close'].iloc[-1]   / df['close'].iloc[-3] > 0.99 \
        and lastFlag :
        #and length - lastIdx - 1 >= 5 :
            findFlag    = True
            i           = 0
            print  ("reGrow", lastIdx, firstCls, length-1, df['close'].iloc[-1], length - lastIdx - 1, round(df['close'].iloc[-1]/firstCls,2))
            return "reGrow", lastIdx, firstCls, length-1, df['close'].iloc[-1], length - lastIdx - 1, round(df['close'].iloc[-1]/firstCls,2)
    
    return findFlag, 99
####################################################################
#####################old and uesless code here######################
####################################################################
    