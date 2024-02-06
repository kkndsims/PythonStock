# -*- coding: utf-8 -*-


























def findHalfGrow(code, name, testFlag, df, hf, start, money, qos):
    info                    = ""
    delta                   = 120
    procMacdData(df)
    if df.macd.iloc[-1] <= df.macd.iloc[-2] : return ""

    dfRust                  = findDaysCross(code, name, testFlag, df, delta)
    if len(dfRust) == 0 : return ""

    close                   = df.close.tolist()
    upidx                   = dfRust[1]
    highmax                 = dfRust[2]
    gaps                    = df.index[-1] - highmax
    botcls                  = close[upidx]
    grow                    = round(close[-1] / min(close[-delta:]), 2)
    amount                  = df.amount.iloc[-1]
    change                  = df.change.iloc[-1]
    
    if gaps == 0:
        info                = str(qos) + "9 " + str(change).rjust(5) + "%"
        info                = ""
    else :
        difmin              = df.dif.iloc[highmax:].idxmin()
        if df.dif.iloc[difmin] < 0 : return ""
        if difmin == df.index[-1]  : return ""
        if df.dif.iloc[-1] < 0     : return ""
        if df.dea.iloc[-1] < 0     : return ""
        if df.dif.iloc[-1] < df.dea.iloc[-1] : return ""
        #cnt1                = df.seal.iloc[upidx:highmax].sum()
        #cnt2                = df.seal.iloc[difmin:].sum()
        #if cnt1 == 0 : return ""
        #if cnt2 == 0 : return ""
        #if cnt1 == 0 : return ""
        #if cnt1 + cnt2 == 0 : return ""
        info                = str(qos) + "8 " + str(change).rjust(5) + "%"
    if info == "" : return ""
    
    #volmax                  = df.change.iloc[upidx:].idxmax()
    #if close[-1] < close[volmax] : return ""
    #if grow > 1.5 : return ""
    #if not (df.change.iloc[-1] > df.change.iloc[volmax] / 2 or df.seal.iloc[-1]) : return ""
    #if not ((amount >= 5 and change >= 5) or (amount >= 8 and change >= 2) or (df.seal.iloc[-1] and change >= 5)) : return ""
    
    #procMacdData(hf)
    #if hf.dif.iloc[-1] < hf.dea.iloc[-1] and hf.dea.iloc[-1] < 0 : return []
    #hfDlt                   = (df.index[-1] - upidx) * 8
    #volmax                  = hf.volume.tail(hfDlt).idxmax()
    #if hf.close.iloc[-1] < hf.close.iloc[volmax] : return ""
    
    #info                    = str(qos) + "9 " + str(change).rjust(5) + "%"
    result                  = [True, code, name.rjust(4), botcls, str(amount)+"亿", close[-1], grow, gaps, info]
    return result
def findMacdGrow(code, name, testFlag, df, hf, start, money, qos):
    global topAmount
    info                    = ""
    procMacdData(df)
    if df.dif.iloc[-1] < df.dif.iloc[-2] : return ""
    df['macdPk']            = df.macd == df.macd.rolling(9, center=True).max()
    df['macdPk']            = [1 if a else 0 for a in df.macdPk]
    dfpeak                  = df.macdPk.where(df.macdPk == 1).dropna().index
    if len(dfpeak) == 0 : return ""
    
    close                   = df.close.tolist()
    amount                  = df.amount.iloc[-1]
    change                  = df.change.iloc[-1]
    pkidx                   = dfpeak[-1]
    macdmin                 = df.macd.iloc[pkidx:].idxmin()
    maxgrow                 = df.grow.iloc[macdmin:].max()
    clsmax                  = max(close[pkidx - 5 : macdmin])
    gaps                    = df.index[-1] - macdmin
    grow                    = round(close[-1] / min(close[-120:]), 2)
    if df.dif.iloc[pkidx:].where(df.dif < 0).dropna().count() : return ""
    if gaps == 0 : return ""
    if close[-1] > clsmax or df.tur.iloc[-1] :
        info                = str(qos) + "9 " + str(change).rjust(5) + "%"
    elif maxgrow > 5 :
        info                = str(qos) + "8 " + str(change).rjust(5) + "%"
    elif df.macd.iloc[-1] > 0 :
        info                = str(qos) + "7 " + str(change).rjust(5) + "%"
    if info == "" : return ""
    
    procMacdData(hf)
    if hf.dif.iloc[-1] < 0 : return ""
    if hf.dif.iloc[-1] < hf.dif.iloc[-2] : return ""
    if grow > 1.5 : return ""
    if not ((amount >= topAmount and change >= 5) or (amount >= 8 and change >= 2) or (df.seal.iloc[-1] and change >= 5)) : return ""
    #print(code, name, close[-1], min(close[-120:]), grow)
    result                  = [True, code, name.rjust(4), close[pkidx], str(amount)+"亿", close[-1], grow, gaps, info]
    return result
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

    info                = ""
    vblist              = findVolume2(code, name, testFlag, df)
    if len(vblist) == 0 : return ""
    close               = df['close'].tolist()
    change              = df['change'].iloc[-1]
    amount              = df['amount'].iloc[-1]
    minval              = df.close.tail(90).min()
    grow                = round(close[-1] / minval, 2)
    point               = vblist[-1] 
    gaps                = df.index[-1] - point
    voltp               = df.volb.iloc[point]

    procMacdData(df)
    dwnum               = df.cross.iloc[point:].where(df.cross == -1).dropna().count()
    #print(code, name, dwnum, close[-1], close[point:point+3])
    if dwnum > 1 : return ""
    if close[-1] < max(close[point:point+3]) : return ""
    if df.dif.iloc[-1] < 0 : return ""
    if df.dif.iloc[-1] < df.dea.iloc[-1] : return ""
    cntmacd             = df.macd.iloc[point:].min()
    cntdif              = df.dif.iloc[point:].where(df.dif < 0).dropna().count()
    
    if cntmacd >= 0 :
        info            = str(qos) + "9." + str(voltp) + " " + str(change).rjust(5) + "%"
    elif cntdif == 0:
        info            = str(qos) + "8." + str(voltp) + " " + str(change).rjust(5) + "%"
    else:
        info            = str(qos) + "7." + str(voltp) + " " + str(change).rjust(5) + "%"
    #print(dwnum, cntmacd, cntdif, grow1)
    #print(code, name, df.volb.iloc[point], close[point], df.index[-1], gaps, vblist)
    if info == "" : return ""
    sumchange           = df.change.iloc[point-2:point+1].sum()
    summoney            = df.amount.iloc[point-2:point+1].sum()
    
    procMacdData(hf)
    if hf.dif.iloc[-1] < 0 : return ""
    if hf.dif.iloc[-1] < hf.dea.iloc[-1] : return ""

    if True \
    and sumchange > 8 \
    and summoney > 10 \
    and gaps > 0 \
    and (grow < 1.5 or (grow < 2 and df.tur.iloc[-1]))\
    and df.tur.iloc[-1] \
    and True :
    #and (df.bigv.iloc[-1] or df.tur.iloc[-1]) \
    #and True :
        result          = [True, code, name.rjust(4), close[point], str(amount)+"亿", close[-1], grow, gaps, info]
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



