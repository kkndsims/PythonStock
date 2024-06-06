# -*- coding: utf-8 -*-


############################ 周/月线中继 #######################
def findLongMacd(code, name, testFlag, ipt, df, qos, tp):
    info            = ""
    change          = df['change'].iloc[-1]
    amount          = df['amount'].iloc[-1]
    close           = ipt['close'].tolist()
    delta           = ipt['delta'].tolist()
    dif             = ipt['dif'].tolist()
    grow            = round(close[-1] / ipt.close.tail(30).min(), 2)
    if delta[-1] < delta[-2] : return ""
    if ipt.high.iloc[-1] / close[-1] > 1.09 : return ""
    if grow > growLevel : return ""

    sta             = list(ipt.cross.where(ipt.cross == 1).dropna().index)
    if len(sta) == 0 : return ""
    P0              = sta[-1]
    gaps            = ipt.index[-1] - P0
    adj             = ipt.cross.iloc[P0:].where(ipt.cross == -1).dropna().index
    ctn             = ipt.cross.iloc[P0:].where(ipt.cross == 2).dropna().index
    adjCnt          = len(adj)
    ctnCnt          = len(ctn)
    if testFlag : print(code, name, P0, ipt.index[-1], adjCnt, ctnCnt, grow)
    if df.seal.iloc[-gaps*5:].sum() == 0 : return ""

    # dif下穿dea调整后又上穿dea
    cntLs           = ipt.delta.tail(gaps).where(ipt.delta <  0).dropna().count()
    if adjCnt >= 1 and ctnCnt >= 1 :
        info        = str(qos) + "9  " + str(change).rjust(5) + "%"
        clsmax      = max(close[P0 : adj[-1]])
        if ctn[-1] < adj[-1]   : return ""          # 最近是dif上穿
        if dif[adj[-1]] < 0    : return ""
        if close[-1] < clsmax * dyaLevel : return ""# cls逼近新高
        #if cntLs > cntGt * dltLevel : return ""     # delta < 0个数少，调整强势
        midx        = ctn[-1]
        gaps        = ipt.index[-1] - midx
        #if incr > 1.1 : return ""                   # 最近上穿后涨幅过大
    
    # dif上穿dea后又下穿dea, 短暂调整delta快速向上, 出现1个明显的底
    if adjCnt >= 1 and ctnCnt == 0 :
        info        = str(qos) + ".8  " + str(change).rjust(5) + "%"
        clsmax      = max(close[P0 : adj[-1]])
        if dif[adj[-1]] < -0.1 : return ""
        if close[-1] < clsmax * dyaLevel : return ""# cls逼近新高
        #if cntLs > cntGt * dltLevel : return ""     # delta < 0个数少，调整强势
        
    # dif上穿dea后未下穿dea, dif缩减靠近dea后又快速上涨
    if adjCnt == 0 and ctnCnt == 0 :
        info        = str(qos) + ".7  " + str(change).rjust(5) + "%"
        midx        = findDifPoke(code, name, testFlag, ipt) # 最近delta极小值
        gaps        = ipt.index[-1] - midx
        #if df.seal.iloc[-gaps*5:].sum() == 0 : return ""
        if dif[-1] < 0 : return ""
        if dif[-1] < dif[-2] : return ""
        if dif[-2] < dif[-3] : return ""
        if df.tur.iloc[-1] == 0 : return ""
        if midx <= P0 : 
            if ipt.tur.iloc[-1] == 0 : return ""
        else :
            info    = str(qos) + ".6  " + str(change).rjust(5) + "%"
            if dif[midx] < 0 : return ""           # 0轴附近出现极小值
    
    if info == "" : return ""
    if testFlag : print(code, name, tp, ipt.index[-1], gaps, delta[-1], delta[-2])
    if True \
    and True :
        result          = [True, code, name.rjust(4), close[P0], str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""
def findShortMacd(code, name, testFlag, ipt, wk, df, hf, qos, tp):
    info            = ""
    change          = df['change'].iloc[-1]
    amount          = df['amount'].iloc[-1]
    close           = ipt['close'].tolist()
    grow            = round(close[-1] / df.close.tail(120).min(), 2)
    if grow > growLevel : return ""

    sta             = list(ipt.cross.where(ipt.cross == 1).dropna().index)
    if len(sta) == 0 : return ""
    P0              = sta[-1]
    gaps            = ipt.index[-1] - P0
    adj             = ipt.cross.iloc[P0:].where(ipt.cross == -1).dropna().index
    ctn             = ipt.cross.iloc[P0:].where(ipt.cross == 2).dropna().index
    adjCnt          = len(adj)
    ctnCnt          = len(ctn)
    if testFlag : print(code, name, P0, ipt.index[-1], adjCnt, ctnCnt, grow)
    if df.seal.iloc[P0:].sum() == 0 : return ""
        
    # dif下穿dea调整后又上穿dea
    if adjCnt >= 1 and ctnCnt >= 1 :
        gaps        = ipt.index[-1] - ctn[-1]
        info        = str(qos) + "9  " + str(change).rjust(5) + "%"
        if ipt.dif.iloc[ctn[-1]] < -0.1 : return "" # dif在0轴附近上穿dea
        if tp == "days" : 
            if ctn[-1] < adj[-1]     : return ""    # 最近是dif上穿
            if ipt.dif[adj[-1]] < 0  : return ""
            if ipt.tur.iloc[-1] == 0 : return ""    # 均线海龟
            
    if tp == "half" : return ""
    if adjCnt >= 1 and ctnCnt == 0 :
        # dif上穿dea后又下穿dea, 短暂调整delta快速向上, 出现1个明显的底
        info        = str(qos) + ".8  " + str(change).rjust(5) + "%"
        clsmax      = max(close[P0 : adj[-1]])
        if close[-1] < clsmax * dyaLevel : return ""# cls逼近新高
        if ipt.dif.iloc[adj[-1]] < 0 : return ""

    # dif上穿dea后未下穿dea, 出现极值后又开始上涨
    if adjCnt == 0 and ctnCnt == 0 :
        info        = str(qos) + ".7  " + str(change).rjust(5) + "%"
        midx        = findDifPoke(code, name, testFlag, ipt) # 最近delta极小值
        gaps        = ipt.index[-1] - midx
        if df.seal.iloc[midx:].sum() == 0 : return ""
        if ipt.dif.iloc[-1] < 0 : return ""
        if ipt.dif.iloc[-1] < ipt.dif.iloc[-2] : return ""
        if ipt.dif.iloc[-2] < ipt.dif.iloc[-3] : return ""
        if ipt.tur.iloc[-1] == 0 : return ""
        if midx < P0 : return "" 
        if ipt.dif.iloc[midx] < 0 : return ""           # 0轴附近出现极小值

    if info == "" : return ""
    if True \
    and True :
        result          = [True, code, name.rjust(4), close[P0], str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""


############################# 周线放量后连涨或回调破新高 #######################
def findWeekDif(code, name, testFlag, df, wk, qos):
    info            = ""
    P0, P1, P2      = findMacdCross(code, name, testFlag, wk, "days")
    if P0 == 0 : return ""
    
    close           = wk['close'].tolist()
    change          = df['change'].iloc[-1]
    amount          = df['amount'].iloc[-1]
    gaps            = wk.index[-1] - P0
    grow            = round(close[-1] / wk.close.tail(max(gaps,20)).min(), 2)
    if df.delta.iloc[-1] < df.delta.iloc[-2] : return ""
    if df.delta.iloc[-2] < df.delta.iloc[-3] : return ""
    if wk.delta.iloc[-1] < wk.delta.iloc[-2] : return ""
    if not (amount >= dayAmount and change >= dayChange) : return ""
    if grow > growLevel : return ""
    if testFlag :
        print(code, name, P0, P1, P2)

    if  P2 > 0 :
        if P2 < P1 : return ""
        gaps        = wk.index[-1] - P2
        info        = str(qos) + "W  " + str(change).rjust(5) + "%"
    else :
        info        = str(qos) + "M  " + str(change).rjust(5) + "%"
        mth         = getMergData("month", df)
        procMacdData(mth)
        if mth.delta.iloc[-1] < mth.delta.iloc[-2] : return ""
        if mth.dif.iloc[-1] < mth.dea.iloc[-1] : return "" 
        if mth.dif.iloc[-1] < 0 : return "" 
        
    if True \
    and True :
        result          = [True, code, name.rjust(4), close[P0], str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""
############################# 周线天量后新高或回调破新高 #######################
def findWeekVol2(code, name, testFlag, wk, df, qos):
    # 周线天量 ：
    # 1）量价齐升,一波冲顶；（边拉边洗）
    # 2）量能放大,股价不涨, 2/3周后回落调整,再创新高（吸筹洗盘）
    info                = ""
    change              = df['change'].iloc[-1]
    amount              = df['amount'].iloc[-1]
    delta               = 25
    point               = wk.change1.tail(weekOfYear).idxmax() # 使用wk换手率
    gaps                = wk.index[-1] - point
    high                = wk.close.iloc[point-3:point+3].max()
    if gaps > delta : return "" 
    if wk.close.iloc[-1] < high * 0.95 : return ""
    if wk.dif.iloc[-1] < -0.1 : return ""
    
    minval              = wk.close.tail(max(gaps, delta)).min()
    close               = wk['close'].tolist()
    grow                = round(close[-1] / minval, 2)
    if testFlag :
        print(code, name, gaps, wk.close.iloc[-1], wk.close.iloc[point])
        
    if True \
    and grow < growLevel \
    and (amount >= dayAmount and change >= dayChange) \
    and True :
        info            = str(qos) + "  " + str(change).rjust(5) + "%"
        result          = [True, code, name.rjust(4), minval, str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""
############################# 周线中继 #######################
def findWeekVol1(code, name, testFlag, wk, df, qos):
    info            = ""
    P0, P1          = findMacdCtniu(code, name, testFlag, wk, 7, 'days')
    if P0 == 0 : return ""
    
    change          = df['change'].iloc[-1]
    amount          = df['amount'].iloc[-1]
    close           = wk['close'].tolist()
    gaps            = wk.index[-1] - P1
    minval          = wk.close.tail(max(gaps, 30)).min()
    grow            = round(close[-1] / minval, 2)
    if wk.dif.iloc[-1] < -0.1 : return ""
    if wk.tur.iloc[-1] == 0 : return ""
    if not (amount >= dayAmount and change >= dayChange) : return ""
    if grow > growLevel : return ""
    if testFlag :
        print(code, name, P0, P1, wk.index[-1], gaps, grow)
    
    if True \
    and True :
        info        = str(qos) + "  " + str(change).rjust(5) + "%"
        result      = [True, code, name.rjust(4), minval, str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""
def findDaysVol1(code, name, testFlag, wk, df, qos):
    info            = ""
    P0, P1          = findMacdCtniu(code, name, testFlag, df, 7, 'days')
    if P0 == 0 : return ""
    
    change          = df['change'].iloc[-1]
    amount          = df['amount'].iloc[-1]
    close           = wk['close'].tolist()
    gaps            = df.index[-1] - P1
    minval          = wk.close.tail(30).min()
    grow            = round(close[-1] / minval, 2)
    if df.tur.iloc[-1] == 0 : return ""
    if not (amount >= dayAmount and change >= dayChange) : return ""
    if grow > growLevel : return ""
    if testFlag :
        print(code, name, P0, P1, df.index[-1], gaps, grow)

    if True \
    and True :
        info        = str(qos) + "  " + str(change).rjust(5) + "%"
        result      = [True, code, name.rjust(4), minval, str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""
def findHalfVol1(code, name, testFlag, df, hf, qos):
    info            = ""
    procMacdData(hf)
    sta             = list(hf.cross.where(hf.cross == 1).dropna().index)
    if len(sta) == 0 : return ""
    
    P0              = sta[-1]
    gaps            = hf.index[-1] - P0
    adj             = hf.cross.iloc[P0:].where(hf.cross == -1).dropna().index
    ctn             = hf.cross.iloc[P0:].where(hf.cross == 2).dropna().index
    adjCnt          = len(adj)
    ctnCnt          = len(ctn)
    
    close           = df['close'].tolist()
    change          = df['change'].iloc[-1]
    amount          = df['amount'].iloc[-1]
    grow            = round(close[-1] / df.close.tail(60).min(), 2)
    #if not (amount >= 3 and change >= 2 or df.seal.iloc[-1]) : return ""

    if adjCnt == 0 :
        info        = str(qos) + "9  " + str(change).rjust(5) + "%"
        if gaps >= 16 : return ""
        # 15min dif > dea
        #ifile       = minipath + code
        #data        = pd.read_csv(ifile,encoding='gbk',header=2)
        #data        = data.iloc[:-1]
        #print(code, name, data.tail(20))
    if adjCnt >= 1 and ctnCnt == 0:
        cntGt       = hf.delta.tail(gaps).where(hf.delta >= 0).dropna().count()
        cntLs       = hf.delta.tail(gaps).where(hf.delta <  0).dropna().count()
        if cntLs > cntGt * 0.4 : return ""
        info        = str(qos) + "8  " + str(change).rjust(5) + "%"
    if adjCnt == 1 and ctnCnt == 1:
        if ctn[-1] < adj[-1] : return ""
        info        = str(qos) + "7  " + str(change).rjust(5) + "%"
        gaps        = hf.index[-1] - ctn[-1]
        if gaps >= 12 and hf.close.iloc[-1] != hf.close.iloc[ctn[-1]:].max() : return ""
        
    if info == "" : return ""
    if grow > 1.8 : return ""
    #if testFlag:
    #    print(code, name, P0, P1, P2, info)

    if True \
    and True :
        result          = [True, code, name.rjust(4), close[P0], str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""
def findDaysMacd(code, name, testFlag, wk, df, hf, qos):


def findDaysGrow1(code, name, testFlag, wk, df, qos):
    sta1            = list(df.cross.where(df.cross == 1).dropna().index)
    sta2            = list(df.cross.where(df.cross == 2).dropna().index)
    sta             = sta1 + sta2
    if len(sta) == 0 : return ""  
    if df.grow.iloc[-1] < -5 : return ""
    if df.high.iloc[-1] / df.close.iloc[-1] > 1.07 : return ""
    if df.tur.iloc[-1] == 0 : return ""
    if df.delta.iloc[-1] < df.delta.iloc[-2] : return ""
    
    P0              = max(sta)

    gaps            = df.index[-1] - P0
    grow            = round(df.close.iloc[-1] / df.close.tail(120).min(), 2)
    left            = P0 - 40 if P0 >= 40 else 0 
    volmax          = df.change.iloc[left : P0].idxmax()
    left            = 0 if volmax == 0 else volmax - 1
    if close[-1] < max(close[left : volmax+2]) : return ""
    if not (amount >= 3 and change >= dayChange) : return ""
    if df.grow.where(df.grow >= 7).dropna().count() == 0 : return ""
    if df.seal.iloc[P0:].sum() == 0 : return ""
    
    info            = str(qos) + "9  " + str(change).rjust(5) + "%"
    if testFlag :
        print(code, name, volmax, max(sta), df.index[-1], gaps)

    if info == "" : return ""
    if grow > growLevel : return ""

    if True \
    and True :
        result          = [True, code, name.rjust(4), close[P0], str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""


def findDaysDif(code, name, testFlag, df, wk, qos):
    info            = ""
    P0, P1, P2      = findMacdCross(code, name, testFlag, df, "days")
    if P0 == 0 : return ""
    
    close           = df['close'].tolist()
    change          = df['change'].iloc[-1]
    amount          = df['amount'].iloc[-1]
    gaps            = df.index[-1] - P0
    #if df.seal.iloc[P0:].sum() == 0 : return ""
    if not (amount >= dayAmount and change >= dayChange) : return ""
    if df.delta.iloc[-1] < df.delta.iloc[-2] : return ""
    if df.delta.iloc[-2] < df.delta.iloc[-3] : return ""
    grow            = round(wk.close.iloc[-1] / wk.close.tail(20).min(), 2)
    
    # days dif cross dea, week调整结束，days/week共振
    if (P1 + P2 == 0) :
        info        = str(qos) + "8.W  " + str(change).rjust(5) + "%"
    # days dif cross dea, dif未拉回0轴，微调后破新高
    elif (P1 > 0 and P2 == 0) :
        if df.tur.iloc[-1] == 0 : return ""
        info        = str(qos) + "8.D  " + str(change).rjust(5) + "%"
    # days dif cross dea, 调整拉回0轴又破新高
    elif (P1 > 0 and P2 > 0) :
        gaps        = df.index[-1] - P2 if P2 else gaps
        info        = str(qos) + "7.W  " + str(change).rjust(5) + "%"
        if wk.tur.iloc[-1] == 0 : return ""
    else :
        info        = str(qos) + "7.D  " + str(change).rjust(5) + "%"
    if info == "" : return ""
    if testFlag:
        print(code, name, P0, P1, P2, info)

    if True \
    and grow < growLevel \
    and True :
        result          = [True, code, name.rjust(4), close[P0], str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""

def findAjustGrow(code, name, testFlag, df, hf, wins, qos):
    info                = ""
    close               = df['close'].tolist()
    change              = df['change'].iloc[-1]
    amount              = df['amount'].iloc[-1]
    minval              = df.close.tail(90).min()
    grow                = round(close[-1] / minval, 2)
    if df.tur.iloc[-1] == 0 : return ""
    if df.tur.iloc[-2] == 1 : return ""
    if grow > 1.5 : return ""
    flag1               = amount >= dayAmount and change >= dayChange
    flag2               = amount >= dayAmount - 1 and df.grow.iloc[-1] >= wins
    if not (flag1 or flag2) : return ""
    result          = [True, code, name.rjust(4), close[-1], str(amount)+"亿", close[-1], close[-1], close[-1], info]
    return result 
    
    slist               = df.grow.where(df.grow >= wins).dropna().index
    if len(slist) == 0 : return ""
    point               = slist[-1]
    gaps                = df.index[-1] - point
    if gaps > 21 : return ""
    if df.grow.iloc[-1] < 0 : return ""
    point               = df.change1.tail(120).idxmax()
    if df.close.iloc[-1] < df.close.iloc[point-2:point+2].max() : return ""

    alist               = df.grow.iloc[point:].where(df.grow < 0).dropna().index
    close               = df['close'].tolist()
    change              = df['change'].iloc[-1]
    amount              = df['amount'].iloc[-1]

    if len(alist) == 0 :
        info            = str(qos) + "9 " + str(change).rjust(5) + "%"
        if df.close.tail(37).idxmax() != df.index[-1] : return "" 
        if grow > 1.6 : return ""
        return ""
    else :
        info            = str(qos) + "8 " + str(change).rjust(5) + "%"
        idx             = alist[0]
        if close[-1] < close[idx-1] : return ""   #连续2天站稳
        if close[-2] < close[idx-1] : return ""
        if df.index[-1] == idx : return ""
        if grow > 1.6 : return ""

    if True \
    and (amount > dayAmount and change > dayChange) \
    and True :
        result          = [True, code, name.rjust(4), close[point], str(amount)+"亿", close[-1], grow, gaps, info]
        return result 
    return ""



















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



