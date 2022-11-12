# -*- coding: utf-8 -*-
"""
Created on Sat Jul 16 10:13:48 2022

@author: sims
"""


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
            print("getTurlProcs:", hastur)
            print("getTurlProcs:", dlttur)
            print("getTurlProcs:", code, name, p0, p1, days, gap, data['close' ].iloc[p1], data['close' ].iloc[p0])
        return [True, p1, p0, days, gap]   

############################# B2:海归筛选 ############################
def getPeakPoke(code, name, testFlag, data, period):
    wins                = 2 * period + 1
    data['peak']        = data['close'] == data['close'].rolling(window=wins,center=True).max()
    data['poke']        = data['close'] == data['close'].rolling(window=wins,center=True).min()
    peak                = data['peak' ].tolist()
    poke                = data['poke' ].tolist()
    haspeak             = np.nonzero(peak)[0]
    haspoke             = np.nonzero(poke)[0]
    return haspeak, haspoke
def getTurtleGrow(code, name, testFlag, data):
    findFlag            = False    
    info                = "7.tur"
    peak, poke          = getPeakPoke(code, name, testFlag, data, 10)
    close               = data['close'].tolist()
    if len(poke) == 0:
        #print(code, name, len(peak), len(poke), "新股天数: ", len(data))
        return findFlag, code, name
    
    lens                = len(data)
    days                = len(data) - poke[-1]
    grow                = round(close[-1 ] / close[poke[-1]], 2)    
    #if grow > 1.5:
    #    print(code, name, grow, days)
    # 获取海归
    data['tur']         = data['close'] == data['close'].rolling(window=50).max()
    tur                 = data['tur'].tail(20).tolist()
    turcnt              = (data['tur'].tail(3) == True).astype(int).sum()
    if turcnt == 0:
        return findFlag, code, name 

    # 获取最近低点
    maxidx              = peak[-1]
    minidx              = poke[-1]
    maxval              = close[peak[-1]] if len(peak) == 1 else max(close[peak[-1]], close[peak[-2]])
    minval              = close[minidx]
    change              = data['change'].iloc[-1]
    amount              = data['amount'].iloc[-1]
    dif                 = data['dif'].tolist()
    mount               = strMount(amount)
    gap                 = len(tur) - tur.index(True)
    # 获取有效海归
    cntm                = getMountDays(code, name, testFlag, data, days) 
    if testFlag:
        print("getTurtleGrow:", code, name, close[peak[-2]], close[peak[-1]], maxval, close[-1])
        print("getTurtleGrow:", code, name, maxidx, maxval, close[-1], lens-maxidx)
        print("getTurtleGrow:", code, name, minidx, minval, days, gap, cntm)

    if  cntm        >= 0 \
    and gap         <= 3 \
    and close[-1]   >= maxval \
    and dif[minidx] >= -0.1 \
    and dif[-1]     >= 0 \
    and (change     >= 2 or  amount >= 10) \
    and (change     >= 2 and amount >= 2) \
    and True :
        findFlag        = True
        info            += "海归：" + "change_" + str(change).rjust(5) + "%"
        result          = [findFlag, code, name.rjust(4), minval, mount, grow, str(gap).rjust(2), info]
        return result
    return findFlag, code, name

############################# B2:dif递增 ############################
def getMacdDifGrow(code, name, testFlag, data):
    findFlag            = False    
    info                = "7.dif"
    lens                = len(data)
    close               = data['close'].tolist()
    change              = data['change'].iloc[-1]
    amount              = data['amount'].iloc[-1]
    mount               = strMount(amount)
    sclose              = strClose(close[-1])
    
    dif                 = data['dif' ].round(decimals=2).tolist()
    dea                 = data['dea' ].round(decimals=2).tolist()
    macd                = data['macd'].round(decimals=2).tolist()
    data['dmin']        = data['dif'] == data['dif'].rolling(window=7,center=True).min()
    data['dmax']        = data['dif'] == data['dif'].rolling(window=7,center=True).max()
    data['mmax']        = data['macd'] == data['macd'].rolling(window=11,center=True).max()
    dmin                = data['dmin'].tolist()
    dmax                = data['dmax'].tolist()
    mmax                = data['mmax'].tolist()
    hasdmax             = np.nonzero(dmax)[0]        # array[0]返回tur非0的索引值
    hasmacd             = np.nonzero(mmax)[0]        # array[0]返回tur非0的索引值
    #print(data.tail(10))
    #print(lens, hasdmax)
    if len(hasdmax) == 0:
        print("新股：", code, name, lens)
    elif len(hasmacd) == 0:
        print("新股：", code, name, lens) 
    else:
        idx             = hasmacd[-1]
        days            = lens - idx
        grow            = round(close[-1] / close[idx], 2)
        cross           = list(map(lambda x, y: 1 if x > y else 0, dif[idx:], dea[idx:]))
        if testFlag:
            print('macd:', idx, days, dif[idx:], dea[idx:], len(dif[idx:]), cross.count(1))
            print('macd:', dif[idx], dif[-1], close[idx], close[-1], grow, change, amount, macd[idx])
        if dif[idx] >= 0 \
        and dif[-1] >= dif[idx] \
        and close[-1] >= close[idx] \
        and days    == cross.count(1) \
        and (change >= 5 and amount >= 3) \
        and grow >= 1 \
        and macd[idx] >= 0 \
        and True :
        #and macd[-1] > macd[idx] \
            findFlag    = True
            info        = "5.macd "
            info        += "连续：" + str(change).rjust(5) + "%"
            result      = [findFlag, code, name.rjust(4), sclose, mount, grow, str(days).rjust(2), info]
            return result
        
        idx             = hasdmax[-1]
        days            = lens - idx
        grow            = round(close[-1] / close[idx], 2)
        if testFlag:
            print('last dif > 0', idx, dif[idx], days, dif[-2], dif[-1], min(dif[idx:]))
        if dif[idx] >= 0 \
        and dif[-2] <  dif[idx] \
        and dif[-1] >= dif[idx] \
        and (change >= 5 and amount >= 3) \
        and grow >= 1 \
        and macd[idx] >= 0 \
        and True :
            findFlag    = True
            info        = "5.dif "
            if min(dif[idx:]) < 0 :
                info    += "跌破0轴：" + str(change).rjust(5) + "%"
            else:
                info    += "不破0轴：" + str(change).rjust(5) + "%"
            result      = [findFlag, code, name.rjust(4), sclose, mount, grow, str(days).rjust(2), info]
            return result
    return findFlag, code, name
    sys.exit(0)
    # 找出dif低点
    dmin                = data['dmin'].tolist()
    close               = data['close'].tolist()
    change              = data['change'].iloc[-1]
    amount              = data['amount'].iloc[-1]
    mount               = strMount(amount)
    sclose              = strClose(close[-1])
    hasdmin             = np.nonzero(dmin)[0]        # array[0]返回tur非0的索引值
        
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

############################# B4:144均线 ############################
def getMaGrow(code, name, testFlag, data):
    findFlag            = False    
    info                = "6.144ma"
    lens                = len(data)
    close               = data['close'].tolist()
    ma144               = data['ma_144'].tolist()
    change              = data['change'].iloc[-1]
    amount              = data['amount'].iloc[-1]
    mount               = strMount(amount)
    sclose              = strClose(close[-1])
    
    data['dlt']         = data['close'] - data['ma_144']
    vlmin               = data['ma_144'].min()
    idmin               = data['ma_144'].idxmin()
    dltIxMin            = data['dlt'].iloc[idmin+1:].idxmin()
    days                = lens - idmin
    day0                = lens - dltIxMin
    grow0               = round(close[-1] / vlmin, 2)
    if testFlag:
        print(data.tail(10))
        print(code, name, lens, idmin, days, vlmin, close[-1], grow0, dltIxMin, day0, close[dltIxMin])
 
    if days >= 3 and days < 10 \
    and close[-1] > ma144[-1]  \
    and amount >= 1 \
    and change >= 3 \
    and True :
        findFlag        = True
        info            = "4.ma144_"
        info            += mount + "_" + str(change).rjust(5) + "%"
        result          = [findFlag, code, name.rjust(4), sclose, mount, grow0, str(days).rjust(2), info]
        return result
    return findFlag, code, name
    
############################# 寻找高换手 ###########################    
def getHighCHange(code, name, testFlag, data):