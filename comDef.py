# -*- coding: utf-8 -*-
# -*- coding:gb2312 -*-

"""
Created on Mon Oct 10 22:59:41 2016

@author: Administrator
"""

import os, sys, datetime, calendar#, codecs, chardet
import re
import pandas as pd

import smtplib
from email.mime.text import MIMEText
from email.header import Header
from email.mime.multipart import MIMEMultipart
#from email.mime.image import MIMEImage

from   comApi  import *

pd.set_option('display.max_columns', 1000) #pands打印不换行
pd.set_option('display.max_rows',    500)
pd.set_option('display.width',       1000)
 
workpath                = "D:\\PythonWorkSpace\\PythonStock3.7\\"   #工作路径
dayipath                = "C:\\new_tdx\\T0001\\export\\d\\"         #日线原始数据
minipath                = "C:\\new_tdx\\T0001\\export\\m\\"         #5分钟原始数据
basepath                = "C:\\new_tdx\\T0001\\export\\"            #基础原始数据

dayspath                = workpath + "DaysPath\\"                   #日线输出
weekpath                = workpath + "WeekPath\\"                   #周线输出
mothpath                = workpath + "MothPath\\"                   #月线输出
rsltpath                = workpath + "RsltPath\\"                   #统计结果
drawType                = []                                        #半小时/日线/周线
codeList                = []
codeName                = []
baseInfo                = pd.DataFrame()
baseFile                = ""

plateList               = []
plateName               = []
plateInfo               = pd.DataFrame()
plateFile               = ""

amountUnit              = 100000000                                 #1亿
volumeUnit              = 1000000                                   #1万手
######################################################################
############################# 获取基本信息 ############################
######################################################################

############################# 获取当天日期 ############################
def getWorkDay(now):    
    year                = now.year
    month               = now.month
    day                 = now.day
    hour                = now.hour  
    day                 = day - 1 if hour < 15 else day
    lastweek            = calendar.weekday(year, month, day)
    weekday             = now.weekday() + 1
    print("%s :: line %3d : ############### weekday = %s"\
    %("comDef", sys._getframe().f_lineno, weekday))
    day                 = day - (lastweek - 4) if lastweek > 4 else day  #weekday 0 is sunday, 4 is friday        
    endDate             = str(year) + str(month).zfill(2) + str(day).zfill(2)
    print("%s :: line %3d : ############### lastday = %s"\
    %("comDef", sys._getframe().f_lineno, endDate))
    return endDate
def getLastWorkDay(now, endDate):    
    year                = int(endDate[0:4])
    month               = int(endDate[4:6])
    day                 = int(endDate[6:8])
    lastweek            = calendar.weekday(year, month, day) 
    day                 = day - 1 if lastweek > 0 else day - 2   
    endDate             = str(year) + str(month).zfill(2) + str(day).zfill(2)
    print("%s :: line %3d : ############### workday = %s"\
    %("comDef", sys._getframe().f_lineno, endDate))
    return endDate
############################# 获取股票code/name ######################
def getStockCode(endDate) : 
    global codeList, codeName, baseInfo, baseFile
    baseFile            = basepath + "\\沪深Ａ股" + endDate + ".xls"    
    if not os.path.exists(baseFile) :
        print("%s :: line %3d : ############### %s socketFile not exists"\
        %("comDef", sys._getframe().f_lineno, baseFile))
        sys.exit(0)
    else:
        baseInfo        = pd.read_csv(baseFile, sep='\t', encoding='gbk')
        baseInfo        = baseInfo[:-1]
        codeName        = baseInfo['名称'].tolist()
        codeList        = baseInfo['代码'].tolist()
        print("%s :: line %3d : ############### CodeNum  = %d\n"\
        %("comDef", sys._getframe().f_lineno, len(baseInfo))) 
    # 获取代码, 原始code格式 '="000001"'
    for i in os.listdir(dayipath):
        char                = '="' + str(i)[2:] + '"'
        if char in codeList:
            idx             = codeList.index(char)
            codeList[idx]   = i
def getPlateCode(endDate) : 
    global plateList, plateName, plateInfo, plateFile
    plateFile               = basepath + "\\板块指数" + endDate + ".xls"    
    if not os.path.exists(plateFile) :
        print("%s :: line %3d : ############### %s plateFile not exists"\
        %("comDef", sys._getframe().f_lineno, plateFile))
        sys.exit(0)
    else:
        plateInfo       = pd.read_csv(plateFile, sep='\t', encoding='gbk')
        plateInfo       = plateInfo[:-1]
        plateName       = plateInfo['名称'].tolist()
        plateList       = plateInfo['代码'].tolist()
        print("%s :: line %3d : ############### PlatesNum  = %d\n"\
        %("comDef", sys._getframe().f_lineno, len(plateInfo)))
    plateList           = [ "SH"+a.replace("\"", "").replace("=", "") for a in plateList]
############################# 股票复盘 ###############################
def getDaysReplay(endDate):
    global rsltpath, baseFile
    df                  = pd.read_csv(baseFile, sep='\t', encoding='gbk')
    print("\n%s :: line %3d : ############### replay with stockNum = %d"\
    %("plateApi", sys._getframe().f_lineno, len(df)))
        
    df.drop(df.tail(1).index, inplace=True)             # drop last rows
    df['amount']        = round(df['总金额'] / 10000, 2)
    summ                = df['amount'].sum()
    midd                = df['amount'].median()
    avrg                = df['amount'].mean()
    print("成交总额(%.1f)亿, 中位数(%.1f)亿, 平均数(%.1f)亿" %(summ, midd, avrg))
    
    df['grow']          = [0 if a == '--  ' else float(a) for a in df['涨幅%']]
    upnum               = len(df[df['grow'] >  0.0])
    dwnum               = len(df[df['grow'] <  0.0])
    flnum               = len(df[df['grow'] == 0.0])
    midd1               = df['grow'].median()
    avrg1               = df['grow'].mean()
    grow10              = len(df[df['grow'] >= 9.7])
    grow05              = len(df[df['grow'] >=   5])
    down10              = len(df[df['grow'] <=-9.7])
    down05              = len(df[df['grow'] <=  -5])
    print("上涨个数(%4d), 下跌个数(%4d), 平盘个数(%4d), 涨幅中位数(%.1f), 平均数(%.1f)" %(upnum, dwnum, flnum, midd1, avrg1))
    print("涨停个数(%4d), >=5个数(%4d)" %(grow10, grow05))
    print("跌停个数(%4d), <=5个数(%4d)" %(down10, down05))
    
    df['change']        = [0 if a == '--  ' else float(a) for a in df['换手%']]
    dt                  = df[['代码', '名称', 'amount', 'change', 'grow']]
    ch                  = dt[dt['amount'] >= 10]
    ch                  = ch[ch['change'] >= 10]
    ch                  = ch[ch['grow']   >=  0]
    ch                  = ch.sort_values('amount', ascending=False)
    print("成交(10亿)换手( 5%%)个数(%4d)" %(len(ch)) )
    print(ch)
def getPlatReplay(endDate, testFlag, testList):
    global rsltpath, plateList, plateName
    if not testFlag:
        iputList        = [[a, b] for a, b in zip(plateList, plateName)]
    else:
        iputList        = [[a, plateName[plateList.index(a)]] for a in testList]
    print("\n%s :: line %3d : ############### 处理板块 with PlateNum = %d"\
    %("comDef", sys._getframe().f_lineno, len(iputList)))
        
    starttime           = datetime.datetime.now() 
    update_plate        = []
    for i in range (len(iputList)) :
        code            = iputList[i][0]
        name            = iputList[i][1]
        daysfile        = dayipath + code
        if not os.path.exists(daysfile) :
            print(code, name, daysfile, "not exist")
            continue
        
        data            = pd.read_csv(daysfile,encoding='gbk',header=2)
        data            = data.iloc[:-1]
        data.columns    = ['date','open','high','low','close','volume','amount']
        data            = data[~data['volume'].isin([0])] 
        data['amount']  = (data['amount'] / amountUnit).round(decimals=1)
        data.sort_values('date', ascending=True, inplace=True)
        if len(data) <= 1:
            continue
        procPlateInfo(data, 20, 10)
        rst             = findPlateBuy(code, name, testFlag, data)
        if str(rst[0]) == 'True':
            update_plate.append(rst)
    endtime             = datetime.datetime.now()
    print("%s :: line %3d : update_succ_num = %d，use time = %s Second"\
    %("comDef", sys._getframe().f_lineno, len(iputList), (endtime - starttime).seconds))
    if update_plate :
        df              = pd.DataFrame(update_plate)
        df.columns      = ['flag', 'code', 'name', 'close', 'amount', 'bot', 'grow', 'days', 'info']
        del df['flag']
        df.sort_values('info', ascending=False, inplace=True)
        print(df)            
def procPlateInfo(df, tur, gap) :
    df['volume']        = ( df['volume'] / 10000     ).round(decimals=1)
    df['grow']          = ((df['close']  / df['close'].shift(1) - 1) * 100).round(decimals=2)
    df['rate']          = ((df['amount'] / df['amount'].shift(1)- 1) * 100).round(decimals=1)
    procBasicInfo(df, tur, gap)
######################################################################
############################# 设置日线/周线/月线使能 ##################
def setUpdateDaysEn(flag):
    global drawType
    drawType.append('days')
def setUpdateWeekEn(flag):
    global drawType
    drawType.append('week') 
def setUpdateMothEn(flag):
    global drawType
    drawType.append('moth')
############################# 处理天/周/月线数据 ####################
def procInitStockData(endDate) :   
    global codeList, codeName
    output                      = rsltpath + endDate
    if not os.path.exists(output) :
        os.mkdir(output)
    
    for tp in drawType:
        update_file             = output + '\\update_' + tp + '.csv'
        update_list             = []
        update_fail             = []
        starttime               = datetime.datetime.now()  
        if not os.path.exists(update_file) :
            print("%s :: line %3d : ############### merge %s with SocketNum = %d"\
            %("comDef", sys._getframe().f_lineno, tp, len(codeList)))
            for i in range(len(codeList)):
                code            = codeList[i]
                name            = codeName[i]
                flag            = getUpdateMap(endDate, code, name, tp)
                #print(i, len(codeList), endDate, code, name, tp, flag)
                update_list.append([code, name]) if flag else update_fail.append([code, name])
                if i > 0 and (i % 2000 == 0):
                    endtime = datetime.datetime.now()
                    print("%s :: line %3d : update_succ_num = %d，use time = %s Second"\
                    %("comDef", sys._getframe().f_lineno, i, (endtime - starttime).seconds))
            
            endtime             = datetime.datetime.now()
            print("%s :: line %3d : update succ_num = %d，fail_num = %d, use time = %s Second"\
            %("comDef", sys._getframe().f_lineno, len(update_list), len(update_fail), (endtime - starttime).seconds))
            if update_list :
                df              = pd.DataFrame(update_list)            
                df.columns      = ['code', 'name']
                df.to_csv(update_file, index=False, encoding="GBK")
            if update_fail :
                print("%s :: line %3d : update_fail : %s"\
                %("comDef", sys._getframe().f_lineno, update_fail))
############################# 处理日线/周线/月线数据 ###################  
def getChangeRate(name, data) :     # 计算换手率
    global baseInfo
    nameList            = baseInfo['名称'].tolist()
    idx                 = nameList.index(name)
    volume              = baseInfo['流通股(亿)'][idx]
    data['change']      = (data['volume'] / float(volume)).round(decimals=2)
    return data
def getUpdateMap(endDate, code, name, tp) :
    if tp == 'moth':
        ifile           = dayipath + code
        ofile           = mothpath + code
    elif tp == 'days':
        ifile           = dayipath + code   
        ofile           = dayspath + code
    elif tp == 'week':
        ifile           = dayipath + code
        ofile           = weekpath + code
    else:
        print("%s :: line %3d : ############### update %s not support"\
        %("comDef", sys._getframe().f_lineno, tp))
        sys.exit(0)
    if not os.path.exists(ifile) :
        return False
    
    # 删除前两行和最后一行无效内容
    data                    = pd.read_csv(ifile,encoding='gbk',header=2)
    data                    = data.iloc[:-1]
    if (len(data)):
        data.columns        = ['date','open','high','low','close','volume','amount']
        data                = data[data['open'].notnull()]
        data                = data[~data['volume'].isin([0])]
        data['amount']      = round( data['amount'] / amountUnit, 2).round(decimals=2)
        data['volume']      = round( data['volume'] / volumeUnit, 2).round(decimals=2)
        data                = getChangeRate(name, data)
        #data['rate']        = round((data['amount'] / data['amount'].shift(1)), 1)
        if tp == 'days':
            data['grow']    = round(((data['close'] / data['close' ].shift(1) - 1) * 100), 2)
            #del data['open'], data['high'], data['low']
            #procBasicInfo(data, 60, 10)
            data.to_csv(ofile, index=False)
            return True
        else:           #保存周线数据
            del data['open'], data['high'], data['low']
            return getMergData(code, name, endDate, tp, data, ofile)
############################# 合并half/周线数据 ######################## 
def getMergData(code, name, endDate, otype, data, ofile) :
    period              = "W" if otype == "week" else "M"
    data['date']        = pd.to_datetime(data['date']) #把字符串转换成时间信息
    dt                  = data.date.iloc[-1]
    dt                  = datetime.datetime.strptime(str(dt),"%Y-%m-%d %H:%M:%S")     
    lastDate            = str(dt.year) + str(dt.month).zfill(2) + str(dt.day).zfill(2)
    if lastDate != endDate :
        return False
    
    data                = data.set_index('date')
    df2                 = data.resample          (period,closed="right",label="right").last()
    #df2['open']         = data['open'].resample  (period,closed="right",label="right").first().round(decimals=2) 
    #df2['high']         = data['high'].resample  (period,closed="right",label="right").max().round(decimals=2) 
    #df2['low']          = data['low'].resample   (period,closed="right",label="right").min().round(decimals=2) 
    df2['close']        = data['close' ].resample(period,closed="right",label="right").last().round(decimals=2)    
    df2['volume']       = data['volume'].resample(period,closed="right",label="right").sum().round(decimals=1)  
    df2['amount']       = data['amount'].resample(period,closed="right",label="right").sum().round(decimals=1)
    df2['change']       = data['change'].resample(period,closed="right",label="right").sum().round(decimals=2)       
    df2                 = df2[~df2['volume'].isin([0])]
    # 每执行100次消耗120s，非常慢
    #df2['grow']         = round(((df2['close'] / df2['close'].shift(1) - 1) * 100), 2)
    #df2['rate']         = round((df2['volume'] / df2['volume'].shift(1)), 1)
    df2.reset_index(inplace=True)
    df2.to_csv(ofile, index=False)
    #print(df2.tail(5))
    #sys.exit(0)
    return True
######################################################################
############################# 处理自选股票 ############################
######################################################################
def getStockImage(endDate, testFlag, testCode):
    global codeList, codeName, baseInfo, baseFile
    output                      = rsltpath + endDate
    if not os.path.exists(output) :
        print("%s :: line %3d : ############### merge data before process\n"\
        %("comDef", sys._getframe().f_lineno))
        sys.exit(0)
        
    baseInfo                    = pd.read_csv(baseFile, sep='\t', encoding='gbk')
    output                      = rsltpath + endDate
    ofile                       = output + '\\limit.txt'
    #getLimitup(baseInfo, ofile)
    
    empty                       = pd.DataFrame(columns=['code', 'name'])
    for tp in drawType:
        ifile                   = output + '\\update_' + tp + '.csv'
        df                      = pd.read_csv(ifile, parse_dates=[0], encoding='gbk')
        empty                   = pd.concat([empty, df], axis=0, ignore_index=True)
        empty.drop_duplicates(inplace=True)
        #print(len(df))
        #print(empty)
    #print(len(empty))
    
    if True:
        ofile                   = output + '\\buy.txt'
        starttime               = datetime.datetime.now()
        process_list            = []
        clist                   = empty['code'].tolist()
        nlist                   = empty['name'].tolist()
        if testFlag :
            index               = clist.index(testCode[0])
            name                = nlist[index]
            clist               = testCode
            nlist               = [name]
            
        if True:
            print("%s :: line %3d : ############### process with testFlag = %d codeNum = %d"\
            %("comDef", sys._getframe().f_lineno, testFlag, len(clist)))
            for i in range(len(clist)):
                code            = clist[i]
                name            = nlist[i]
                if (re.search("SH88",  code)):      # 剔除行业指数
                    continue
                if (re.search("SH688", code)):      # 剔除科创板块
                    continue
                if (re.search("ST",    name)):      # 剔除ST板块
                    continue

                value           = baseInfo.loc[ baseInfo['名称'] == name] 
                if len(value) < 1:
                    print("%s :: line %3d : code = %s name = %s not find"\
                          %("comDef", sys._getframe().f_lineno, code, name))
                    sys.exit(0)
                
                if int(value['总金额'].iloc[-1]) == 0:
                    continue
                    
                mfile           = mothpath + code
                dfile           = dayspath + code
                wfile           = weekpath + code
                flag            = getStockBuy(code, name, baseInfo, testFlag, mfile, dfile, wfile, value)
                #print(i, endDate, code, name, flag)
                if flag[0] == True:
                    process_list.append(flag)
                if i > 0 and (i % 2000 == 0):
                    endtime = datetime.datetime.now()
                    print("%s :: line %3d : process = %d，succ = %d use time = %s Second"\
                    %("comDef", sys._getframe().f_lineno, i, len(process_list), (endtime - starttime).seconds))
            
            endtime             = datetime.datetime.now()
            print("%s :: line %3d : process_succ = %d，use time = %s Second"\
            %("comDef", sys._getframe().f_lineno, len(process_list), (endtime - starttime).seconds))
            if process_list :
                df              = pd.DataFrame(process_list)
                df.columns      = ['flag', 'code', 'name', 'close', 'amount', 'bot', 'grow', 'days', 'info']
                del df['flag']
                df.sort_values('info', ascending=False, inplace=True)
                print(df)
                #writer          = pd.ExcelWriter(ofile)
                #df.to_excel(excel_writer=writer, sheet_name='sheet1')
                df.to_csv(ofile, index=False, encoding="GBK")
                #print(ofile)
                #df.to_excel(ofile)
############################# 发送处理邮件 ##########################         
def sendMail(endDate, befDate):
    if True:
    #if False:
        mail_host               = "smtp.163.com"
        mail_user               = "kkndsims@163.com"
        mail_pass               = "IIYCKAQYPWXFHYUD"
        mail_pass               = "MAEUSTLPXPJZJOTF"    # pop3/SMTP/IMAP 授信
        sender                  = 'kkndsims@163.com'
        receivers               = ['xieweisims@163.com']
        message                 = MIMEMultipart()
        subject                 = "result of " + endDate 
        message['Subject']      = Header(subject, 'utf-8') #title
        message['From']         = sender
        message['To']           = receivers[0]
    else:
        mail_host               = "smtp.139.com"
        mail_user               = "18621103823@139.com"
        mail_pass               = "bcdde81563eabc0fde00"
        sender                  = '18621103823@139.com'
        receivers               = ['kkndsims@163.com']
        message                 = MIMEMultipart()
        subject                 = "result of " + endDate 
        message['Subject']      = Header(subject, 'utf-8') #title
        message['From']         = sender
        message['To']           = receivers[0]
    
    # 添加一个文本附件
    output                      = rsltpath + endDate
    ofile                       = output + '\\buy.txt'
    if os.path.exists(ofile):
        att                         = MIMEText(open(ofile, 'rb').read(), 'base64','utf-8')
        att['Content-Type']         = 'application/octet-stream'
        nfile                       = "attachment;filename=\"" + "buy.txt" + "\""
        att['Content-Disposition']  = nfile
        message.attach(att)
        
    # 对比今天和前一个工作日的差异
    output1                     = rsltpath + befDate
    if os.path.exists(output1):
        ofile1                  = output1 + '\\buy.txt'
        ofile2                  = output  + '\\diff.txt'
        df                      = pd.read_csv(ofile , parse_dates=[0], encoding='gbk')
        df1                     = pd.read_csv(ofile1, parse_dates=[0], encoding='gbk')
        empty                   = pd.DataFrame(columns=['code','name','close','amount','bot','grow','days','info'])
        code                    = df['code'].tolist()
        code1                   = df1['code'].tolist()
        diff                    = list(set([i for i in code if i not in code1]))
        for i in range(len(diff)):
            idx                 = df[(df.code==diff[i])].index.tolist()[0]
            empty.loc[i]        = df.loc[idx]
        empty.to_csv(ofile2, index=False, encoding="GBK")
        print("different code with before workday")
        print(empty)

        att                         = MIMEText(open(ofile2, 'rb').read(), 'base64','utf-8')
        att['Content-Type']         = 'application/octet-stream'
        nfile                       = "attachment;filename=\"" + "diff.txt" + "\""
        att['Content-Disposition']  = nfile
        message.attach(att)
    else :
        print("before workday %s not exists" %(befDate))
        
    # 添加一个文本附件
    output                      = rsltpath + endDate
    ofile2                      = output + '\\limit.txt'
    if os.path.exists(ofile2):
        att                         = MIMEText(open(ofile2, 'rb').read(), 'base64','utf-8')
        att['Content-Type']         = 'application/octet-stream'
        nfile                       = "attachment;filename=\"" + "limit.txt" + "\""
        att['Content-Disposition']  = nfile
        message.attach(att)

    if True :
    #if False:
        try:
            smtpObj = smtplib.SMTP() 
            smtpObj.connect(mail_host, 25)
            smtpObj.login(mail_user, mail_pass)
            smtpObj.sendmail(sender, receivers, message.as_string())
            smtpObj.quit()
            print ("邮件发送成功")
        except smtplib.SMTPException as e:
            print ("Error: 无法发送邮件")
            print (e)
