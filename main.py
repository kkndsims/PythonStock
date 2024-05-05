# -*- coding: utf-8 -*-
# -*- coding:gb2312 -*-
"""
Spyder Editor:This is a temporary script file.
"""
import sys, datetime
from   comDef      import *
    
if __name__ == '__main__' :
    #endDate             = getWorkDay(datetime.datetime.now())
    #befDate             = getLastWorkDay(datetime.datetime.now(), endDate)
    befDate             = '20240430'
    endDate             = '20240430'
    #sys.exit(0)
    
    getPlateCode(endDate)       #获取板块中文名称
    getStockCode(endDate)       #获取股票中文名称
    setUpdateDaysEn (True)      #使能处理日线数据
    procInitStockData(endDate)  #处理days/week/month数据
    
    testList            = []    
    #testList            = ['SH880350']
    testFlag            = True if testList else False
    getPlatReplay(endDate, testFlag, testList)  #板块买点
    
    testList            = []
    #testList            = ['SH603228']  #上证
    #testList            = ['SZ000897']  #深证
    testFlag            = True if testList else False
    getStockImage(endDate, testFlag, testList)  #个股买点
      
    sendFlag            = True
    sendFlag            = False
    if sendFlag and not testFlag  :
        print("\n%s :: line %3d : ############### 发送邮件 ###############"\
        %("comDef", sys._getframe().f_lineno))            
        sendMail(endDate, befDate)
    
    print("\n%s :: line %3d : ############### 所有工作处理完成 ###############"\
    %("comDef", sys._getframe().f_lineno))
    sys.exit(0)