# -*- coding: utf-8 -*-
# -*- coding:gb2312 -*-
"""
Spyder Editor:This is a temporary script file.
"""
import sys, datetime
from   comDef      import *
from   plateApi    import *
    
if __name__ == '__main__' :
    endDate             = getWorkDay(datetime.datetime.now())
    befDate             = getLastWorkDay(datetime.datetime.now(), endDate)
    #endDate             = '20220930'
    
    getStockCode(endDate)       #获取股票中文名称
    #getPlateCode()              #获取板块中文名称
    
    setUpdateDaysEn (True)      #使能处理日线数据    
    #setUpdateHalfEn (False)      #使能处理half数据
    #setUpdateWeekEn (True)      #使能处理周线数据
    procInitStockData(endDate)  #处理half/days/week数据
    
    testList            = []
    #testList            = ['SH605398']  #上证
    #testList            = ['SZ002995']  #深证
    testFlag            = True if testList else False
    getStockImage(endDate, testFlag, testList)
        
    testList            = []
    #getDaysReplay(endDate, codeList + plateList, codeName, plateName)
    #getPlateImage(endDate, plateList, plateName, testFlag, testList)
      
    sendFlag            = True
    sendFlag            = False
    if sendFlag and not testFlag  :
        print("\n%s :: line %3d : ############### 发送邮件 ###############"\
        %("comDef", sys._getframe().f_lineno))            
        sendMail(endDate, befDate)
    
    print("\n%s :: line %3d : ############### 所有工作处理完成 ###############"\
    %("comDef", sys._getframe().f_lineno))
    sys.exit(0)