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
    #endDate             = '20220216'
    
    getStockCode(endDate)       #获取股票中文名称
    #getPlateCode()              #获取板块中文名称
    
    setUpdateDaysEn (True)      #使能处理日线数据    
    #setUpdateHalfEn (True)      #使能处理half数据
    #setUpdateWeekEn (True)      #使能处理周线数据
    procInitStockData(endDate)  #处理half/days/week数据
    
    testList            = []
    #testList            = ['SH603538']  #上证
    #testList            = ['SZ000759']  #深证
    testFlag            = True if testList else False
    getStockImage(endDate, testFlag, testList)
        
    testList            = []
    #testList            = ['SH880423']     #板块
    #testFlag            = True if testList else False
    #getDaysReplay(endDate, codeList + plateList, codeName + plateName)
    #getPlateImage(endDate, plateList, plateName, testFlag, testList)
      
    sendFlag            = True
    sendFlag            = False
    if sendFlag and not testFlag  :
        print("\n%s :: line %3d : ############### 发送邮件 ###############"\
        %("comDef", sys._getframe().f_lineno))            
        sendMail(endDate)
    
    print("\n%s :: line %3d : ############### 所有工作处理完成 ###############"\
    %("comDef", sys._getframe().f_lineno))
    sys.exit(0)