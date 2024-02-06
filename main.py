# -*- coding: utf-8 -*-
# -*- coding:gb2312 -*-
"""
Spyder Editor:This is a temporary script file.
"""
import sys, datetime
from   comDef      import *
    
if __name__ == '__main__' :
    endDate             = getWorkDay(datetime.datetime.now())
    befDate             = getLastWorkDay(datetime.datetime.now(), endDate)
    #befDate             = '20231229'
    #endDate             = '20231229'
    #sys.exit(0)
    
    getStockCode(endDate)       #获取股票中文名称
    #getPlateCode(endDate)       #获取板块中文名称
    
    #setUpdateHalfEn (True)      #使能处理half数据
    setUpdateDaysEn (True)      #使能处理日线数据
    procInitStockData(endDate)  #处理days/week/month数据
    
    testList            = []
    #testList            = ['SH601858']  #上证
    #testList            = ['SZ300232']  #深证
    testFlag            = True if testList else False
    getStockImage(endDate, testFlag, testList)
        
    testList            = []    
    #testList            = ['SH880350']
    #testFlag            = True if testList else False
    #getPlatReplay(endDate, testFlag, testList)
      
    sendFlag            = True
    #sendFlag            = False
    if sendFlag and not testFlag  :
        print("\n%s :: line %3d : ############### 发送邮件 ###############"\
        %("comDef", sys._getframe().f_lineno))            
        sendMail(endDate, befDate)
    
    print("\n%s :: line %3d : ############### 所有工作处理完成 ###############"\
    %("comDef", sys._getframe().f_lineno))
    sys.exit(0)