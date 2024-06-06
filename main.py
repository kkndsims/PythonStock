# -*- coding: utf-8 -*-
# -*- coding:gb2312 -*-

import sys, datetime
from   comDef      import *

enDays                  = True
enHalf                  = True
enHalf                  = False
enPlat                  = False
testList                = []
    
if __name__ == '__main__' :
    endDate             = getWorkDay(datetime.datetime.now())
    #endDate             = '20240531'
    #sys.exit(0)
    
    if enHalf : setUpdateHalfEn(enHalf)     #使能处理日线数据
    if enPlat : getPlateCode(endDate)       #获取板块中文名称
    
    getStockCode(endDate)                   #获取股票中文名称
    procInitStockData(endDate)              #处理half/days数据
    
    #testList            = ['SH880350']
    if enPlat : 
        getPlatImage(endDate, testList)     #板块买点

    #testList            = ['SH600230']     #上证
    #testList            = ['SZ300014']     #深证
    getStockImage(endDate, testList)        #个股买点
      
    sendFlag            = True
    sendFlag            = False
    if sendFlag and len(testList) == 0 :
        print("\n%s :: line %3d : ############### 发送邮件 ###############"\
        %("comDef", sys._getframe().f_lineno))            
        sendMail(endDate)
    
    print("\n%s :: line %3d : ############### 所有工作处理完成 ###############"\
    %("comDef", sys._getframe().f_lineno))
    sys.exit(0)