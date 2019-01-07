# encoding: UTF-8

import sys
import signal

import multiprocessing
from time import sleep
from datetime import datetime, time

from vnpy.event import EventEngine2
from vnpy.trader.vtEvent import EVENT_LOG, EVENT_ERROR
from vnpy.trader.vtEngine import MainEngine, LogEngine
from vnpy.trader.gateway import tushareGateway
from vnpy.trader.app import ctaStrategy
from vnpy.trader.app import dataRecorderCT
from vnpy.trader.app.ctaStrategy.ctaBase import EVENT_CTA_LOG

#----------------------------------------------------------------------
def processErrorEvent(event):
    """
    处理错误事件
    错误信息在每次登陆后，会将当日所有已产生的均推送一遍，所以不适合写入日志
    """
    error = event.dict_['data']
    print (u'错误代码：%s，错误信息：%s' %(error.errorID, error.errorMsg))
    
    
#----------------------------------------------------------------------
def runChildProcess():
    """子进程运行函数"""
    print ('-'*20)
    
    # 创建日志引擎
    le = LogEngine()
    le.setLogLevel(le.LEVEL_INFO)
    le.addConsoleHandler()
    le.addFileHandler()

    
    le.info(u'启动CTA策略运行子进程')
    
    DAY_START = time(19, 00)         # 日盘启动和停止时间
    DAY_END = time(19, 43)
    
    
    cta = None        # 子进程句柄
    
    while True:
        currentTime = datetime.now().time()
        recording = False
        
        # 判断当前处于的时间段
        if (currentTime >= DAY_START and currentTime <= DAY_END):
            recording = True
        else:
            le.info(u'未在启动时间段')
        
        # 记录时间则需要启动子进程
        if recording and cta is None:
            ee = EventEngine2()
            #le.info(u'事件引擎创建成功')
            
            me = MainEngine(ee)
  
            #连接数据库
            me.dbConnect()
            le.info("连接数据库")    
            
            me.addGateway(tushareGateway)
            me.addApp(dataRecorderCT)
            me.addApp(ctaStrategy)
            
            le.info(u'主引擎创建成功')
            
            ee.register(EVENT_LOG, le.processLogEvent)
            ee.register(EVENT_CTA_LOG, le.processLogEvent)
            ee.register(EVENT_ERROR, processErrorEvent)
            le.info(u'注册日志事件监听')
            
        
            #连接数据接口
            me.connect(tushareGateway.gatewayName)
            le.info(u'连接TUSHARE接口')
            
            #sleep(10)    # 等待CTP接口初始化
            
            cta = me.getApp(ctaStrategy.appName)
            
            cta.loadSetting()
            le.info(u'CTA策略载入成功')
    
            cta.initAll()
            le.info(u'CTA策略初始化成功')
            
            cta.startAll()
            le.info(u'CTA策略启动成功')
            
        # 非记录时间则退出子进程
        if not recording and cta is not None:
            
            cta.stopAll()
            
            ee.stop()
            
            le.info(u'关闭子进程')
      
            return 
            
        sleep(5)    



#----------------------------------------------------------------------
def runParentProcess():
    """父进程运行函数"""
    # 创建日志引擎
    le = LogEngine()
    le.setLogLevel(le.LEVEL_INFO)
    le.addConsoleHandler()
    
    le.info(u'启动CTA策略守护父进程')
    
    DAY_START = time(19, 00)         # 日盘启动和停止时间
    DAY_END = time(19, 28)
    
    NIGHT_START = time(20, 45)      # 夜盘启动和停止时间
    NIGHT_END = time(2, 45)
    
    p = None        # 子进程句柄
    
    while True:
        currentTime = datetime.now().time()
        recording = False
        
        # 判断当前处于的时间段
        if (currentTime >= DAY_START and currentTime <= DAY_END):
            recording = True
        
        # 记录时间则需要启动子进程
        if recording and p is None:
            le.info(u'启动子进程')
            p = multiprocessing.Process(target=runChildProcess)
            p.start()
            le.info(u'子进程启动成功')
            
        # 非记录时间则退出子进程
        if not recording and p is not None:
            le.info(u'关闭子进程')
            p.terminate()
            p.join()
            p = None
            le.info(u'子进程关闭成功')
            return 
            
        sleep(5)

#----------------------------------------------------------------------
def runBacktestingProcess():
    """子进程运行函数"""
    print ('-'*20)
    
    # 创建日志引擎
    le = LogEngine()
    le.setLogLevel(le.LEVEL_INFO)
    le.addConsoleHandler()
    le.addFileHandler()
    
    le.info(u'启动CTA策略运行子进程')
    
    ee = EventEngine2()
    
    me = MainEngine(ee)

    me.addApp(ctaStrategy)
    
    le.info(u'主引擎创建成功')
    
    ee.register(EVENT_LOG, le.processLogEvent)
    ee.register(EVENT_CTA_LOG, le.processLogEvent)
    ee.register(EVENT_ERROR, processErrorEvent)
    le.info(u'注册日志事件监听')
    
    me.dbConnect()
    le.info("连接数据库")
    
    cta = me.getApp(ctaStrategy.appName)
    
    cta.loadSetting()
    le.info(u'CTA策略载入成功')
    
    cta.setBackTestingMode()
    le.info(u'运行CTA策略回测')
    
    cta.runBacktesting()

    cta.stopAll()
    ee.stop()

    
    while True:
        sleep(1)


if __name__ == '__main__':
    runBacktestingProcess()
    
    #runChildProcess()
    
    # 尽管同样实现了无人值守，但强烈建议每天启动时人工检查，为自己的PNL负责
    #runParentProcess()