# encoding: UTF-8

'''
本文件中包含了CTA模块中用到的一些基础设置、类和常量等。
'''

# CTA引擎中涉及的数据类定义
from vnpy.trader.vtConstant import *

# 常量定义
# CTA引擎中涉及到的交易方向类型
CTAORDER_BUY = u'买开'
CTAORDER_SELL = u'卖平'
CTAORDER_SHORT = u'卖开'
CTAORDER_COVER = u'买平'

# 本地停止单状态
STOPORDER_WAITING = u'等待中'
STOPORDER_CANCELLED = u'已撤销'
STOPORDER_TRIGGERED = u'已触发'

# 本地停止单前缀
STOPORDERPREFIX = 'CtaStopOrder.'



#DATABASE_NAMES = [TICK_DB_NAME, MINUTE_DB_NAME,
#                  MINUTE_5_DB_NAME,MINUTE_15_DB_NAME,
#                  MINUTE_30_DB_NAME, MINUTE_60_DB_NAME, 
#                  DAILY_DB_NAME,WEEKLY_DB_NAME]

DATABASE_DICT = {
'5MIN' : MINUTE_5_DB_NAME,
'15MIN' : MINUTE_15_DB_NAME,
'30MIN' : MINUTE_30_DB_NAME,
'60MIN' : MINUTE_60_DB_NAME,
'D' : DAILY_DB_NAME,
'W' : WEEKLY_DB_NAME
}

DATABASE_NAMES = [MINUTE_5_DB_NAME, CHT_NODE_5_DB_NAME,
                  CHT_CB_5_DB_NAME]

# 引擎类型，用于区分当前策略的运行环境
ENGINETYPE_BACKTESTING = 'backtesting'  # 回测
ENGINETYPE_TRADING = 'trading'          # 实盘

# CTA模块事件
EVENT_CTA_LOG = 'eCtaLog'               # CTA相关的日志事件
EVENT_CTA_STRATEGY = 'eCtaStrategy.'    # CTA策略状态变化事件


########################################################################
class StopOrder(object):
    """本地停止单"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.vtSymbol = EMPTY_STRING
        self.orderType = EMPTY_UNICODE
        self.direction = EMPTY_UNICODE
        self.offset = EMPTY_UNICODE
        self.price = EMPTY_FLOAT
        self.volume = EMPTY_INT
        
        self.strategy = None             # 下停止单的策略对象
        self.stopOrderID = EMPTY_STRING  # 停止单的本地编号 
        self.status = EMPTY_STRING       # 停止单状态