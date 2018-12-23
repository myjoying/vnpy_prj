# encoding: UTF-8

import sys
import json
from datetime import datetime
from time import time, sleep
import tushare as ts
import pandas as pd
import talib as ta
from pymongo import MongoClient, ASCENDING

from vnpy.trader.vtObject import VtBarData
from vnpy.trader.app.ctaStrategy.ctaBase import *



# 加载配置
config = open('config.json')
setting = json.load(config)

MONGO_HOST = setting['MONGO_HOST']
MONGO_PORT = setting['MONGO_PORT']
SYMBOLS = setting['SYMBOLS']
START = setting["START"]
END = setting["END"]
FREQS = setting["FREQ"]

mc = MongoClient(MONGO_HOST, MONGO_PORT)        # Mongo连接


DB_NAME_DICT = {
'1MIN': MINUTE_DB_NAME,
'5MIN':MINUTE_5_DB_NAME,
'15MIN':MINUTE_15_DB_NAME,
'30MIN':MINUTE_30_DB_NAME,
'60MIN':MINUTE_60_DB_NAME,
'D':DAILY_DB_NAME,
'W':WEEKLY_DB_NAME
}

#----------------------------------------------------------------------
def generateExchange(symbol):
    """生成VT合约代码"""
    if symbol[0:2] in ['60', '51']:
        exchange = 'SSE'
    elif symbol[0:2] in ['00', '15', '30']:
        exchange = 'SZSE'
    else:
        exchange = 'QH'
    return exchange

#----------------------------------------------------------------------
def generateVtBar(row):
    """生成K线"""
    bar = VtBarData()
    
    bar.symbol = row['code']
    bar.exchange = generateExchange(bar.symbol)
    bar.vtSymbol = '.'.join([bar.symbol, bar.exchange])
    bar.open = row['open']
    bar.high = row['high']
    bar.low = row['low']
    bar.close = row['close']
    bar.volume = row['vol']
    #bar.volume = row['volume']
    bar.datetime = row.name
    #bar.datetime = datetime.strptime(row.date,"%Y-%m-%d %H:%M")
    bar.date = bar.datetime.strftime("%Y%m%d")
    bar.time = bar.datetime.strftime("%H:%M:%S")
    
    return bar

#----------------------------------------------------------------------
def downBarBySymbol(symbol, start_date=None, end_date=None, freq='D'):
    """下载某一合约的分钟线数据"""
    start = time()
    
    db_freq = freq
    #if freq[0] in ['0', '1', '2','3','4','5','6','7','8','9']:
        #db_freq = freq + 'MIN'
    
    db = mc[DB_NAME_DICT[db_freq]]                         # 数据库
    cl = db[symbol]
    cl.ensure_index([('datetime', ASCENDING)], unique=True)         # 添加索引


    if generateExchange(symbol)=="QH":
        asset = 'X'
    else:
        asset = 'E'
        
    asset = 'INDEX'    
    df = ts.bar(symbol, conn=ts.get_apis(), freq=freq, asset=asset, start_date=start_date, end_date=end_date, adj='qfq')
    #df = ts.get_hist_data(symbol,start=start_date, end=end_date, ktype=freq)
    
    df = df.sort_index()
    
    for ix, row in df.iterrows():
        bar = generateVtBar(row)
        d = bar.__dict__
        flt = {'datetime': bar.datetime}
        cl.replace_one(flt, d, True)            

    end = time()
    cost = (end - start) * 1000

    print u'合约%s 周期%s数据下载完成%s - %s，耗时%s毫秒' %(symbol, freq, df.index[0], df.index[-1], cost)

    
#----------------------------------------------------------------------
def downloadBarData():
    """下载所有配置中的合约的K线数据"""
    print '-' * 50
    print u'开始下载合约K线数据'
    print '-' * 50
    
    # 添加下载任务
    start_date = datetime.strptime(START, "%Y-%m-%d")
    end_date = datetime.strptime(END, "%Y-%m-%d")

    failure_dict ={}
    for symbol in SYMBOLS:
        for freq in FREQS:
            try:
                downBarBySymbol(str(symbol), start_date, end_date, freq)
            except:
                print '首次下载失败：%s--%s' %( symbol,freq)
                if symbol not in failure_dict.keys():
                    freq_list = []
                    freq_list.append(freq)
                    failure_dict[symbol] = freq_list
                else:
                    freq_list = failure_dict[symbol]
                    freq_list.append(freq)
                    failure_dict[symbol] = freq_list
    
    for symbol,freq_list in failure_dict.items():
        for freq in freq_list:
            print '补充下载:%s--%s'%(symbol, freq)
            try:
                downBarBySymbol(str(symbol), start_date, end_date, freq)
            except:
                print '下载失败：%s--%s'%(symbol,freq)
                
    print '-' * 50
    print u'合约K线数据下载完成'
    print '-' * 50
    
def indexGeneratorAndStore():
    
    for symbol in SYMBOLS:
        for freq in FREQS:
            print u'开始指标计算合约%s 周期%s' %(symbol, freq)
            
            db = mc[DB_NAME_DICT[freq]]
            collection = db[symbol]
            data =  pd.DataFrame(list(collection.find()))
            
            data['SMA5'] = ta.SMA(data['close'].values, timeperiod = 5)  #5日均线
            data['SMA10'] = ta.SMA(data['close'].values, timeperiod = 10)  #10日均线
            macd_talib, signal, hist = ta.MACD(data['close'].values,fastperiod=12, slowperiod=26, signalperiod=9 )
            data['DIF'] = macd_talib #DIF
            data['DEA'] = signal #DEA
            data['MACD'] = hist #MACD 
            
            macd_talib2, signal2, hist2 = ta.MACD(data['close'].values,fastperiod=24, slowperiod=52, signalperiod=9 )
            data['DIF2'] = macd_talib2 #DIF
            data['DEA2'] = signal2 #DEA
            data['MACD2'] = hist2 #MACD     
            
            for item in data.index:
                collection.update_one({'_id':data.ix[item, '_id']}, {'$set':{'SMA5':data.ix[item, 'SMA5'],\
                                                                             'SMA10':data.ix[item, 'SMA10'],\
                                                                             'DIF':data.ix[item, 'DIF'],\
                                                                             'DEA':data.ix[item, 'DEA'],\
                                                                             'MACD':data.ix[item, 'MACD'],\
                                                                             'DIF2':data.ix[item, 'DIF2'],\
                                                                             'DEA2':data.ix[item, 'DEA2'],\
                                                                             'MACD2':data.ix[item, 'MACD2']}})


    
