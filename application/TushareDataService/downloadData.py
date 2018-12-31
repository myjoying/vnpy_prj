# encoding: UTF-8

"""
立即下载数据到数据库中，用于手动执行更新操作。
"""
import sys, getopt
from dataService import *

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

def main(argv):
    try:
        # 这里的 h 就表示该选项无参数，i:表示 i 选项后需要有参数
        opts, args = getopt.getopt(argv, "hdi",[])
    except getopt.GetoptError:
        print 'Error arguments:'
        print '-h   help'
        print '-d   get history data'
        print '-i   generate data index using talib'
        sys.exit(2)

    op = 0
    for opt, arg in opts:
        if opt == "-h":
            print 'arguments:'
            print '-h   help'
            print '-d   get history data'
            print '-i   generate data index using talib'
        if opt == '-d':
            op = 1 + op
        if opt == '-i':
            op = 10 + op

    if op == 1:
        downloadBarData()
    elif op == 10:
        indexGeneratorAndStore()
    else:
        downloadBarData()
        indexGeneratorAndStore()


    print '完成处理'


if __name__ == '__main__':
    main(sys.argv[1:])
