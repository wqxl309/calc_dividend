import datetime as dt
import os
import pandas as pd

basepath = r'E:\calc_dividend\holding_gen'
raw = 'rawholding'
file = 'holdingfiles'
newfile = 'newfiles'
db = 'holdingdb'
dividend = 'dividendfiles'


textvars = {'bq1': ('备注','股东代码','证券代码','证券名称','资金帐号'),
            'bq2': ('股东代码','证券代码','证券名称'),
            'bq3': ('股东代码','证券代码','证券名称'),
            'jq1': ('股东代码','证券代码','证券名称'),
            'hj1': ('股东代码','证券代码','证券名称'),
            'gd2': ('股东代码','证券代码','证券名称'),
            'ls1': ('产品名称','到期日','股东账号','账号名称','证券代码','证券名称','状态','资金账号'),
            'xy7': ('股东代码','证券代码','证券名称','交易所名称')
            }

assetvars = {'bq1': ['可用','参考市值'],
            'bq2': ['资产'],
            'bq3': ['资产'],
            'jq1':['资产'],
            'hj1': ['资产'],
            'ls1': [],
            'gd2': ['资产'],
            'xy7': ['资产'],
            }

holdvars = { 'bq1': ('证券代码','参考持股','可用股份','当前价'),
             'bq2': ('证券代码','证券数量','可卖数量','当前价'),
             'bq3': ('证券代码','库存数量','可卖数量','当前价'),
             'jq1': ('证券代码','证券数量','可卖数量','当前价'),
             'hj1': ('证券代码','库存数量','可卖数量','当前价'),
             'ls1': ('证券代码','当前拥股','可用数量','最新价'),
             'gd2': ('证券代码','证券数量','可卖数量','当前价'),
             'xy7': ('证券代码','实际数量','可卖数量','当前价'),
             }

logdir = {
    'bq1':{'near1':r'\\BQ1_ICHEDGE\blog'},
    'bq2':{'near1':r'\\BQ2_ICHEDGE\blog'},
    'bq3':{'near1':r'\\BQ2_ICHEDGE\blog'},
    'gd2':{'near1':r'\\GD2_ICHEDGE\blog','back2':r'\\gd2_yyichedge\blog'},
    'ls1':{'near1':r'\\LS1_ICHEDGE\blog'},
    'xy7':{'near1':r'\\XY7_ICHEDGE\blog'}
}

cwdir = {
    'bq1':{'near1':r'\\BQ1_ICHEDGE\cwstate'},
    'bq2':{'near1':r'\\BQ2_ICHEDGE\cwstate'},
    'bq3':{'near1':r'\\BQ3_ICHEDGE\cwstate'},
    'gd2':{'near1':r'\\GD2_ICHEDGE\cwstate','back2':r'\\gd2_yyichedge\cwstate'},
    'ls1':{'near1':r'\\LS1_ICHEDGE\cwstate'},
    'xy7':{'near1':r'\\XY7_ICHEDGE\cwstate'}
}


if __name__=='__main__':
    dir = r'E:\calc_dividend\holding_gen\dividendfiles\psz_20170615.csv'
    t = pd.read_csv(dir)
    t=t.fillna(0)
    print(t)