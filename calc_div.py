import os
import re
import sys
import sqlite3
import datetime as dt
import numpy as np
import pandas as pd

from remotewind import w
import holding_generate

sys.path.append(r'E:\realtime_monitors\realtime_returns\src\database_assistant')
import database_assistant




def take_holding_fmt(dbdir,divdir,product,date):
    # 提取分红数据
    div = get_divinfo(divdir=divdir)

    # 提取持仓数据
    with holding_generate.DatabaseConnect(dbdir) as conn:
        #tables = conn.execute(' SELECT name FROM sqlite_master WHERE type=\'table\' ').fetchall()
        tablename = product+'_'+date
        cols = conn.execute('PRAGMA table_info('+tablename+'_afternoon )').fetchall()
        cols = [c[1] for c in cols]
        if ('证券代码' in cols) and ('库存数量' in cols) and ('当前价' in cols):
            outtitle = ('证券代码','库存数量','当前价')
        elif ('证券代码' in cols) and ('参考持股' in cols) and ('当前价' in cols):
            outtitle = ('证券代码','参考持股','当前价')
        elif ('证券代码' in cols) and ('当前拥股' in cols) and ('最新价' in cols):
            outtitle = ('证券代码','当前拥股','最新价')
        elif ('证券代码' in cols) and ('实际数量' in cols) and ('当前价' in cols):
            outtitle = ('证券代码','实际数量','当前价')
        else:
            outtitle = ('证券代码','证券数量','当前价')
        exeline = ''.join(['SELECT ',','.join(outtitle),' FROM ',tablename+'_afternoon'])
        holdings = pd.read_sql(exeline,conn)
        holdings.columns = ['stkcd','num','prc']
        if product == 'ls1':
            asset = float( open(''.join([r'E:\calc_dividend\holding_gen\ls1_value_',date,'.txt'])).readline())
        elif product == 'bq1':
            #asset = float( open(''.join([r'E:\calc_dividend\holding_gen\bq1_value_',date,'.txt'])).readline())
            exeline = ''.join(['SELECT 可用,参考市值 FROM ',tablename+'_afternoon_summary'])
            values = conn.execute(exeline).fetchall()
            asset = np.sum(values[0])
        else:
            exeline = ''.join(['SELECT 资产 FROM ',tablename+'_afternoon_summary'])
            asset = conn.execute(exeline).fetchall()[0][0]
    # 匹配分红除息数据
    shape = holdings.shape
    idx1 = holdings['stkcd'].isin(div['stkcd'])
    idx2 = div['stkcd'].isin(holdings['stkcd'])
    tempval = np.zeros([shape[0],shape[1]-1])
    tempval[idx1.values,:] = div.loc[idx2.tolist(),['cash','share']].values
    holdings.insert(0,'date',np.ones([shape[0],1])*float(date))
    holdings.insert(shape[1],'cash',tempval[:,0])
    holdings.insert(shape[1]+1,'share',tempval[:,1])
    # 取消 .SH .SZ
    newcd = [cd.split('.')[0] for cd in holdings['stkcd'].tolist()]
    holdings['stkcd'] = newcd
    # 剔除非股票持仓和零持仓代码
    # 逆回购 理财产品等
    refound_sz = ['131810','131811','131800','131809','131801','131802','131803','131805','131806']
    refound_sh = ['204001','204007','204002','204003','204004','204014','204028','204091','204182']
    other_vars = ['131990','888880','SHRQ88','SHXGED','SZRQ88','SZXGED','511990']
    tofilter = refound_sz+refound_sh+other_vars
    holdings = holdings[~ holdings['stkcd'].isin(tofilter)]
    holdings = holdings[holdings['num']>0]
    # 计算现金量
    cashamt = (asset - np.sum(holdings['num']*holdings['prc'])) * 0.95
    temp = [[date,'999156',cashamt,0,0,1]]
    holdings = pd.concat([holdings,pd.DataFrame(temp,columns=['date','stkcd','num','cash','share','prc'])],ignore_index=True)
    holdings['stkcd'] = holdings['stkcd'].map(lambda x : float(x))
    # 转换代码类型并排序
    holdings = holdings.sort_values(by=['stkcd'],ascending=[1])
    return holdings
    #holdings.to_csv(outdir,header = True,index=False)





if __name__ == '__main__':

    today = dt.date.today().strftime('%Y%m%d')

    basepath = r'E:\calc_dividend\holding_gen'
    raw = 'rawholding'
    file = 'holdingfiles'
    newfile = 'newfiles'
    db = 'holdingdb'
    dividend = 'dividendfiles'

    products = ['bq1','bq2','jq1','hj1','gd2','ls1','xy7']
    textvars = {'bq1': ('备注','股东代码','证券代码','证券名称','资金帐号'),
                'bq2': ('股东代码','证券代码','证券名称'),
                'jq1': ('股东代码','证券代码','证券名称'),
                'hj1': ('股东代码','证券代码','证券名称'),
                'gd2': ('股东代码','证券代码','证券名称'),
                'ls1': ('产品名称','到期日','股东账号','账号名称','证券代码','证券名称','状态','资金账号'),
                'xy7': ('股东代码','证券代码','证券名称','交易所名称')
                }


    settleprc = w.wsd('IC1706.CFE','settle',).Data[0][0]
    print(settleprc)
    futinfo = {'bq1':{'tot_value': 2235598.22 ,
                      'IC1706.CFE': { 'settle': settleprc, 'trdside': -1,'multiplier': 200,'holdnum':np.array([2]) } },
               'bq2':{'tot_value': 3910775.24 ,
                      'IC1706.CFE': { 'settle': settleprc, 'trdside': -1,'multiplier': 200,'holdnum':np.array([4]) } },
               'gd2' :{'tot_value': 3893882.86,
                       'IC1706.CFE': { 'settle': settleprc, 'trdside': -1,'multiplier': 200, 'holdnum':np.array([4]) } },
               'ls1' :{'tot_value': 8697636.87,
                        'IC1706.CFE': { 'settle': settleprc, 'trdside': -1,'multiplier': 200, 'holdnum':np.array([8]) } },
               'xy7' :{'tot_value': 4279250.46,
                       'IC1706.CFE': { 'settle': settleprc, 'trdside': -1,'multiplier': 200, 'holdnum':np.array([6]) } }
               }


    date = str(dt.datetime.strftime(dt.date.today(),'%Y%m%d'))
    divfile = os.path.join(basepath,dividend,'dividend_'+date+'.txt')
    newfiletoday = os.path.join(basepath,date)
    if not os.path.exists(newfiletoday):

        os.mkdir(newfiletoday)
    print(newfiletoday)

    for p in products:
        rawfile = os.path.join(basepath,raw,p,p+'_'+date+'.csv')
        rawdb = os.path.join(basepath,db,p+'.db')
        obj = holding_generate.ClientToDatabase(rawdb,'',p)
        #inputdate = dt.datetime(2017,5,4,17,0,0)
        #obj.set_holdtbname(inputdate=inputdate)

        obj.holdlist_to_db(rawfile,textvars[p],currencymark='币种',codemark='证券代码',replace=True)

        holdings = take_holding_fmt(dbdir=rawdb,divdir=divfile,product=p,date=date)
        fut = futinfo.get(p)
        if fut:
            futholding = holding_generate.futures_holding(date,fut,outputfmt='for_calcdiv')['cashinfo']
            holdings = holdings.append(futholding,ignore_index=True)

        outfile = os.path.join(basepath,newfile,p, p+'_'+date+'.csv')
        holdings.to_csv(outfile,header = True,index=False)
        os.system ("copy %s %s" % (outfile, os.path.join(newfiletoday,p+'_'+date+'.csv')))
        os.system ("copy %s %s" % (outfile, os.path.join(r'\\JASONCHEN-PC\Positions','Positions'+date,p+'_'+date+'.csv')))  # 推送


