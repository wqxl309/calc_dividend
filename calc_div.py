import os
import re
import sys
import sqlite3
import datetime as dt
import numpy as np
import pandas as pd

import database


def take_holding_fmt(dbdir,divdir,product,date,outdir):
    # 提取分红数据
    with open(divfile,'r') as df:
        stkcd = []
        share = []
        cash = []
        for f in df.readlines():
            temp = f.split()
            cd = re.search('[\d]{6}',temp[0]).group()
            if cd[0] in ('0','3'):
                stkcd.append(cd+'.SZ')
            else:
                stkcd.append(cd+'.SH')
            match = re.search('转[\d]*股',temp[1])
            if match is None:
                shr = 0
            else:
                shr = re.search('[\d]+',match.group()).group()
            share.append(shr)

            match2 = re.search('派[\.\d]+元',temp[1])
            if match2 is None:
                ch = 0
            else:
                ch = re.search('[\.\d]+',match2.group()).group()
            cash.append(ch)
    div = pd.DataFrame({'stkcd':stkcd, 'share':share, 'cash':cash})

    # 提取持仓数据
    with database.DatabaseConnect(dbdir) as conn:
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
        else:
            outtitle = ('证券代码','证券数量','当前价')
        exeline = ''.join(['SELECT ',','.join(outtitle),' FROM ',tablename+'_afternoon'])
        holdings = pd.read_sql(exeline,conn)
        holdings.columns = ['stkcd','num','prc']
        if product == 'ls1':
            asset = float( open(r'E:\calc_dividend\holding_gen\ls1_value.txt').readline() )
        elif product == 'bq1':
            asset = float( open(r'E:\calc_dividend\holding_gen\bq1_value.txt').readline() )
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
    holdings = holdings[~ holdings['stkcd'].isin(['131990','888880','SHRQ88','SHXGED','SZRQ88','SZXGED'])]
    holdings = holdings[holdings['num']>0]
    # 计算现金量
    cashamt = (asset - np.sum(holdings['num']*holdings['prc'])) * 0.95
    temp = [[date,'999156',cashamt,0,0,1]]
    holdings = pd.concat([holdings,pd.DataFrame(temp,columns=['date','stkcd','num','cash','share','prc'])],ignore_index=True)
    holdings['stkcd'] = holdings['stkcd'].map(lambda x : float(x))
    # 转换代码类型并排序
    holdings = holdings.sort_values(by=['stkcd'],ascending=[1])
    holdings.to_csv(outdir,header = True,index=False)





if __name__ == '__main__':
    basepath = r'E:\calc_dividend\holding_gen'
    raw = 'rawholding'
    file = 'holdingfiles'
    newfile = 'newfiles'
    db = 'holdingdb'
    dividend = 'dividendfiles'

    date = str(dt.datetime.strftime(dt.date.today(),'%Y%m%d'))
    #inputdate = dt.datetime(2017,5,4,17,0,0)

    products = ['bq1','bq2','jq1','hj1','gd2','ls1']
    textvars = {'bq1': ('备注','股东代码','证券代码','证券名称','资金帐号'),
                'bq2': ('股东代码','证券代码','证券名称'),
                'jq1': ('股东代码','证券代码','证券名称'),
                'hj1': ('股东代码','证券代码','证券名称'),
                'gd2': ('股东代码','证券代码','证券名称'),
                'ls1': ('产品名称','到期日','股东账号','账号名称','证券代码','证券名称','状态','资金账号')
                }
    divfile = os.path.join(basepath,dividend,'dividend_'+date+'.txt')

    newfiletoday = os.path.join(basepath,date)
    if not os.path.exists(newfiletoday):
        os.mkdir(newfiletoday)
    print(newfiletoday)
    for p in products:
        rawfile = os.path.join(basepath,raw,p,p+'_'+date+'.csv')
        rawdb = os.path.join(basepath,db,p+'.db')
        obj = database.ClientToDatabase(rawdb,p)
        # # obj.set_holdtbname(inputdate=inputdate)
        obj.holdlist_to_db(rawfile,textvars[p],currencymark='币种',codemark='证券代码',replace=True)
        outfile = os.path.join(basepath,newfile,p, p+'_'+date+'.csv')
        take_holding_fmt(dbdir=rawdb,divdir=divfile,product=p,date=date,outdir=outfile)
        os.system ("copy %s %s" % (outfile, os.path.join(newfiletoday,p+'_'+date+'.csv')))

