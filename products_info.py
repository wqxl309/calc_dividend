import os
import re
import sys
import sqlite3
import datetime as dt
import time
import numpy as np
import pandas as pd

from remotewind import w

#sys.path.append(r'E:\realtime_monitors\realtime_returns\src\database_assistant')
import database_assistant.database_assistant as dbass
import raw_holding_process as rhp


class product_info:

    def __init__(self,dbdir,divdir,product,title_hold,title_asset,textvars,cwdir,logdir):
        self._divdir = divdir
        self._dbdir = dbdir
        self._product = product
        self._title_hold = title_hold
        self._title_asset = title_asset
        self._textvars = textvars
        self._cwdir = cwdir
        self._logdir = logdir

    def stkhold_to_db(self,rawfiledir,date=None,currencymark='币种',codemark='证券代码',replace=True):
        if date is None:
            date = dt.date.today().strftime('%Y%m%d')
        tablename = '_'.join([self._product,'stocks',date])
        with dbass.db_assistant(dbdir=self._dbdir) as holddb:
            conn = holddb.connection
            c = conn.cursor()
            with open(rawfiledir,'r') as fl:
                rawline = fl.readline()
                startwrite = False
                summary = False
                while rawline:
                    line = rawline.strip().split(',')
                    if not startwrite:
                        if currencymark: # 在找到详细数据之前会先查找汇总,如果不需要汇总则直接寻找标题
                            if not summary:    # 检查持仓汇总部分
                                if currencymark in line:  #寻找汇总标题
                                    stitles = line
                                    currpos = stitles.index(currencymark)
                                    stitlecheck = dbass.db_assistant.gen_table_titles(stitles,{'TEXT':(currencymark,)})
                                    stitletrans = stitlecheck['typed_titles']
                                    stitle_empty = stitlecheck['empty_pos']
                                    dbass.db_assistant.create_db_table(c,tablename+'_summary',stitletrans,replace)
                                    rawline = fl.readline()
                                    summary = True
                                    continue
                            else:
                                if line[currpos] == '人民币':   # 读取人民币对应的一行
                                    exeline = ''.join(['INSERT INTO ', tablename, '_summary VALUES (', ','.join(['?']*len(stitletrans)), ')'])
                                    newline = []
                                    for dumi in range(len(line)):
                                        if not stitle_empty[dumi]:
                                            newline.append(line[dumi])
                                    c.execute(exeline, newline)
                                    conn.commit()
                        #寻找正表标题
                        if codemark in line:
                            titles = line
                            titlecheck = dbass.db_assistant.gen_table_titles(titles,{'TEXT':self._textvars})
                            titletrans = titlecheck['typed_titles']
                            # title_empty = titlecheck['empty_pos']   # 此处尤其暗藏风险，假设正表数据没有空列
                            dbass.db_assistant.create_db_table(c,tablename,titletrans,replace)
                            rawline = fl.readline()
                            startwrite = True
                            continue
                    else:
                        exeline = ''.join(['INSERT INTO ', tablename, ' VALUES (', ','.join(['?']*len(line)), ')'])
                        c.execute(exeline, line)
                        conn.commit()
                    rawline = fl.readline()
            if startwrite: #实现写入并退出循环
                print('Table '+tablename+' updated to database !')
            else:  # 未能实现写入
                print('Table '+tablename+' cannot read the main body, nothing writen !')


    def get_divinfo(self,sourcetype = 'bydb'):
        """ 提取分红数据 """
        if sourcetype=='byhand':
            with open(self._divdir,'r') as df:
                stkcd = []
                share = []
                cash = []
                for f in df.readlines():
                    temp = f.split()
                    cd = re.search('[\d]{6}',temp[0]).group()
                    stkcd.append(cd)
                    # if cd[0] in ('0','3'):
                    #     stkcd.append(cd+'.SZ')
                    # else:
                    #     stkcd.append(cd+'.SH')
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
        elif sourcetype=='bydb':
            divdata = pd.read_csv(self._divdir)
            divdata = divdata.fillna(0)
            def fix_off(x):
                return x.split('.')[0]
            stkcd = divdata['hsStockCode'].map(fix_off)
            share = divdata['hfEESongPerTen']+divdata['hfEEZhuanPerTen']
            cash = divdata['hfEEPaiPerTen']
            div = pd.concat([stkcd,share,cash],ignore_index=True,axis=1)
            div.columns = ['stkcd','share','cash']
            div = div.sort_values(by=['stkcd'],ascending=[1])
        return div

    def stk_holdinfo(self,date=None):
        if date is None:
            date = dt.date.today().strftime('%Y%m%d')
        with dbass.db_assistant(dbdir=self._dbdir) as holddb:
            conn = holddb.connection
            tablename = '_'.join([self._product,'stocks',date])
            exeline = ''.join(['SELECT ',','.join(self._title_hold),' FROM ',tablename])
            holdings = pd.read_sql(exeline,conn)
            holdings.columns = ['stkcd','num','prenum','prc']
        return holdings

    def stk_asset(self,date=None,othersource=None):
        if date is None:
            date = dt.date.today().strftime('%Y%m%d')
        if othersource is not None:
            asset = float( open(othersource).readline())
        else:
            with dbass.db_assistant(dbdir=self._dbdir) as holddb:
                conn = holddb.connection
                tablename = '_'.join([self._product,'stocks',date,'summary'])
                exeline = ''.join(['SELECT ',','.join(self._title_asset),' FROM ',tablename])
                values = conn.execute(exeline).fetchall()
                asset = np.sum(values[0])
        return asset

    def get_holding_stk(self,date=None,othersource=None):
        """ 提取股票持仓信息并匹配分红送股信息 """
        if date is None:
            date = dt.date.today().strftime('%Y%m%d')
        div = self.get_divinfo()
        holdings = self.stk_holdinfo(date = date)
        asset = self.stk_asset(date = date,othersource=othersource)
        # 匹配分红除息数据
        shape = holdings.shape
        idx1 = holdings['stkcd'].isin(div['stkcd'])
        idx2 = div['stkcd'].isin(holdings['stkcd'])
        tempval = np.zeros([shape[0],shape[1]-2])
        tempval[idx1.values,:] = div.loc[idx2.tolist(),['cash','share']].values
        holdings['date'] = date
        #holdings['cash'] = tempval[:,0]
        holdings['share'] = tempval[:,1]
        holdings['num'] = holdings['num'] + holdings['share']*holdings['prenum']/10
        holdings = holdings[['date','stkcd','num','prc']]
        # 剔除非股票持仓和零持仓代码
        # 逆回购 理财产品等
        refound_sz = ['131810','131811','131800','131809','131801','131802','131803','131805','131806']
        refound_sh = ['204001','204007','204002','204003','204004','204014','204028','204091','204182']
        other_vars = ['131990','888880','SHRQ88','SHXGED','SZRQ88','SZXGED','511990']
        tofilter = refound_sz+refound_sh+other_vars
        holdings = holdings[~ holdings['stkcd'].isin(tofilter)]
        holdings = holdings[holdings['num']>0]
        # 计算现金量
        cashamt = (asset - np.sum(holdings['num']*holdings['prc'])) * 0.95  # 保留5%
        temp = [[date,'999156',cashamt,1]]
        holdings = pd.concat([holdings,pd.DataFrame(temp,columns=['date','stkcd','num','prc'])],ignore_index=True)
        holdings['stkcd'] = holdings['stkcd'].map(lambda x : float(x))
        # 转换代码类型并排序
        holdings = holdings.sort_values(by=['stkcd'],ascending=[1])
        return holdings

    def get_holding_fut(self,date=None,prctype='settle',margin=0.3):
        w.start()
        if date is None:
            date = dt.date.today()
        futobj = rhp.rawholding_futures(hold_dbdir=self._dbdir,pofname=self._product,logdir=self._logdir,cwdir=self._cwdir)
        holding = futobj.holdlist_format(date=date,prctype=prctype,preday=False)
        holding['val'] = holding['num']*holding['prc']*holding['multi']
        totval = futobj.get_totval(date=date,prctype=prctype)
        cashkeep = np.sum(np.abs(holding['val']*(margin+0.1)))
        cash = totval - cashkeep
        vals = [date.strftime('%Y%m%d'),'999157',cash,1]
        holding = pd.DataFrame([vals],columns=['date','stkcd','num','prc'])
        return holding


if __name__=='__main__':
    conn = sqlite3.connect(r'E:\calc_dividend\holding_gen\holdingdb\calcdiv_ls1.db')
    c = conn.cursor()
    print( c.execute('SELECT 当前拥股,可用数量 FROM ls1_stocks_20170704').fetchall() )