import os
import re
import datetime as dt
import numpy as np
import pandas as pd

from remotewind import w
import database_assistant.DatabaseAssistant as da


class ProductPositions:

    def __init__(self,prodcode,prodname,stddbdir,divdir):
        self._prodcode = prodcode
        self._prodname = prodname
        self._stddbdir = stddbdir
        self._divdir = divdir

    def get_divinfo(self,date=None,sourcetype = 'bydb'):
        """ 提取分红数据 """
        if date is None:
            date = dt.date.today()
        divpath = os.path.join(self._divdir,''.join(['psz_',date.strftime('%Y%m%d'),'.csv']))
        if sourcetype=='byhand':
            with open(divpath,'r') as df:
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
            divdata = pd.read_csv(divpath)
            divdata = divdata.fillna(0)
            def fix_off(x):
                return x.split('.')[0]
            stkcd = divdata['hsStockCode'].map(fix_off)
            share = divdata['hfEESongPerTen']+divdata['hfEEZhuanPerTen']
            #cash = divdata['hfEEPaiPerTen']
            #div = pd.concat([stkcd,share,cash],ignore_index=True,axis=1)
            #div.columns = ['stkcd','share','cash']
            div = pd.concat([stkcd,share],ignore_index=True,axis=1)
            div.columns = ['stkcd','share']
        return div

    def get_holding_stk(self,date=None):
        """ 提取股票持仓信息并匹配分红送股信息 """
        if date is None:
            date = dt.date.today()
        datestr = date.strftime('%Y%m%d')
        ###### 从标准格式中提取股票持仓 #########
        tablename = '_'.join([self._prodcode,'positions','stocks',datestr])
        with da.DatabaseAssistant(dbdir=self._stddbdir) as stddb:
            conn_std = stddb.connection
            exeline = 'SELECT code AS stkcd,num,prenum,close AS prc FROM {0}'.format(tablename)
            posistk = pd.read_sql(sql=exeline,con=conn_std)
            ###### 匹配分红除息数据 ######
            div = self.get_divinfo(date=date)
            posistk = posistk.merge(div,left_on='stkcd',right_on='stkcd',how='left')
            posistk = posistk.fillna(0)
            posistk.loc[:,['num','prenum','prc','share']] = posistk.loc[:,['num','prenum','prc','share']].astype(float)
            posistk['num'] += posistk['prenum']*posistk['share']/10   # 补充分红股票数
            posistk.drop(['prenum','share'],axis=1,inplace=True)
            posistk['date'] = datestr
            # 剔除非股票持仓和零持仓代码 逆回购 理财产品等
            refound_sz = ['131810','131811','131800','131809','131801','131802','131803','131805','131806']
            refound_sh = ['204001','204007','204002','204003','204004','204014','204028','204091','204182']
            other_vars = ['131990','888880','SHRQ88','SHXGED','SZRQ88','SZXGED','511990','082271']
            tofilter = refound_sz+refound_sh+other_vars
            posistk = posistk[~ posistk['stkcd'].isin(tofilter)]
            posistk = posistk[posistk['num']>0]
            ####### 计算现金量 ######
            nonstkidx = posistk['stkcd'].isin(['999999'])
            purestocks = posistk[~nonstkidx]
            totasset = posistk[nonstkidx]
            stkval = sum(purestocks['num']*purestocks['prc'])
            if self._prodname in ('ls1','bq1','gd2'):
                with open(os.path.join(r'C:\Users\Jiapeng\Desktop\Net Value\百泉净值_rawdata\{0}_values'.format(self._prodname),datestr+'.txt'),'w') as f:
                    if self._prodname in ('ls1'):
                        f.writelines('stocks values: %f ' %stkval)
                    else:
                        f.writelines('stocks assets: %f \n' %totasset['num'].values[0])
            cashamt = totasset['num']*totasset['prc'] - stkval
            cashpd = pd.DataFrame([[datestr,'999156',cashamt.values[0],1]],columns=['date','stkcd','num','prc'])
            holdings = purestocks.append(cashpd,ignore_index=True)
            holdings['stkcd'] = holdings['stkcd'].astype(float)
            holdings = holdings.sort_values(by=['stkcd'],ascending=[1])
        return holdings.loc[:,['date','stkcd','num','prc']]

    def get_holding_fut(self,date=None,margin=0.3):
        if date is None:
            date = dt.date.today()
        datestr = date.strftime('%Y%m%d')
        tablename = '_'.join([self._prodcode,'positions','futures',datestr])
        with da.DatabaseAssistant(dbdir=self._stddbdir) as stddb:
            conn_std = stddb.connection
            # 只提取结算价相关
            exeline = 'SELECT code AS stkcd,num,settle AS prc,multi FROM {0} WHERE stkcd NOT IN (999998)'.format(tablename)
            posifut = pd.read_sql(sql=exeline,con=conn_std)
            def ctnametrans(name):
                ctdict = {'IF':'91','IC':'92','IH':'93'}
                if name[0:2] in ctdict:
                    return name.replace(name[0:2],ctdict[name[0:2]])
                else:
                    return name
            posifut['stkcd'] = posifut['stkcd'].map(ctnametrans)
            posifut['date'] = datestr
            nonfutidx = posifut['stkcd'].isin(['999997'])
            totsettle = posifut[nonfutidx]
            purefutures = posifut[~nonfutidx].copy()
            purefutures.loc[:,['num']] = purefutures['num'].abs() *purefutures['multi']*margin
            purefutures.drop('multi',axis=1,inplace=True)
            purefutures = purefutures[purefutures['num']>0]
            futval = sum(purefutures['num']*purefutures['prc'])
            cashamt = totsettle['num']*totsettle['prc'] - futval
            if self._prodname in ('ls1','bq1','gd2'):
                with open(os.path.join(r'C:\Users\Jiapeng\Desktop\Net Value\百泉净值_rawdata\{0}_values'.format(self._prodname),datestr+'.txt'),'a+') as f:
                    if self._prodname in ('ls1'):
                        f.writelines('futures values: %f ' %futval)
                    else:
                        f.writelines('futures assets stl: %f ' %totsettle['num'].values[0])
            cashpd = pd.DataFrame([[datestr,'999157',cashamt.values[0],1]],columns=['date','stkcd','num','prc'])
            holdings = purefutures.append(cashpd,ignore_index=True)
            holdings['stkcd'] = holdings['stkcd'].astype(float)
        return holdings.loc[:,['date','stkcd','num','prc']]


if __name__=='__main__':
    sdb = r'E:\Baiquan_Positions\data\positions_database\BaiQuan1_standard_tables.db'
    ddb = r'E:\calc_dividend\holding_gen\dividendfiles'
    obj = ProductInfo(prodcode='BaiQuan1',prodname='test',stddbdir=sdb,divdir=ddb)
    print(obj.get_holding_stk(date=dt.datetime(2017,9,21)))
    print(obj.get_holding_fut(date=dt.datetime(2017,9,21)))