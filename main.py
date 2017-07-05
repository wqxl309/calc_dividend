

import datetime as dt
import os
import configparser as cp
import sys

import numpy as np
import pandas as pd

from help_functions import *
from products_info import *
#from products_configures import *

if __name__ == '__main__':
    today = dt.datetime.today()
    #today = dt.datetime(year=2017,month=7,day=4)
    todaystr = today.strftime('%Y%m%d')

    # 处理百泉砺石一号的特殊情况
    tempfile = r'C:\Users\Jiapeng\Desktop\tempholdings'
    file_ls1 = os.path.join(tempfile,''.join(['ls1_',todaystr,'.csv']))
    temp_ls1 = os.path.join(tempfile,''.join(['ls1_',todaystr,'_filtered.csv']))
    ls1_filter(infile=file_ls1,outfile=temp_ls1)
    os.system('del %s' % file_ls1)
    os.system('move %s %s' %(temp_ls1,file_ls1))
    os.system('call %s ' %r'E:\calc_dividend\holding_gen\rawholding\file2folder.bat')

    basepath = r'E:\calc_dividend\holding_gen'
    raw = 'rawholding'
    file = 'holdingfiles'
    newfile = 'newfiles'
    db = 'holdingdb'
    dividend = 'dividendfiles'
    configpath = r'E:\realtime_monitors\realtime_returns\configures'
    #divdir = os.path.join(basepath,dividend,'dividend_'+today+'.txt')
    divdir = os.path.join(basepath,dividend,'psz_'+todaystr+'.csv')
    newfiletoday = os.path.join(basepath,todaystr)

    products = ['bq1','bq2','bq3','jq1','hj1','gd2','ls1','xy7','ms1']
    nametrans = {'bq1':'BaiQuan1',
                 'bq2':'BaiQuan2',
                 'bq3':'BaiQuan3',
                 'jq1' :'BaiQuanJinQu1',
                 'hj1':'BaiQuanHuiJin1',
                 'gd2':'GuoDaoLiShi2',
                 'ls1' : 'BaiQuanLiShi1',
                 'xy7':'XingYing7',
                 'ms1':'BaiQuanMS1'}

    # 创建当日新文件夹
    if not os.path.exists(newfiletoday):
        os.mkdir(newfiletoday)
    print(newfiletoday)

    for p in products:
        print('updating %s ...' %p)
        # 构建基础路径
        rawfile = os.path.join(basepath,raw,p,p+'_'+todaystr+'.csv')
        rawdb = os.path.join(basepath,db,''.join(['calcdiv_',p,'.db']))
        outfile = os.path.join(basepath,newfile,p, p+'_'+todaystr+'.csv')
        if p=='ls1':
            othersource = os.path.join(basepath,'ls1_value','ls1_value_%s.txt' %todaystr )
        else:
            othersource = None
        # 读取产品配置
        cf = cp.ConfigParser()
        cf.read(os.path.join(configpath,'.'.join([nametrans[p],'ini'])))
        # 创建对象
        obj = product_info(dbdir=rawdb,divdir=divdir,product=p,title_hold=cf.get('stocks','vars_fordiv').split(','),
                           title_asset=cf.get('stocks','vars_value').split(','),textvars=cf.get('stocks','text_vars_hold').split(','),
                           cwdir=dict(cf.items('cwstate')),logdir=dict(cf.items('blog')))
        # 股票存入数据库，并声称持仓单
        obj.stkhold_to_db(rawfiledir=rawfile,date=todaystr)
        holdings = obj.get_holding_stk(date=todaystr,othersource=othersource)
        # 期货生成持仓单
        if cf.options('blog'):
            holdfut = obj.get_holding_fut(date=today)
            holdings = pd.concat([holdings,holdfut],axis=0,ignore_index=True)
        # 输出持仓单到本地
        holdings.to_csv(outfile,header = True,index=False)
        # 结果推送
        os.system ("copy %s %s" % (outfile, os.path.join(newfiletoday,p+'_'+todaystr+'.csv')))
        os.system ("copy %s %s" % (outfile, os.path.join(r'\\JASONCHEN-PC\Positions','Positions'+todaystr,p+'_'+todaystr+'.csv')))  # 推送


