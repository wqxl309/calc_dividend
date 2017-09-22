
import configparser as cp
import datetime as dt
import os

import pandas as pd

from products_info import ProductInfo


if __name__ == '__main__':
    today = dt.datetime.today()
    #today = dt.datetime(year=2017,month=9,day=21)
    todaystr = today.strftime('%Y%m%d')
    ####### 构建基础路径 ########
    basepath = r'E:\calc_dividend\holding_gen'
    configdir = r'E:\Baiquan_Positions\configures'
    divdir = os.path.join(basepath,'dividendfiles')
    dbdir = 'E:\Baiquan_Positions\data\positions_database'
    ######## 创建当日新文件夹 #######
    newfiletoday = os.path.join(basepath,'position_lists',todaystr)
    if not os.path.exists(newfiletoday):
        os.mkdir(newfiletoday)

    products = ['bq1','bq2','bq3','jq1','hj1','ms1','ls1','gd2']
    nametrans = {'bq1':'BaiQuan1',
                 'bq2':'BaiQuan2',
                 'bq3':'BaiQuan3',
                 'jq1' :'BaiQuanJinQu1',
                 'hj1':'BaiQuanHuiJin1',
                 'ms1':'BaiQuanMS1',
                 'gd2':'GuoDaoLiShi2',
                 'ls1' : 'BaiQuanLiShi1'
                 }

    for p in products:
        print('updating %s ...' %p)
        # 读取产品配置
        pcode = nametrans[p]
        base_parser = cp.ConfigParser()
        base_parser.read(os.path.join(configdir,'.'.join([pcode,'ini'])))
        # 创建对象
        stddbdir = os.path.join(dbdir,'_'.join([pcode,'standard','tables.db']))
        obj = ProductInfo(prodcode=pcode,prodname=p,stddbdir=stddbdir,divdir=divdir)
        # 提取股票持仓数据
        positions = obj.get_holding_stk(date=today)
        # 提取期货持仓数据
        if base_parser.options('blog'):
            posifut = obj.get_holding_fut(date=today)
            positions = positions.append(posifut,ignore_index=True)
        # 输出持仓单到本地
        outfile = os.path.join(newfiletoday,''.join([p,'_',todaystr,'.csv']))
        positions.to_csv(outfile,header = True,index=False)
        print('updating %s successfully' %p)
        # 结果推送
        targetdir = os.path.join(r'\\JASONCHEN-PC\Positions','Positions'+todaystr)
        if not os.path.exists(targetdir):
            os.system('mkdir %s' %targetdir)
        os.system ("copy %s %s" % (outfile, os.path.join(targetdir,p+'_'+todaystr+'.csv')))  # 推送
        print()

