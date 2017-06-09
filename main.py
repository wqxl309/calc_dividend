

import datetime as dt
import os
import sys

import numpy as np
import pandas as pd

from products_info import *
from products_configures import *


if __name__ == '__main__':


    products = ['bq1','bq2','jq1','hj1','gd2','ls1','xy7']

    if not os.path.exists(newfiletoday):
        os.mkdir(newfiletoday)
    print(newfiletoday)

    for p in products:
        rawfile = os.path.join(basepath,raw,p,p+'_'+today+'.csv')
        rawdb = os.path.join(basepath,db,''.join(['calcdiv_',p,'.db']))
        outfile = os.path.join(basepath,newfile,p, p+'_'+today+'.csv')
        if p=='ls1':
            othersource = os.path.join(basepath,'ls1_value_%s.txt' %today )
        else:
            othersource = None

        cw = cwdir.get(p)
        hasfut = False
        if cw:
            cwd = cw
            logd = logdir[p]
            hasfut = True
        else:
            cwd = ''
            logd = ''

        obj = product_info(dbdir=rawdb,divdir=divdir,product=p,title_hold=holdvars[p],title_asset=assetvars[p],textvars=textvars[p],cwdir=cwd,logdir=logd)
        obj.stkhold_to_db(rawfiledir=rawfile,date=today)
        holdings = obj.get_holding_stk(date=today,othersource=othersource)
        if hasfut:
            holdfut = obj.get_holding_fut(date=today)
            holdings = pd.concat([holdings,holdfut],axis=0,ignore_index=True)
        holdings.to_csv(outfile,header = True,index=False)

        os.system ("copy %s %s" % (outfile, os.path.join(newfiletoday,p+'_'+today+'.csv')))
        os.system ("copy %s %s" % (outfile, os.path.join(r'\\JASONCHEN-PC\Positions','Positions'+today,p+'_'+today+'.csv')))  # 推送


