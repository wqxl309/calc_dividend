
import configparser as cp
import datetime as dt
import os
import sys
import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import pandas as pd

from ProductPositions import ProductPositions


if __name__ == '__main__':
    try:
        today = dt.datetime.today()
        # today = dt.datetime(year=2017,month=10,day=19)
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

        products = ['bq1','bq2','bq3','jq1','hj1','ms1','ms2','ls1']
        nametrans = {'bq1':'BaiQuan1',
                     'bq2':'BaiQuan2',
                     'bq3':'BaiQuan3',
                     'jq1' :'BaiQuanJinQu1',
                     'hj1':'BaiQuanHuiJin1',
                     'ms1':'BaiQuanMS1',
                     'ms2':'BaiQuanMS2',
                     #'gd2':'GuoDaoLiShi2',
                     'ls1' : 'BaiQuanLiShi1'
                     }
        suc = 0
        for p in products:
            print('updating %s ...' %p)
            # 读取产品配置
            pcode = nametrans[p]
            base_parser = cp.ConfigParser()
            base_parser.read(os.path.join(configdir,'.'.join([pcode,'ini'])))
            # 创建对象
            stddbdir = os.path.join(dbdir,'_'.join([pcode,'standard','tables.db']))
            obj = ProductPositions(prodcode=pcode,prodname=p,stddbdir=stddbdir,divdir=divdir)
            # 提取股票持仓数据
            positions = obj.get_holding_stk(date=today)
            # 提取期货持仓数据
            if base_parser.options('blog'):
                posifut = obj.get_holding_fut(date=today)
                positions = positions.append(posifut,ignore_index=True)
            # 输出持仓单到本地
            outfile = os.path.join(newfiletoday,''.join([p,'_',todaystr,'.csv']))
            positions.to_csv(outfile,header = True,index=False)
            print('%s updated  successfully' %p)
            # 结果推送
            pushtotarget = True
            if pushtotarget:
                targetdir = os.path.join(r'\\JASONCHEN-PC\Positions','Positions'+todaystr)
                if not os.path.exists(targetdir):
                    os.system('mkdir %s' %targetdir)
                cpresult = os.system ("copy %s %s" % (outfile, os.path.join(targetdir,p+'_'+todaystr+'.csv')))  # 推送
                assert cpresult==0, '[-] push failed, please check'
                suc += 1

        # 生成邮件
        message = MIMEMultipart()
        message['From'] = Header("百泉投资", 'utf-8')
        #message['To'] =  Header("测试", 'utf-8')
        message['Subject'] = Header('仓位推送情况简报_{0}'.format(todaystr), 'utf-8')
        #添加邮件正文内容
        message.attach(MIMEText('{0} pushed successfully with tot {1} products'.format(suc,len(products)), 'plain', 'utf-8'))
        # 发送邮件
        try:
            sender = 'baiquaninvest@baiquaninvest.com'
            receivers = ['wangjp@baiquaninvest.com']
            smtpobj = smtplib.SMTP()
            smtpobj.connect(host='smtp.qiye.163.com',port=25)
            smtpobj.login(user=sender,password='Baiquan1818')
            smtpobj.sendmail(sender,receivers,message.as_string())
        except BaseException:
            raise Exception('sending emails failed')

    except BaseException as e:
        raise e
        print('ERROR：%s' %e)
        os.system('PAUSE')
