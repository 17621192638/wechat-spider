# -*- coding: utf-8 -*-
'''
Created on 2018-05-09 15:13
---------
@summary:
---------
@author: Boris
'''

import sys
sys.path.append('..')
import init

import utils.tools as tools
from utils.log import log
from db.oracledb import OracleDB
from base.wechat_public_platform import WechatPublicPlatform
from base.wechat_sogou import WechatSogou

if __name__ == '__main__':
    db = OracleDB()
    # wechat_public_platform =  WechatPublicPlatform()
    wechat_sogou = WechatSogou()
    # 取微信号
    # sql = 'select t.name, t.keyword2 from TAB_IOPM_CLUES t where t.zero_id = 7 and t.first_id = 137 and t.second_id = 183'
    # accounts = db.find(sql)
    accounts = ['骨朵网络影视']
    for account in accounts:
        account_id = ''
        account_name = account
        biz = wechat_sogou.get_biz(account_id = account_id, account = account_name)
        if biz:
            sql = "insert into TAB_IOPM_SITE t (t.id, t.name, t.position, t.classify, t.mointor_status, t.biz, t.priority) values (seq_iopm_site.nextval, '{name}', 1, 2, 701, '{biz}', 1)".format(name = account_name, biz = biz)
            print(sql)
            db.add(sql)
        tools.delay_time(10)
        # break
