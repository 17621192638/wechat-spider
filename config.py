# -*- coding: utf-8 -*-
'''
Created on 2017-08-02 09:20
---------
@summary: 配置
---------
@author: Boris
'''

# url映射配置


# Turn on/off debugging
DEBUG = False

URLS = (
    '/tip/(.*)', 'action.tip_action.TipAction',
    '/wechat/(.*)', 'action.wechat_action.WechatAction',
    '/(.*)', 'action.help.Help'
)

API_PORT = 6210