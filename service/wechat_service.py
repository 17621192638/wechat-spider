# -*- coding: utf-8 -*-
'''
Created on 2017-09-22 15:55
---------
@summary:
---------
@author: Boris
'''
import sys
sys.path.append('..')

import collections

from utils.log import log
import utils.tools as tools
from db.oracledb import OracleDB
from db.elastic_search import ES
from base.wechat_sogou import WechatSogou
from base.wechat_public_platform import WechatPublicPlatform
from base import constance
from db.redisdb import RedisDB
import random

SIZE = 100
TIME_INTERVAL = 24 * 60 * 60

CHECK_NEW_ARTICLE = False #int(tools.get_conf_value('config.conf', 'spider', 'only_today_msg'))  # 有新发布的文章才爬取

class WechatService():
    _db = OracleDB()
    _es = ES()
    _redisdb = RedisDB()
    _wechat_sogou = WechatSogou()
    _wechat_public_platform = WechatPublicPlatform()

    _todo_accounts = collections.deque()
    _rownum = 1

    _is_done = False # 做完一轮
    _is_all_done = False # 所有账号当日发布的消息均已爬取

    # wechat_sogou 最后没被封的时间
    _wechat_sogou_enable = True
    _wechat_sogou_last_unenable_time = tools.get_current_timestamp()

    # wechat_public_platform 最后没被封的时间
    _wechat_public_platform_enable = True
    _wechat_public_platform_last_unenable_time = tools.get_current_timestamp()

    def __init__(self):
        pass

    def __load_todo_account(self):
        accounts = WechatService._redisdb.sget('wechat:account', count = 1)

        for account in accounts:
            account = eval(account)
            WechatService._todo_accounts.append(account)

    def is_have_new_article(self, account_id, account_name, __biz):
        '''
        @summary: 检查是否有新发布的文章
        ---------
        @param account_id:
        @param __biz:
        ---------
        @result:
        '''

        result = ''
        if WechatService._wechat_sogou_enable: # 搜狗微信可用
            result = WechatService._wechat_sogou.is_have_new_article(account_id = account_id, account = account_name)
            if result == constance.UPDATE:
                # 有新发布的文章 抓取
                pass

            elif result == constance.NOT_UPDATE:
                # 无新发布的文章 pass
                pass

            elif result == constance.ERROR:
                pass

            elif result == constance.VERIFICATION_CODE:
                # 被封了 请求失败 记录下失败时间
                WechatService._wechat_sogou_enable = False
                WechatService._wechat_sogou_last_unenable_time = tools.get_current_timestamp()

        # 搜狗微信停用时间超过24小时了 可重新尝试
        elif tools.get_current_timestamp() - WechatService._wechat_sogou_last_unenable_time > TIME_INTERVAL: # 搜狗微信不可用 但是已经间歇一天 还可以一试
            result = WechatService._wechat_sogou.is_have_new_article(account_id = account_id, account = account_name)
            if result == constance.UPDATE:
                # 搜狗微信可用
                WechatService._wechat_sogou_enable = True

            elif result == constance.NOT_UPDATE:
                pass

            elif result == constance.ERROR:
                pass

            elif result == constance.VERIFICATION_CODE:
                pass

            # 更新下可用时间
            WechatService._wechat_sogou_last_unenable_time = tools.get_current_timestamp()

        # 如果搜狗微信不可用 则使用微信公众平台检查是否有新发布的文章
        if not result or result == constance.VERIFICATION_CODE:
            if WechatService._wechat_public_platform_enable: # 微信公众平台可用
                result = WechatService._wechat_public_platform.is_have_new_article(__biz)
                if result == constance.UPDATE:
                    # 有新发布的文章 抓取
                    pass

                elif result == constance.NOT_UPDATE:
                    # 无新发布的文章 pass
                    pass

                elif result == constance.ERROR:
                    # 被封了 请求失败 记录下失败时间
                    WechatService._wechat_public_platform_enable = False
                    WechatService._wechat_public_platform_last_unenable_time = tools.get_current_timestamp()

            elif tools.get_current_timestamp() - WechatService._wechat_public_platform_last_unenable_time > TIME_INTERVAL: # 搜狗微信不可用 但是已经间歇一天 还可以一试
                result = WechatService._wechat_public_platform.is_have_new_article(__biz)
                if result == constance.UPDATE:
                    # 有新发布的文章 抓取
                    WechatService._wechat_public_platform_enable = True

                elif result == constance.NOT_UPDATE:
                    # 无新发布的文章 pass
                    pass

                elif result == constance.ERROR:
                    # 被封了 请求失败 记录下失败时间
                    pass

                # 更新下可用时间
                WechatService._wechat_public_platform_last_unenable_time = tools.get_current_timestamp()

        return result

    def get_next_account(self):
        '''
        @summary:
        ---------
        ---------
        @result: 返回biz, 是否已做完一圈 (biz, True)
        '''

        if not WechatService._todo_accounts:
            self.__load_todo_account()

        if not WechatService._todo_accounts:
            return None

        oralce_id, account_id, account_name, last_article_release_time, biz =  WechatService._todo_accounts.popleft()
        next_account_id = account_id
        next_account_biz = biz
        next_account_name = account_name

        next_account = next_account_id, next_account_biz

        sql = "update TAB_IOPM_SITE t set t.spider_status=602 where t.biz = '%s'"%(next_account_biz)
        WechatService._db.update(sql)

        return next_account

    def update_account_article_num(self, __biz):
        # 查询es 统计数量
        # 今日
        body = {
            "size":0,
            "query":{
                "filtered":{
                    "filter":{
                        "range":{
                            "record_time":{
                                "gte":tools.get_current_date('%Y-%m-%d') + ' 00:00:00',
                                "lte":tools.get_current_date('%Y-%m-%d') + ' 23:59:59'
                            }
                        }
                    },
                    "query":{
                        'match':{
                            "__biz" : __biz
                        }
                    }
                }
            }
        }
        result = WechatService._es.search('wechat_article', body)
        today_msg = result.get('hits', {}).get('total', 0)

        # 历史总信息量
        body = {
            "size":0,
            "query":{
                "filtered":{
                    "query":{
                        'match':{
                            "__biz" : __biz
                        }
                    }
                }
            }
        }
        result = WechatService._es.search('wechat_article', body)
        total_msg = result.get('hits', {}).get('total', 0)

        if total_msg:
            sql = "update TAB_IOPM_SITE t set t.today_msg = %d, t.total_msg = %d, t.spider_status=603 where t.biz = '%s'"%(today_msg, total_msg, __biz)
        else:
            sql = "update TAB_IOPM_SITE t set t.today_msg = %d, t.spider_status=603 where t.biz = '%s'"%(today_msg, __biz)
        print(sql)
        WechatService._db.update(sql)

    def is_exist(self, table, data_id):
        if WechatService._es.get(table, data_id = data_id, doc_type = table):
            return True
        else:
            return False

    def add_article_info(self, article_info):
        '''
        @summary:
        ---------
        @param article_info:
        ---------
        @result:
        '''


        log.debug('''
            -----文章信息-----
            标题     %s
            发布时间 %s
            作者     %s
            公众号   %s
            url      %s
            '''%(article_info['title'], article_info['release_time'], article_info['author'], article_info['account'], article_info['url'])
            )

        WechatService._es.add('wechat_article', article_info, article_info.get('article_id'))

    def add_account_info(self, account_info):
        log.debug('''
            -----公众号信息-----
            %s'''%tools.dumps_json(account_info))

        WechatService._es.add('wechat_account', account_info, account_info.get('__biz'))

if __name__ == '__main__':
    wechat = WechatService()
    wechat.get_next_account()
    pass
