# -*- coding: utf-8 -*-
'''
Created on 2018-11-03 16:35
---------
@summary: 利用搜狗微信，监测新文章发布
601等待 602正在抓取 603抓取完毕
---------
@author: Boris
'''


import sys
sys.path.append('../')

import init
import utils.tools as tools
from utils.log import log
from base.wechat_sogou import WechatSogou
import datetime
from db.oracledb import OracleDB
from db.redisdb import RedisDB
import threading

MAX_THREAD_COUNT = 100

class CheckNewArticle():
    def __init__(self):
        self._oracledb = OracleDB()
        self._redisdb = RedisDB()
        self._wechat_sogo = WechatSogou()

    def get_wait_check_account(self):
        '''
        @summary:
        ---------
        @param :
        ---------
        @result:
        '''
        # 取抓取完的公众号，且最近发布时间已过去两小时，则再次监测是否又发布新文章
        before_tow_hours = tools.timestamp_to_date(tools.get_current_timestamp() - 60 * 60 * 2)
        sql = '''
            select t.id,
                   t.domain,
                   t.name,
                   to_char(t.last_article_release_time, 'yyyy-mm-dd hh24:mi:ss'),
                   t.biz
              from TAB_IOPM_SITE t
             where t.biz is not null
               and mointor_status = 701
               and t.spider_status = 603
               and (t.last_article_release_time is null or
                   t.last_article_release_time <=
                   to_date('{}', 'yyyy-mm-dd hh24:mi:ss'))
        '''.format(before_tow_hours)

        accounts = self._oracledb.find(sql)

        # 若无抓取完的公众号，且redis中无抓取任务，则数据库中非603任务可能为丢失任务，需要重新下发
        if not accounts and not self._redisdb.sget_count('wechat:account'):
            sql = '''
                select t.id,
                       t.domain,
                       t.name,
                       to_char(t.last_article_release_time, 'yyyy-mm-dd hh24:mi:ss'),
                       t.biz
                  from TAB_IOPM_SITE t
                 where t.biz is not null
                   and mointor_status = 701
                   and t.spider_status ！= 603
            '''

            accounts = self._oracledb.find(sql)

        return accounts

    def check_new_article(self, account):
        oralce_id, account_id, account_name, last_article_release_time, biz = account

        article_release_time = self._wechat_sogo.get_article_release_time(account_id = account_id, account = account_name)
        print(article_release_time)
        if article_release_time:
            last_article_release_time = last_article_release_time or ''
            if article_release_time >= tools.get_current_date('%Y-%m-%d') and article_release_time > last_article_release_time:
                print('{} 有新文章发布，等待抓取。 发布时间：{}'.format(account_name, article_release_time))

                sql = '''
                    update TAB_IOPM_SITE t set t.spider_status = 601,
                     t.last_article_release_time =
                           to_date('{}', 'yyyy-mm-dd hh24:mi:ss')
                     where id = {}
                '''.format(article_release_time, oralce_id)

                # 多线程， 数据库需每个线程持有一个
                oracledb = OracleDB()
                oracledb.update(sql)
                oracledb.close()

                # 入redis， 作为微信爬虫的任务池
                data = (oralce_id, account_id, account_name, last_article_release_time, biz)
                self._redisdb.sadd('wechat:account', data)


if __name__ == '__main__':
    check_new_article = CheckNewArticle()
    while True:
        accounts = check_new_article.get_wait_check_account()

        while accounts:
            threads = []
            for i in range(MAX_THREAD_COUNT):
                if accounts:
                    thread = threading.Thread(target=check_new_article.check_new_article, args=(accounts.pop(0),))
                    threads.append(thread)
                    thread.start()
                else:
                    break

            for thread in threads:
                thread.join()

        print('休眠10分钟之后检查下一轮')
        tools.delay_time(600)