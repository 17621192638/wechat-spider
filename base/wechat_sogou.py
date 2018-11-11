# -*- coding: utf-8 -*-
'''
Created on 2017-11-28 18:34
---------
@summary: 基于搜狗微信取_biz及检查今日是否发文
---------
@author: Boris
'''


import sys
sys.path.append('../')

import init
import utils.tools as tools
from utils.log import log
from base import ip_proxies
from base import constance
from base.sogou_cookies_manager import SogouCookiesManager

class WechatSogou():
    def __init__(self):
        self._sogou_cookies_manager = SogouCookiesManager()

    def __get_account_blocks(self, account_id = '', account = ''):
        keyword = account_id or account # 账号id优先

        log.debug('search keywords ' + keyword)

        cookie = self._sogou_cookies_manager.get_cookie()

        headers = {
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Cookie":cookie[1] if cookie else "ABTEST=5|1518054397|v1; SNUID=EAEB52552E2B4B87BB3903692F2AC2DE; IPLOC=CN1100; SUID=C5C47C7B6E2F940A000000005A7BABFD; JSESSIONID=aaa2WHQuoILPuc70EEQfw; SUID=C5C47C7B2313940A000000005A7BABFE; SUV=00BC2C447B7CC4C55A7BABFE845F5410",
            "Host": "weixin.sogou.com"
        }

        proxies = ip_proxies.get_proxies()
        headers["User-Agent"] = ip_proxies.get_user_agent()

        url = 'http://weixin.sogou.com/weixin?type=1&s_from=input&query=%s&ie=utf8&_sug_=n&_sug_type_='%(keyword)
        html, request = tools.get_html_by_requests(url, headers = headers)#, proxies = proxies)

        # 公众号信息块
        regex = '<!-- a -->(.*?)<!-- z -->'
        account_blocks = tools.get_info(html, regex)

        regex = '<input type=text name="c" value="" placeholder="(.*?)" id="seccodeInput">'
        check_info = tools.get_info(html, regex, fetch_one = True)
        if check_info:
            log.debug('''取公众号列表 : %s
                         url : %s
                      '''%(check_info, url)
                      )

            self._sogou_cookies_manager.set_cookie_un_available(cookie)
            self._sogou_cookies_manager.monitor_cookies()

            # return constance.VERIFICATION_CODE
        else:
            self._sogou_cookies_manager.set_cookie_available(cookie)

        for account_block in account_blocks:
            regex = '<a.*?account_name.*?>(.*?)</a>'
            account = tools.get_info(account_block, regex, fetch_one = True)
            account = tools.del_html_tag(account)

            regex = '<label name="em_weixinhao">(.*?)</label>'
            account_id = tools.get_info(account_block, regex, fetch_one = True)

            if account.lower() == keyword.lower() or account_id.lower() == keyword.lower():
                return account_block
        else:
            return ''


    def get_biz(self, account_id = '', account = ''):
        '''
        @summary: 获取公众号的__biz参数
        ---------
        @param account_id:
        @param account:
        ---------
        @result:
        '''
        account_block = self.__get_account_blocks(account_id, account)
        if account_block == constance.VERIFICATION_CODE:
            return constance.VERIFICATION_CODE

        keyword = account_id or account

        regex = '<a.*?account_name.*?>(.*?)</a>'
        account = tools.get_info(account_block, regex, fetch_one = True)
        account = tools.del_html_tag(account)

        regex = '<label name="em_weixinhao">(.*?)</label>'
        account_id = tools.get_info(account_block, regex, fetch_one = True)

        regex = '<a.*?account_name.*?href="(.*?)">'
        account_url = tools.get_info(account_block, regex, fetch_one = True)
        account_url = account_url.replace('&amp;',"&")

        # 取biz
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.8",
            "Host": "mp.weixin.qq.com",
            "Connection": "keep-alive",
            "Referer": "http://weixin.sogou.com/weixin?type=1&s_from=input&query=%s&ie=utf8&_sug_=n&_sug_type_="%keyword,
            "Cookie": account_url,
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "max-age=0",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",
            "Upgrade-Insecure-Requests": "1"
        }

        # proxies = ip_proxies.get_proxies()
        # headers["User-Agent"] = ip_proxies.get_user_agent()

        html, request = tools.get_html_by_requests(account_url)#, proxies = proxies)
        regex = '<div class="weui_cells_tips">(.*?)</div>'
        check_info = tools.get_info(html, regex, fetch_one = True)
        if check_info:
            log.debug('''取公众号文章页 : %s
                         url : %s
                      '''%(check_info, account_url)
                      )
            return ''

        regex = 'var biz = "(.*?)"'

        __biz = tools.get_info(html, regex, fetch_one = True)

        log.debug('''
            公众号名称          %s
            公众号账号          %s
            账号url             %s
            __biz               %s
            '''%(account, account_id, account_url, __biz))

        return __biz

    def is_have_new_article(self, account_id = '', account = ''):
        '''
        @summary: 检查公众号今日是否发文
        ---------
        @param account_id:
        @param account:
        ---------
        @result:
        '''

        release_time = self.get_article_release_time(account_id, account)

        if release_time:
            log.debug("最近发文时间 %s"%release_time)

            if release_time >= tools.get_current_date('%Y-%m-%d'):
                return constance.UPDATE
            else:
                return constance.NOT_UPDATE

        else:
            return constance.ERROR

    def get_article_release_time(self, account_id = '', account = ''):
        account_block = self.__get_account_blocks(account_id, account)
        if account_block == constance.VERIFICATION_CODE:
            return constance.VERIFICATION_CODE

        regex = "timeConvert\('(\d*?)'\)"
        release_time = tools.get_info(account_block, regex, fetch_one = True)

        if release_time:
            release_time = int(release_time)
            release_time = tools.timestamp_to_date(release_time)

        return release_time




if __name__ == '__main__':
    wechat_sogo = WechatSogou()
    # __biz = wechat_sogo.get_biz(account_id = '', account = '美游宜宾')
    # print(__biz)
    is_have_new_article = wechat_sogo.is_have_new_article(account_id = '', account = 'python')
    print(is_have_new_article)

