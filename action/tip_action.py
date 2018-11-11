# -*- coding: utf-8 -*-
'''
Created on 2017-08-02 09:24
---------
@summary: 提示
---------
@author: Boris
'''
import web


render = web.template.render('templates')

class TipAction(object):
    """docstring for InterfaceDecument"""

    def wait_tip(self, data):
        tip = '''
            <h3>暂无待抓取url</h3>
            <p>休眠{}</p>
            <p>下次抓取于{}开始</p>
        '''.format(data.sleep_time, data.next_start_time)

        return tip

    def GET(self, name):
        web.header('Content-Type','text/html;charset=UTF-8')
        data = web.input()

        tip = '未知错误'
        if name == 'wait':
            tip = self.wait_tip(data)

        return render.spider_tip(tip)