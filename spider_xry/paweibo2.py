import pandas as pd
import requests
from bs4 import BeautifulSoup as BS
import re
import time
import datetime
import warnings
import os

# 基本设置
warnings.filterwarnings("ignore")
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 100)
pd.set_option('display.max_colwidth', 100)

'''
todo:
5.技术难点、要点，关键环节的截图以及第三方复现的流程
6.列举绕开微博反爬虫的思路、方案

'''

num_article = 200  # 博文数量
num_comment = 200  # 评论数量

# list_user_id = ['finance']  # 博主id
list_user_id = ['finance','5489115923']  # 博主id
save_path = './data/'  # 数据保存路径
headers = {'Connection': 'close',
           'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Mobile Safari/537.36',
           # 'Cookie': 'e4e076cf6445d9b0207274c64ad4fc67',
           'Cookie': '_T_WM=c4cabb0c2c0cb170c5c25c1e016f006e; SCF=ApHgZS2y3HqEbv2sOCI-YtjQLOmrxwl4AAY6W7kykJ2tL-QzM72gLghBhJwzPuHrpIOFjqq4Xl-NMy6mRhIlTzc.; SUB=_2A25LW8pEDeRhGeBN7lIS8yzOzjWIHXVoGUOMrDV6PUJbktAbLVjSkW1NREgUKy4SipOLtDUMlDm0sKXAyDRRVYyY; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5_2mm95TkcExQgvQ.NMuf45JpX5KMhUgL.Foq0SK50e0zESK.2dJLoIpjLxKnLBo5LBKzLxK-LBo5L12qLxKqL1-zL12et; SSOLoginState=1717549588; ALF=1720141588; MLOGIN=1; M_WEIBOCN_PARAMS=luicode%3D20000174'

           }


class weibo_data_collect:

    def __init__(self, **kwargs):

        self.num_article = kwargs['num_article']
        self.num_comment = kwargs['num_comment']
        self.list_user_id = kwargs['list_user_id']
        self.header = kwargs['header']
        self.save_path = kwargs['save_path']
        if not os.path.exists(self.save_path):
            os.makedirs(self.save_path)

    def id_collect(self):
        '''

        :param 博主id
        :return: 博文网页列表
        '''

        # 所有博文网址
        list_comment_http = []

        for user_id in self.list_user_id:

            # 网址拼接
            urls = ['https://weibo.cn/' + user_id + '?filter=1&page=' + str(i) for i in range(1, 30)]

            list_web = []
            for url in urls:
                time.sleep(2)

                if len(list_web) >= self.num_article:
                    list_comment_http += list_web
                    break

                r = requests.get(url, headers=self.header, verify=False)  # 发送请求

                if '403' in str(r):
                    time_now = datetime.datetime.now()
                    print('403_1', time_now.strftime("%Y-%m-%d %H:%M"))


                soup = BS(r.text, 'html.parser')

                items = soup.find_all('div', {'class': 'c'})[1:-1]  # 解析

                for txti in items:
                    if len(list_web) >= self.num_article:
                        break

                    # 获取博文评论网址
                    list_web.append(txti.find('a', {'class': 'cc'}).get('href'))

        self.list_comment_http = list_comment_http

    def data_collect(self):
        '''

        :param : 博文id列表
        :return: 博文内容保存
        '''
        if os.path.exists(self.save_path + '文件1.csv'):
            return

        list_content = []
        list_hot_comment_url = []

        count_ = 0

        for webi in self.list_comment_http:

            if count_ % 10 == 0:
                print(count_)
            count_ += 1
            zan = 0
            zhuan = 0
            ping = 0

            time.sleep(2)

            r = requests.get(webi, headers=self.header, verify=False)  # 发送请求
            if '403' in str(r):
                time_now = datetime.datetime.now()
                print('403_2', time_now.strftime("%Y-%m-%d %H:%M"))
            soup = BS(r.text, 'html.parser')

            # 匹配内容
            content_id = re.search(r'comment[/](.*?)uid', webi).group(1)[:-1]
            user_id = re.search(r'uid=(\d+)', webi).group(1)
            name = soup.find('a', {'href': '/' + user_id}).text
            content = soup.find('span', {'class': 'ctt'}).get_text()[1:]

            time_ = soup.find('span', {'class': 'ct'}).get_text().rstrip()
            time_ = time_transform(time_)  # 时间转换

            if re.search(r'赞[[](.*?)[]]', str(r.text)):
                zan = re.search(r'赞[[](.*?)[]]', str(r.text)).group(1)
            if re.search(r'转发[[](.*?)[]]', str(r.text)):
                zhuan = re.search(r'转发[[](.*?)[]]', str(r.text)).group(1)
            if re.search(r'评论[[](.*?)[]]', str(r.text)):
                ping = re.search(r'评论[[](.*?)[]]', str(r.text)).group(1)
                if int(ping) > 0:
                    list_hot_comment_url.append('https://weibo.cn/comment/' + 'hot/' + content_id)

            list_content.append([name, content_id, content, time_, zhuan, ping, zan])

        df_result = pd.DataFrame(list_content)
        df_result.columns = ['博主', '博文id', '博文内容', '发布时间', '转发数', '评论数', '点赞数']
        print(df_result)
        df_result.to_excel(self.save_path + '文件1.xlsx', index=False)
        print('文件1完成')

        self.list_hot_comment_url = list_hot_comment_url

    def hot_comment_collect(self):
        '''

        :param 热门评论url:
        :return:
        '''

        df_hot_comment = []

        count_ = 0
        for hot_comment_url in self.list_hot_comment_url:
            if count_ % 10 == 0:
                print(count_)
            count_ += 1

            load_all = 0
            url_list = [hot_comment_url + '?page=' + str(i) for i in range(1, 30)]
            hot_comment_list = {}
            article_id = hot_comment_url.split('/')[-1]

            for url in url_list:
                print(url)
                if len(hot_comment_list) >= self.num_comment:
                    df_hot_comment += list(hot_comment_list.values())
                    break
                if load_all == 1:
                    df_hot_comment += list(hot_comment_list.values())
                    break

                time.sleep(2)

                r = requests.get(url, headers=self.header, verify=False)  # 发送请求
                if '403' in str(r):
                    time_now = datetime.datetime.now()
                    print('403_3', time_now.strftime("%Y-%m-%d %H:%M"))
                if '赞' not in r.text:
                    print('None')
                    df_hot_comment += list(hot_comment_list.values())
                    break

                soup = BS(r.text, 'html.parser')
                for datai in soup.find_all('div', {'class': 'c'})[2:]:
                    zan = 0

                    if len(hot_comment_list) >= self.num_comment:
                        break

                    comment_id = re.search(r'id="(.*?)"', str(datai)).group(1)
                    if comment_id in hot_comment_list:
                        load_all = 1
                        break
                    name = datai.get_text().split(':')[0]
                    time_ = ' '.join(datai.find('span', {'class': 'ct'}).get_text().split()[:2])
                    time_ = time_transform(time_)
                    content = datai.find('span', {'class': 'ctt'}).get_text()

                    if re.search(r'赞[[](.*?)[]]', str(datai)):
                        zan = re.search(r'赞[[](.*?)[]]', str(datai)).group(1)

                    hot_comment_list[comment_id] = [article_id, comment_id, name, time_, content, zan]

        df_hot_comment = pd.DataFrame(df_hot_comment)
        df_hot_comment.columns = ['博文id', '评论id', '评论用户名', '评论时间', '评论内容', '点赞数']
        df_hot_comment.to_excel(self.save_path + '文件2.xlsx', index=False)
        print(df_hot_comment)
        print('文件2完成')

    def main(self):
        # for user_id in self.list_user_id:
        self.id_collect()
        self.data_collect()
        self.hot_comment_collect()


def time_transform(time_str):
    time_now = datetime.datetime.now()
    if '今天' in time_str:
        return time_now.strftime("%Y-%m-%d") + ' ' + time_str[3:]
    elif '分钟前' in time_str:
        return (time_now - datetime.timedelta(minutes=int(time_str.split('分钟前')[0]))).strftime("%Y-%m-%d %H:%M")
    if '月' in time_str:
        return time_now.strftime("%Y") + '-' + time_str.split('月')[0] + '-' + \
            time_str.split('月')[-1].split('日')[0] + ' ' + time_str.split()[-1]
    else:
        return time_str


if __name__ == '__main__':
    wb_data_c = weibo_data_collect(header=headers
                                   , list_user_id=list_user_id
                                   , save_path=save_path
                                   , num_comment=num_comment
                                   , num_article=num_article
                                   )
    wb_data_c.main()

