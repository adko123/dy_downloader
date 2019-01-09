# -*- coding:utf-8 -*-
import requests
import time
import datetime
import uuid
import pymongo
import os
import yaml
import logging
import urllib

logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)


class DouYin(object):
    client = pymongo.MongoClient('mongodb://localhost:27017/', connect=False)
    db = client['douyin']
    conf_path = os.path.abspath(os.path.join(os.path.dirname(__file__))) + '/conf.yml'

    def __init__(self):
        self.video_amount = 0
        self.headers = {
            'Cache-Control': 'max-stale=0',
            'Host': 'aweme.snssdk.com',
            'sdk-version': '1',
            'User-Agent': 'okhttp/3.10.0.1',
            'Connection': 'Keep-Alive',
            'Accept-Encoding': 'gzip',
        }
        self.col_videos = self.db['videos']  # 视频信息表

        f = open(self.conf_path)
        cfg = f.read()
        yam_data = yaml.load(cfg)
        self.download_video_keywords = yam_data.get('download_video_keywords')
        self.challenge_keyword = yam_data.get('challenge_keyword')
        self.video_path = yam_data.get('video_path')
        self.video_max_count = yam_data.get('video_max_count')

    def run(self):

        logger.info(u'==========获取关键字为"%s"的挑战列表==========' % self.challenge_keyword)
        host = 'aweme.snssdk.com'
        url = 'https://aweme.snssdk.com/aweme/v1/challenge/search/?cursor=0&keyword=%s&count=20&' \
              'hot_search=0&is_pull_refresh=0&search_source=challenge&ts=1546930078&device_type=SM-G9200&' \
              'device_platform=android&iid=56238234934&app_name=aweme' % urllib.quote(
            self.challenge_keyword.encode("utf-8"))
        self.headers['Host'] = host

        r = requests.get(url, headers=self.headers)

        # 挑战列表
        challenge_list = r.json()['challenge_list']
        # 第0个挑战完全匹配
        ch_id = challenge_list[0]['challenge_info']['cid']

        for cursor in range(0, 10000, 20):

            logger.info(u'-----获取第%s-%s条视频-----' % (str(cursor), str(cursor + 20),))
            # 进入挑战主页 下载视频
            url = 'https://aweme.snssdk.com/aweme/v1/challenge/aweme/?ch_id=%s&query_type=0&cursor=%s&count=20&' \
                  'type=5&device_id=61363007759&app_name=aweme&version_code=390&version_name=3.9.0' % (ch_id, cursor)

            host = 'aweme.snssdk.com'
            self.headers['Host'] = host
            try:
                r = requests.get(url, headers=self.headers, timeout=10)

            except requests.exceptions.ConnectTimeout:
                logger.info(u'==========视频请求超时==========')
                continue

            aweme_list = r.json()['aweme_list']

            for aweme in aweme_list:
                aweme_id = aweme['aweme_id']
                nickname = aweme['author']['nickname']
                dy_id = aweme['author']['short_id']
                user_id = aweme['author']['uid']
                video_urls = aweme['video']['play_addr_lowbr']['url_list']
                video_url = video_urls[0]
                self.save_video(user_id, video_url, nickname, dy_id, aweme_id)

    def save_video(self, user_id, video_url, user_name, douyin_id, aweme_id):
        """
        保存视频
        :param user_id:
        :param video_url:
        :param user_name:
        :param douyin_id:
        :param aweme_id:
        :return:
        """

        if self.col_videos.find_one({'aweme_id': aweme_id}):
            logger.info(u'---视频"%s"已存在---' % (aweme_id,))
            return

        comment_dict = self.check_download_video(aweme_id)
        if not comment_dict:
            logger.info(u'---视频"%s"评论关键字次数不达标,%s---' % (aweme_id, video_url,))

            doc = {
                'user_id': user_id,  # 用户ID
                'user_name': user_name,  # 用户姓名
                'douyin_id': douyin_id,  # 抖音账号
                'created_at': datetime.datetime.utcnow(),  # 创建时间
                'aweme_id': aweme_id,  # 视频唯一标识
                'video_url': video_url,  # 视频地址
                'filename': None,  # 文件名称
                'comment_dict': comment_dict,
            }
            self.col_videos.insert(doc)
            return

        filename = '%s.mp4' % (user_id + '-' + str(uuid.uuid1()))
        try:
            res = requests.get(video_url, stream=True, timeout=50)
        except requests.exceptions.ConnectTimeout:
            logger.info(u'==========视频下载请求超时==========')
            return
        os.chdir(self.video_path)
        # 将视频写入文件夹
        with open(filename, 'ab') as f:
            f.write(res.content)
            f.flush()

        doc = {
            'user_id': user_id,  # 用户ID
            'user_name': user_name,  # 用户姓名
            'douyin_id': douyin_id,  # 抖音账号
            'created_at': datetime.datetime.utcnow(),  # 创建时间
            'aweme_id': aweme_id,  # 视频唯一标识
            'video_url': video_url,  # 视频地址
            'filename': filename,  # 文件名称
            'comment_dict': comment_dict,
        }
        self.col_videos.insert(doc)
        logger.info(u'===视频"%s"(%s)保存完成===' % (aweme_id, video_url,))
        time.sleep(2)

        self.video_amount += 1
        if self.video_amount >= self.video_max_count:
            exit()

    def check_download_video(self, aweme_id):
        """
        检查是否满足下载视频的条件
        :return:
        """
        logger.info(u'-----检查%s是否满足下载视频的条件-----' % aweme_id)

        comment_dict = {i: [] for i in self.download_video_keywords}

        for cursor in range(20, 1000, 20):
            logger.info(u'-----获取第%s-%s条评论-----' % (str(cursor), str(cursor + 20),))
            comments = self.get_comments_by_aweme_id(aweme_id, cursor)

            if not comments and not self.check_download_video_keyword_count(comment_dict):
                logger.info(u'-----评论不满足条件-----')

                return False

            for comment in comments:
                text = comment['text']

                keyword = self.get_has_comment_keyword(text)
                if not keyword:
                    continue
                comment_dict[keyword].append(text)

                if self.check_download_video_keyword_count(comment_dict):
                    return comment_dict

    def check_download_video_keyword_count(self, comment_dict):
        """
        检查关键字出现次数是否满足
        :param comment_dict:
        :return:
        """

        for k, v in comment_dict.items():
            if len(v) >= self.download_video_keywords[k]:
                return True
        return False

    def get_has_comment_keyword(self, text):
        """
        检查评论是否包含关键字,有则返回关键字
        :param text:
        :return:
        """
        for keyword in self.download_video_keywords:
            if keyword in text:
                return keyword
        return False

    def get_comments_by_aweme_id(self, aweme_id, cursor):
        """
        根据视频ID获取当前视频的评论列表
        :param aweme_id:
        :param cursor:
        :return:
        """
        time.sleep(1)
        url = 'https://aweme.snssdk.com/aweme/v2/comment/list/?aweme_id=%s&cursor=%s&count=20&app_type=normal&' \
              'device_type=SM-G9200&device_platform=android&iid=56238234934&app_name=aweme&version_name=3.9.0&' \
              'device_id=61363007759&os_version=6.0.1' % (aweme_id, cursor)
        host = 'aweme.snssdk.com'
        self.headers['Host'] = host

        try:
            r = requests.get(url, headers=self.headers, timeout=20)
            comments = r.json()['comments']
        except requests.exceptions.ConnectTimeout:
            logger.info(u'==========评论请求超时==========')
            return
        return comments


if __name__ == '__main__':
    dy = DouYin()
    dy.run()
