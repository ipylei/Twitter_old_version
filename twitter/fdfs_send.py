# coding=utf-8
import typing

import requests
from fdfs_client.client import *

NET_IP_PORT = '222.82.235.230:5003'


class Sender(object):
    """fdfs资源传输"""

    def __init__(self):
        self.client = Fdfs_client('twitter/fastdfs-client.conf')
        self.headers = {
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'zh-CN,zh;q=0.9'
        }



    def send_file(self, file: typing.Union[str, bytes], file_ext_name=None) -> str:
        """传输文件"""

        if isinstance(file, str):
            file_info = self.client.upload_by_filename(file)  # 通过文件路径传输
        elif isinstance(file, bytes):
            file_info = self.client.upload_by_buffer(file, file_ext_name=file_ext_name)  # 通过二进制流传输
        else:
            # raise TypeError("file type error, send a PathLike or bytes object please.(ps: don't include './')")
            return ''
        file_url = 'http://' + NET_IP_PORT + '/' + file_info['Remote file_id']
        return file_url.replace('\\', '/')

    def send_image(self, img, file_ext_name='jpg') -> str:
        """传输图片"""

        return self.send_file(img, file_ext_name)

    def send_video(self, vdo, file_ext_name='mp4') -> str:
        """传输视频"""

        return self.send_file(vdo, file_ext_name)

    def download_upload_image(self, image_url):
        """下载并上传图片"""
        for i in range(3):
            try:
                image_content = requests.get(image_url, headers=self.headers).content
                return self.send_image(image_content, file_ext_name='jpg')
            except Exception as e:
                print(f'第{i}次下载图片失败，图片url:{image_url}，失败原因:{e}')
        else:
            return

    def download_upload_video(self, video_url):
        """下载并上传视频"""
        video_content = requests.get(video_url, headers=self.headers).content
        return self.send_video(video_content, file_ext_name='mp4')


if __name__ == '__main__':
    fdfs_sender = Sender()
    # 以上传图片为例
    # img_url = 'http://125.64.9.153:4000/group1/M00/60/04/wKgLZV0-tCuAOfaEAAA_NWwDar4171.jpg'
    # content = requests.get(img_url).content

    # 通过字节流上传图片，获取其资源服务器url，可指定后缀，默认为“jpg”
    # res_url = fdfs_sender.send_image(content)
    # print(res_url)  # res_url.split('#')[-1]

    # 视频同理
    # res_url = fdfs_sender.send_video(content, file_ext_name='mp4')

    # 也可通过文件路径上传（不能包含“./”，会解析报错）
    # res_url = fdfs_sender.send_image('test.jpg')

    # 或者调用通用的文件上传方法，通过字节流上传
    # res_url = fdfs_sender.send_file(content, file_ext_name='jpg')
    # url = "https://pbs.twimg.com/card_img/1216803512354508804/aXmDP8Q1?format=png&name=600x314"
    url = "https://pbs.twimg.com/card_img/1217174852668358658/DzwFS4-u?format=jpg&name=600x314"
    res = fdfs_sender.download_upload_image(image_url=url)
    print(res)

    pass
