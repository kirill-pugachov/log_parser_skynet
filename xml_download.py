# -*- coding: utf-8 -*-
"""
Created on Thu May 25 17:25:28 2017

@author: Kirill
"""

import requests
import shutil



file_path = 'C:/bus/'
file_name = 'bus_'
path_to_log = []
RESULT_LIST = []
HTAG_SET = set()
RESULT_DICT = dict()
KEY_DICT_SUC = dict()
KEY_LIST_RES = ['head','user','addr','name','fail']
url = 'http://ticket.bus.com.ua/partner/v2/SellListV2.xml'
url1 = 'http://ticket.bus.com.ua/partner/SellList.xml'
url_list = [url, url1]


def download_file(url):
    local_filename = file_path + ''.join(url.split('/')[-1:])
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        shutil.copyfileobj(r.raw, f)
    return local_filename

for url in url_list:
    path_to_log.append(download_file(url))
print('done')
print(path_to_log)