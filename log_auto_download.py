# -*- coding: utf-8 -*-
"""
Created on Thu Apr 20 16:07:37 2017

@author: Kirill
"""

#автоматическая выгрузка файла лога

import requests
import shutil
import gzip
import json
#import tarfile


url = 'https://skynet.morion.ua/system/get-zlog'
file_path = 'C:/zlog/zlog7/'

def download_file(url):
    local_filename = file_path + url.split('/')[-1] + '.gz'
#    print(local_filename, url.split('/'))
    r = requests.post(url, auth=('api', 'key-sysdba'), stream=True)
    with open(local_filename, 'wb') as f:
        shutil.copyfileobj(r.raw, f)
    return local_filename

def unzip_downloaded_file(local_filename):
    with gzip.open(local_filename, 'rb') as f:
        file_content = f.read().decode('utf-8')
    return file_content
    

#def unzip_downloaded_file_2(local_filename, url):
#    print(local_filename)
#    print(url)
#    tar = tarfile.open(local_filename, 'r:gz')
#    tar.extractall(file_path)
#    
##    with tarfile.open(local_filename, 'r') as tar:
##        tar.extractall(file_path)
#    local_filename_2 = file_path + url.split('/')[-1]
#    print(local_filename_2)
#    return local_filename_2    
    
    
body = (unzip_downloaded_file(download_file(url)))#.decode('utf-8')
log_json = json.loads(body)

    