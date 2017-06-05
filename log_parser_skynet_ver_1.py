# -*- coding: utf-8 -*-
"""
Created on Fri Apr  7 17:25:02 2017

@author: Kirill
"""

import requests
import json
from datetime import date
import csv
import shutil
import gzip


#
file_path = 'C:/zlog/zlog7/'
file_name = 'zlog7'
PATH_TO_LOG = (file_path + file_name)
RESULT_LIST = []
HTAG_SET = set()
RESULT_DICT = dict()
KEY_DICT_SUC = dict()
KEY_LIST_RES = ['head','user','addr','name','fail']
url = 'https://skynet.morion.ua/system/get-zlog'


def download_file(url):
    local_filename = file_path + url.split('/')[-1] + '.gz'
    r = requests.post(url, auth=('api', 'key-sysdba'), stream=True)
    with open(local_filename, 'wb') as f:
        shutil.copyfileobj(r.raw, f)
    return local_filename


def unzip_downloaded_file(local_filename):
    with gzip.open(local_filename, 'rb') as f:
        file_content = f.read().decode('utf-8')
    return file_content


def read_log_file(file_content):
    log_json = json.loads(file_content)
    return log_json    


def htag_set_generator(LOG, HTAG_SET):
    if LOG['htag'] not in HTAG_SET:
        HTAG_SET.add(LOG['htag'])
    return HTAG_SET


def dict_list_generator(full_log):
    for log in full_log:
        htag_set_generator(log, HTAG_SET)
        if 'fail' in log.keys():
            try:
                RESULT_DICT[log['htag']][0].append(log)
            except KeyError:
                RESULT_DICT[log['htag']] = [[log],[]]                    
        else:
            try:
                RESULT_DICT[log['htag']][1].append(log)

            except KeyError:
                RESULT_DICT[log['htag']] = [[],[log]]
    return RESULT_DICT                


def write_result_to_file(file_path, tag, result_file_list):
    RESULT_FILE_NAME = tag.replace('.', '_') +'_'+ str(date.today()).replace('-', '_') + '.csv'
    RESULT_file_path = file_path + RESULT_FILE_NAME
    with open(RESULT_file_path, 'w', encoding='utf8', newline='') as csv_file:
        csv_writer = csv.writer(csv_file, dialect='excel', quoting=csv.QUOTE_ALL)
        for item in result_file_list:
            csv_writer.writerow(item)


def result_list_generator(HTAG_SET, dict_list):
    for tag in HTAG_SET:
        result_file_list = []
        for data in dict_list[tag][0]:
            result_line = ''
            for key in data.keys():
                result_line += (key +':'+' '+ str(data[key])) + '|'
            result_file_list.append(((result_line).strip()).split('|'))
        write_result_to_file(file_path, tag, result_file_list)


def cloud_key_extractor(data):
    '''Получаем ключ облачной авторизации'''
    try:
        cloud_key = data['auth']['id']
    except KeyError:
        cloud_key = None
    return cloud_key

    
def head_generator(data):
    try:
        head_name = data['head']
    except KeyError:
        head_name = 'no head name'
#    print(head_name)
    return head_name
        

def head_substitute(data, cloud_key):
    try:
        real_head = data['head']
    except KeyError:
        if data['auth']['id'] in cloud_key.keys():
            real_head = '-'.join(cloud_key[data['auth']['id']])
        else:
            real_head = 'no head in log'
    return real_head


def user_substitute(data):
    try:
        real_user = data['user']
    except KeyError:
        real_user = 'no user in log'
    return real_user
    

def addr_substitute(data):
    try:
        real_addr = data['addr']
    except KeyError:
        real_addr = 'no addr in log'
    return real_addr
    

def name_substitute(data):
    try:
        real_name = data['name']
    except KeyError:
        real_name = 'no name in log'
    return real_name
    

def fail_substitute(data):
    try:
        real_fail = data['fail']
    except KeyError:
        real_fail = 'no fail in log'
    return real_fail    

    
def cloud_key_dict_generator(dict_list):
    for data in dict_list['geoapt.ua'][1]:
        if cloud_key_extractor(data):
            if cloud_key_extractor(data) in KEY_DICT_SUC.keys():
                KEY_DICT_SUC[cloud_key_extractor(data)].add(head_generator(data))
            else:
                KEY_DICT_SUC[cloud_key_extractor(data)] = set([head_generator(data)])
    return KEY_DICT_SUC
                

def result_list_generator_geoapt(HTAG_SET, dict_list):
    tag = 'geoapt.ua'
    result_file_list = []
    for data in dict_list[tag][0]:
        result_line = data['time'] + '|' + head_substitute(data, cloud_key) + '|' + user_substitute(data) + '|' + addr_substitute(data) + '|' + name_substitute(data) + '|' + fail_substitute(data)
        result_file_list.append(((result_line).strip()).split('|'))
    write_result_to_file(file_path, tag, result_file_list)            

                    
downloaded_file = download_file(url)
full_log = read_log_file(unzip_downloaded_file(downloaded_file))
dict_list = dict_list_generator(full_log)
cloud_key = cloud_key_dict_generator(dict_list)
result_list_generator_geoapt(HTAG_SET, dict_list)