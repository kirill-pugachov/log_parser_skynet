# -*- coding: utf-8 -*-
"""
Created on Fri Apr  7 17:25:02 2017

@author: Kirill
"""

import json
import csv

#
FILE_PATH = 'C:/zlog/zlog7/'
FILE_NAME = 'zlog7'
KEY_TO_FIND = '80abed878209882726b191eaf4bff74a10d741a4'
PATH_TO_LOG = (FILE_PATH + FILE_NAME)
RESULT_LIST = []
HTAG_SET = set()

def read_log_file(PATH_TO_LOG):
    with open(PATH_TO_LOG, encoding='utf-8') as log_data:
        log_json = json.load(log_data)
        print(len(log_json))
        return log_json

def data_income(log):
    data_row = log['time'] + ' ' + log['name'] + ' ' + log['user'] + ' ' + log['head'] + ' ' + log['addr']
    return data_row


def htag_list_generator(LOG, HTAG_SET):
    if LOG['htag'] not in HTAG_SET:
        HTAG_SET.add(LOG['htag'])      
    return HTAG_SET
        

full_log = read_log_file(PATH_TO_LOG)
   
for log in full_log:
    htag_list_generator(log, HTAG_SET)
    if log['auth']['id'] == KEY_TO_FIND and log['htag'] == 'geoapt.ua':
        RESULT_LIST.append(data_income(log))
print(len(RESULT_LIST))
print(RESULT_LIST)
print(HTAG_SET)

with open('C:/Python/Service/Drug_shop_Farmpostach.csv', 'w', newline='') as csv_file:
    csv_writer = csv.writer(csv_file)
    for Item in RESULT_LIST:
        csv_writer.writerow([Item])

