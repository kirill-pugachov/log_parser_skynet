import json
import requests
import csv

#Path to files
Path_to_file = 'C:/zlog/zlog7/'
File_name = 'zlog7'
File_result = 'Key_adress_id_links.csv'

Result_Dict={}

def get_data(Path_to_file, File_name):
    '''
    Получаем данные из файла лога
    '''
    with open(Path_to_file + File_name, encoding='utf-8') as Log_File:
        Data_Log_List = json.load(Log_File)
    return Data_Log_List

def get_id_addr(Log):
    '''
    Получаем id_addr из файла лога
    '''
    try:
        id_addr = Log['link']['id_addr']
    except KeyError:
        id_addr = 'no_addr'
    return id_addr

def get_id_key(Log):
    '''
    Получаем key из файла лога
    '''
    try:
        id_key = Log['auth']['id']
    except KeyError:
        id_key = 'no_key'
    return id_key


def get_shop(shop_id):
    '''
    Получаем данные по аптеке из АПИ
    '''
    try:   
        shop = requests.get("http://geoapt.morion.ua/get_shop/" + str(shop_id))
        shop.raise_for_status()
    except requests.exceptions.HTTPError:
        shop_result = ['не получены данные по аптеке из АПИ']
    else:
        shop_out = shop.json()
        shop_result = [shop_out['id']]
        shop_result.append(shop_out['id_head'])
        shop_result.append(shop_out['mark'])
        shop_result.append(shop_out['addr_country'])
        shop_result.append(shop_out['addr_area'])
        shop_result.append(shop_out['addr_city'])
        shop_result.append(shop_out['addr_street'])
    return shop_result  
        

Data_Log_List = get_data(Path_to_file, File_name)

for Log in Data_Log_List:    
    if get_id_key(Log) in Result_Dict.keys():
        Result_Dict[get_id_key(Log)].update(set([get_id_addr(Log)]))
    else:
        Result_Dict[get_id_key(Log)] = set([get_id_addr(Log)])
print(len(Result_Dict))

for key in Result_Dict.keys():
    if key != 'sysdba':
        for adress_id in Result_Dict[key]:
            Page_Links = [key]
            Page_Links.extend(get_shop(adress_id))
            with open(Path_to_file + File_result, 'a', newline='') as resultFile:
                wr = csv.writer(resultFile, dialect='excel', quoting=csv.QUOTE_ALL)
                wr.writerow(Page_Links)
            
##    print(get_id_addr(Log), get_id_key(Log))    
##f = open('C:/Python/Service/Key_adress_id_links.csv', 'w')
##for key, value in sorted(result_dict.items()):
##    f.write(str(key) + '\t' + str(value) + '\n' )
##f.close()                
##            print(key, get_shop(adress_id))    
##with open(Path_to_file + File_name, encoding='utf-8') as Log_File:
##    Data_Log_File = json.load(Log_File)
##    print(len(Data_Log_File))
##    print(Data_Log_List[3]['auth']['id'])
##    print(Data_Log_List[3]['link']['id_addr'])
