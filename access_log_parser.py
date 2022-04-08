# See the LICENSE file for license rights and limitations.

# CREATED BY RHYANKWON 20220408


import json
import pdb
import re
import time
from datetime import datetime
import pandas as pd
import numpy as np
import os
import pickle
import create_mysql
# import create_mysql
import sys
sys.path.append('C:\python_8_me\Lib\site-packages')
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb

# access log format example 2.2.2.4 - ryankwon@gitthub.co.kr [09/Jan/2026:00:20:01 +0900] GET /api/info HTTP/1.0 200 671 "https://portal.hanyang.co.kr" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/7.36 (KHTML, like Gecko) Chrome/6.0.4.10 Safari/7.36" "-" 0.1

result = {}
parts = ('(?P<host>[(\d\.\-)]+) - (?P<user>.*?) \[(?P<time>.*?)\]'
        ' (?P<request>\S+) (?P<path>[(\S\/)]*) (?P<protocol>[(\w\.\/)]+)'
        ' (?P<status>\S+) (?P<bytes>\S+) \"(?P<referer>.*?)\" \"(?P<user_agent>.*?)\"'
        ' (?P<duration_time>[(\d\.\-)]+)')
pattern = re.compile(''.join(parts))
result = []
# start = time.time()


class parsing():
    def __init__(self, path, sql_info, hash_name_dict, hash_web_dict, is_original = False, f_name = ''):
        self.path = path
        self.is_original = is_original
        self.f_name = f_name
        self.sql_name = sql_info[0]
        self.sql_conn = sql_info[1]
        self.sql_exists = sql_info[2]
        self.sql_index = sql_info[3]
        self.hash_name = hash_name_dict
        self.hash_web = hash_web_dict
    
    def _parsing(self):

        with open(self.path) as f:
            lines = f.readlines()
            result = []
            for i, line in enumerate(lines):
                try :
                    ab = re.match(parts, line).groupdict()
                except:
                    try :
                        line = line[line.index(' ')+1:]
                        ab = re.match(parts, line).groupdict()
                    except:
                        print(line)
                        continue
                
                ab['time'] = (datetime.strptime(ab['time'].split()[0], '%d/%b/%Y:%H:%M:%S')).strftime("%Y-%m-%d %H:%M:%S")

                if not self.is_original:
                    if ab['user'][0] == '-': #user 특이한 경우 제외
                        ab['user'] = ab['user'].split()[-1]
                    
                    if ab['user'] == '-':
                        ab['id'], ab['web'] = 0, 0
                    else:
                        id, web = re.split('@', ab['user']) 
                        try :
                            ab['id'] = self.hash_name[id]
                        except :
                            ab['id'] = self.hash_name['start_point']
                            self.hash_name['start_point'] += 1
                            self.hash_name[id] = ab['id']
                        try :
                            ab['web'] = self.hash_web[web]
                        except :
                    
                            ab['web'] = self.hash_web['start_point']
                            self.hash_web['start_point'] += 1
                            self.hash_web[web] = ab['web']
                    
                    try:  #duration시간 float 변환
                        ab['duration_time'] = float(ab['duration_time'])
                    except:
                        if ab['duration_time'] == '-':
                            ab['duration_time'] = np.nan
                        else:
                            ab['duration_time'] = float(re.sub(r'[^0-9, .]', '', ab['duration_time']))
                    try : #bytes int 변환
                        ab['bytes'] = int(ab['bytes'])
                    except:
                        ab['bytes'] = np.nan

                    if ab['user_agent'] == '-':
                        ab['OS'] = '-'
                        ab['is_pc'] = '-'
                        ab['browser'] = '-'
                    else:
                        if re.search('Windows', ab['user_agent']):
                            ab['is_pc'] = 'PC'
                            ab['OS'] = 'Windows'
                            if re.search('Trident', ab['user_agent']):
                                ab['browser'] = 'IE'
                            elif re.search('Fire', ab['user_agent']):
                                ab['browser'] = 'Firefox'
                            elif re.search('Edg', ab['user_agent']):
                                ab['browser'] = 'MS_edge'
                            elif re.search('Chrome', ab['user_agent']):
                                ab['browser'] = 'Chrome'
                            elif re.search('SamsungBrowser', ab['user_agent']):
                                ab['browser'] = 'SamsungBrowser'
                            else :
                                ab['browser'] = '-'
                        elif ab['user_agent'][13:15] == 'iP':
                            ab['is_pc'] = 'Mobile'
                            ab['OS'] = ['Apple_mobile_device']
                            if re.search('CriOS', ab['user_agent']):
                                ab['browser'] = 'Chrome'
                            elif re.search('FxiOS', ab['user_agent']):
                                ab['browser'] = 'Firefox'
                            elif re.search('EdgiOS', ab['user_agent']):
                                ab['browser'] = 'MS_edge'
                            elif re.search('Safari', ab['user_agent']):
                                ab['browser'] = 'Safari'
                            else :
                                ab['browser'] = '-'
                        elif re.search('Mac', ab['user_agent']):
                            ab['is_pc'] = 'PC'
                            ab['OS'] = ['Mac']
                            if re.search('Edg', ab['user_agent']):
                                ab['browser'] = 'MS_edge'
                            elif re.search('Chrome', ab['user_agent']):
                                ab['browser'] = 'Chrome'
                            elif re.search('Safari', ab['user_agent']):
                                ab['browser'] = 'Safari'
                            elif re.search('Fire', ab['user_agent']):
                                ab['browser'] = 'Firefox'
                            elif re.search('Trident', ab['user_agent']):
                                ab['browser'] = 'IE'
                            elif re.search('SamsungBrowser', ab['user_agent']):
                                ab['browser'] = 'SamsungBrowser'
                            else :
                                ab['browser'] = '-'
                        elif re.search('Android', ab['user_agent']):
                            ab['is_pc'] = 'Mobile'
                            ab['OS'] = ['Android']
                            if re.search('Edg', ab['user_agent']):
                                ab['browser'] = 'MS_edge'
                            elif re.search('Chrome', ab['user_agent']):
                                ab['browser'] = 'Chrome'
                            elif re.search('SamsungBrowser', ab['user_agent']):
                                ab['browser'] = 'SamsungBrowser'
                            elif re.search('Fire', ab['user_agent']):
                                ab['browser'] = 'Firefox'
                            else :
                                ab['browser'] = '-'
                        else :
                            ab['OS'] = '-'
                            ab['is_pc'] = '-'
                            ab['browser'] = '-'

                del ab['go_agent']
                del ab['user_agent']
                del ab['protocol']
                del ab['user']

                result.append(ab)
                if i%1000 == 0:
                    try: #판다스 데이터 프레임으로 변환 후 Mysql에 저장
                        (pd.DataFrame.from_dict(result)).to_sql(name=self.sql_name, con=self.sql_conn, if_exists=self.sql_exists, index=self.sql_index)
                    except:
                        pdb.set_trace()
                    result = []
            if result:
                (pd.DataFrame.from_dict(result)).to_sql(name=self.sql_name, con=self.sql_conn, if_exists=self.sql_exists, index=self.sql_index)
        return self.hash_name, self.hash_web
    # def save_dataframe_pickle(self, f_name): #날짜별 판다스 dataframe 저장(pickle 형식)
    #     with open(f_name2+'.pkl', 'ab+') as fr:
    #         pickle.dump(self.txt_to_pd_dataframe(), fr)
    #     fr.close()


def take_hash(): #해시맵 가져오기

    try:
        with open("hash_name.pickle", "rb") as fr1:
            hash_name_list = pickle.load(fr1)
    except:
        # No privious hash_list
        hash_name_list = {}
        hash_name_list['start_point'] = 1

    try:
        with open("hash_web.pickle", "rb") as fr2:
            hash_web_list = pickle.load(fr2)
    except:
        # No privious hash_web_list
        hash_web_list = {}
        hash_web_list['start_point'] = 1
    
    return hash_name_list, hash_web_list

def dump_hash(hash_name_list, hash_web_list): #해시맵 저장하기

    with open("hash_name.pickle", "wb") as fw1:
        pickle.dump(hash_name_list, fw1)

    with open("hash_web.pickle", "wb") as fw2:
        pickle.dump(hash_web_list, fw2)


hash_name, hash_web = take_hash()

kinds = ['admin_log', 'web_log']

week = ['', 'aweek_']


for kind in kinds:

    for w in week:

        create_mysql.create(w, kind)

        db_connection_str = 'mysql+pymysql://abcd:1234@localhost/'+w+kind
        db_connection = create_engine(db_connection_str)
        conn = db_connection.connect()

        dir_paths = [r'C:\Users\user\Desktop\과제\2주차 과제\internship']

        if w == 'original_':
            is_original = True

        for dir_path in dir_paths:

            dir_path += '\\'+kind
            print(dir_path)

            check = 0
            file_name_index = dir_path[::-1].index('\\')

            for (root, directories, files) in os.walk(dir_path):
                for file in files:
                    if w[:5] == 'aweek':
                        if int(file[12:14]) == 1 and 16 >= int(file[-6:-4]) >= 10:
                            pass
                        else :
                            continue
                    file_name = dir_path+'_dataframe'+file.split('.')[0]+'_dataframe'
                    open_path = os.path.join(root, file)
                    print(open_path)

                    to_sql_info = [w+kind, db_connection, 'append', False]

                    data = parsing(open_path, to_sql_info, hash_name, hash_web, is_original = False)
                    hash_name, hash_web = data._parsing()


dump_hash(hash_name, hash_web)
