# See the LICENSE file for license rights and limitations.

# CREATED BY RHYANKWON 20220408

import pdb
import sys
sys.path.append('C:\python_8_me\Lib\site-packages')

import pymysql

def create(name1, name2):

    conn = pymysql.connect(host='localhost',
                       user='', #Use your own id EX) 'ryan'
                       password='', #Use your own password  '1234'
                       charset='') #Use your own data type EX) 'int'

    name = name1+name2

    with conn:

        with conn.cursor() as cur:
            try:
                cur.execute('DROP DATABASE {0}'.format(name))
            except:
                pass
            cur.execute('CREATE DATABASE {0}'.format(name))
            conn.commit()
