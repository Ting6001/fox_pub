from sqlalchemy import create_engine
import csv
import psycopg2
import json
import datetime, time
import pandas as pd
import os
import shutil
# from tqdm import tqdm

with open('.\info.txt', 'r', encoding='utf8') as f:
	dic_info = json.load(f)
from_folder = dic_info['ori_json_folder']
to_folder = dic_info['saved_json_folder']
logfolder = dic_info['log_folder']
s_database = dic_info['database']
s_user = dic_info['user']
s_password = dic_info['password']
s_host = dic_info['host']
s_port = dic_info['port']

t_start = time.time()

folder_day = ''
num = 10
count_limit = 11
# filename = 'datas_sample.json'
def get_machine_id(cursor, dic):
	dic = dic['machine_info']

	sql = f"""select machine_id from machine_info where 
	device_no = '{dic['device_no']}' 
	and station_no = '{dic['station_no']}' 
	and robot_no = '{dic['robot_no']}'"""

	cursor.execute(sql)
	res = cursor.fetchone()
	if res == None:
		return res
	else:
		return res[0]

def get_sql(dics, tablename, tp_datas, machine_id=None):
	lst_col, lst_val = [], []
	for key in dics:
		dic = dics[key]
		for k,v in dic.items(): # 先存machine_info、再存machine_para
			if k == 'report_time':
				v = datetime.datetime.strptime(v, "%Y-%m-%d_%H:%M:%S")
			lst_col.append(k)
			lst_val.append(v)

	s_col = ','.join(lst_col)
	s_val = ','.join(['%s'] * (len(lst_col)))
	sql_insert = "INSERT INTO " + tablename + '(' + s_col + ") VALUES (" + s_val + ")"
	tp_datas += (lst_val,)
	return sql_insert, tp_datas

def insert_table(cusor, filename, tablename):
	print('table：', tablename)
	with open(filename, 'r') as f:
		lst_datas = json.load(f)
	tp_datas = ()
	sql = ''
	for i in range(len(lst_datas)): # list裡有多筆不同時間dict
		dic_data = lst_datas[i]
		sql, tp_datas = get_sql(dic_data, tablename, tp_datas)
	if sql != '':
		cusor.executemany(sql, tp_datas)
		print('Finish execution')

try:
	print(os.path.basename(__file__))	
	print(dic_info)
	connection = psycopg2.connect(database=s_database, user=s_user, password=s_password,
		host=s_host, port=s_port)	
	# print('Opened database successfully')
	cursor = connection.cursor()
	count = 0
	ori_date = datetime.datetime.now()
	logname = logfolder + 'json2db_' + datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.log'
	with open(logname, 'w', encoding='utf8') as f:
		f.write('Start')
	while True:
		cur = datetime.datetime.now()
		if count > count_limit:
			break
		if (cur - ori_date).seconds % num == 0:
			ori_date = cur
			count += 1
			print(count, cur)
			time.sleep(1)
	# ============ insert table ============
			lst_finish = []
			set_day = set()
			for f in os.listdir(from_folder):
				set_day.add(f.split('_')[0])
				print('file：', f)
				filename = from_folder + f
				insert_table(cursor, filename, 'machine_para')
				lst_finish.append(f)
				connection.commit()
			for folder_day in set_day:
				if not os.path.exists(to_folder + folder_day):
					os.mkdir(to_folder + folder_day)
				for f in lst_finish:
					print(from_folder + f)
					shutil.move(from_folder + f, to_folder + folder_day)

# except(Exception, psycopg2.Error) as error:
# 	print('Error while connecting to PostgreSQL', error)
# except(Exception, psycopg2.DatabaseError) as error:
# 	print('Error while creating PostgreSQL table', error)

except(Exception) as e:
	print('Error：', e)
	with open(logname, 'w', encoding='utf8') as f:
		f.write(str(e))	
finally:
	if (connection):
		cursor.close()
		connection.close()
		print('PostgreSQL connection is closed')
	t_end = time.time()
	print('Time:',str(datetime.timedelta(seconds=t_end-t_start)))