from sqlalchemy import create_engine
import csv
import psycopg2
import json
import datetime, time
import pandas as pd
import os

t_start = time.time()
def create_table(cursor, filename):
	tablename = os.path.splitext(os.path.basename(filename))[0]
	s = '(\n'
	comment = ''

	with open(filename, 'r', encoding='utf8', newline='') as f:
		reader = csv.reader(f, delimiter=',')
		pk = next(reader)[1]
		header = next(reader)
		for row in reader:
			colname, colunit, colnull, colcomment = row[1], row[2], row[3], row[-2]
			s += (colname + ' ' + colunit + ' ' + colnull + ',\n')
			comment += ('comment on column ' + tablename +'.' + colname + " is '" + colcomment + "';\n")

	sql_createtable = ' create table if not exists '+ tablename + s + '''
	primary key (''' + pk + '));\n'

	cursor.execute(sql_createtable + comment)
	return sql_createtable + comment

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
	# if tablename == 'machine_para':
	# 	lst_col = ['machine_id']
	# 	lst_val = [machine_id]
	# else:
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
		if i % 100 == 0:
			print(i)
		dic_data = lst_datas[i]
		# machine_id = get_machine_id(cursor, dic_data)
		# if not ((tablename == 'machine_info' and machine_id) or (tablename == 'machine_info' and machine_id in lst_machine_id)) :
		# 	lst_machine_id.append(machine_id)
		sql, tp_datas = get_sql(dic_data, tablename, tp_datas)
	# print(sql, '\n', tp_datas)
	# print('============')
	if sql != '':
		cusor.executemany(sql, tp_datas)
		print('Finish execution')

try:
	# connection = psycopg2.connect(database='dbfoxconn', user='foxconn', password='foxconn',
	# 	host='35.185.149.0', port='5432')	
	connection = psycopg2.connect(database='dbfoxconn', user='foxconn', password='foxconn',
		host='35.223.193.119', port='5432')	
	# print('Opened database successfully')

	cursor = connection.cursor()
	# print(connection.get_dsn_parameters(), '\n')
	# cursor.execute('select version();')
	# record = cursor.fetchone()
	# print('You are connected to - ', record, '\n')
	
	# ============ create table ============
	# create_table(cursor, 'D:\\_MiaFoxconn\\PHM\\log\\machine_info.csv')
	create_table(cursor, 'D:\\_MiaFoxconn\\PHM\\log\\machine_para.csv')

	# ============ insert table ============
	# filename = 'D:\\_MiaFoxconn\\PHM\\log\\datas_sample_3000.json'
	# insert_table(cursor, filename, 'machine_para')
	connection.commit()

	# insert_table(cursor, filename, 'machine_para')
	# connection.commit()

	# a = 'drop table if exists machine_info, machine_para;'
	# cursor.execute(a)
	# cursor.executemany(sql_insert, tp_datas)
	# connection.commit()

# except(Exception, psycopg2.Error) as error:
# 	print('Error while connecting to PostgreSQL', error)
# except(Exception, psycopg2.DatabaseError) as error:
# 	print('Error while creating PostgreSQL table', error)
except(Exception) as e:
	print('Error：', e)
finally:
	if (connection):
		cursor.close()
		connection.close()
		print('PostgreSQL connection is closed')
	t_end = time.time()
	print('Time:',str(datetime.timedelta(seconds=t_end-t_start)))