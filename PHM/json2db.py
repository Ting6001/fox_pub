from sqlalchemy import create_engine
import csv
import psycopg2
import json
import time
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np
import os
import shutil
import spc_8rules
# from tqdm import tqdm
from logger import create_logger, get_err_dtl
# ===================================================================================
#  平均值從machine_mean取得
# ===================================================================================



# 【建立log檔】
workfilename = os.path.splitext(os.path.basename(__file__))[0]
logname = workfilename + '_' + \
    datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.log'
logger = create_logger(logname)  # 在 logs 目錄下建立 tutorial 目錄
logger.info(logname)
t_start = time.time()
tablename = 'machine_para'

# 【從設定檔讀取路徑】
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
get_pre_second = dic_info['get_pre_second'] # 每次更新資料時，需DB取出前幾秒的資料一併計算8rules
update_second = dic_info['update_second']  # 每隔幾秒更新一次資料
count_limit = dic_info['count_limit'] +1 # 模擬一個log檔含有多筆時間資料

folder_day = ''
json_lst_time = None
lst_mr = None
sql = ''
dic_errdtl = {1: '1個點落在A區(3 sigma)外',
              2: '連續3點中有2點落在中心線同一側的Zone B(2 sigma)以外',
              3: '連續5點有4點落在中心線同一側的Zone C(1 sigma)以外',
              4: '連續9個以上的點落在中心線同一側(Zone C以外)',
              5: '連續7點遞增or遞減',
              6: '連續8點皆無落在Zone C(1 sigma)',
              7: '連續15點落在中心線二側的Zone C(1 sigma)內',
              8: '連續14點相鄰交替上下', }

# 【組成 SQL Insret 語法】
def get_sql(dics, tablename, tp_datas, machine_id=None):
    global json_lst_time
    lst_col, lst_val = [], []
    for key in dics:
        dic = dics[key]
        for k, v in dic.items():
            if k == 'report_time':
                v = datetime.strptime(v, "%Y-%m-%d_%H:%M:%S")
                json_lst_time = v
            lst_col.append(k)
            lst_val.append(v)

    s_col = ','.join(lst_col)
    s_val = ','.join(['%s'] * (len(lst_col)))
    sql_insert = "INSERT INTO " + tablename + \
        '(' + s_col + ") VALUES (" + s_val + ")"
    tp_datas += (lst_val,)
    return sql_insert, tp_datas


def insert_table(cusor, filename, tablename):
    with open(filename, 'r') as f:
        lst_datas = json.load(f)
    tp_datas = ()
    sql = ''
    msg = 'json file count：' + str(len(lst_datas))
    logger.info(msg)
    for i in range(len(lst_datas)):  # list裡有多筆不同時間dict
        dic_data = lst_datas[i]
        sql, tp_datas = get_sql(dic_data, tablename, tp_datas)
    if sql != '':
        logger.info('Start execution')
        cusor.executemany(sql, tp_datas)

# 【從DB取出最新幾筆資料】
def getdata(cursor, from_time):
    global tablename
    sql = "select * from " + tablename +" where report_time >= '" + \
        from_time + "' order by report_time"
    cursor.execute(sql)
    res = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(res, columns=colnames)
    return df

# 【update，寫入分析結果】
def update_sql(cursor, colname, id, value):
    if len(id) == 0:
        return
    global tablename

    sql = '''
  WITH sample (id, value) AS (
      SELECT * FROM
          unnest(
              %(id)s,
              %(value)s
          )
  )
  UPDATE
      machine_para
      SET '''+ colname + ''' = s.value
      FROM sample s
      WHERE machine_para.id = s.id;
  '''
    logger.info(cursor.mogrify(sql))
    data = {
    'id' : id,
    'value' : value,
    'tablename' : tablename,
    'colname' : colname
    }
    cursor.execute(sql, data)

# =======================================================================================
#  取得違反各個rule的點dic{rule1：[list(id), list(value)], rule2：[list(id), list(value)]} 
#  轉換成dict{id：違反的各項rules} 如{1：(1,2,3)}
# 【Input】 ：dict
# 【Output】：dict
# =======================================================================================
def get_dicerr(dic_ofc):
    dic_err = {}
    for r in range(1, len(dic_ofc)+1):
        ofc_type = str(r)
        ofc = dic_ofc[r]
        ofc_id = ofc[0]
        # ofc_x = ofc[1]
        # ofc_y = ofc[-1]
        msg = "Against Rule %d:" % (r) + dic_errdtl[r] + ', count：' + str(len(ofc_id))
        logger.info(msg)
        for num in range(len(ofc_id)):
            key = ofc_id[num]
            dic_err[key] = dic_err.get(key, set()) | {ofc_type}
    return dic_err

# ===================================================================================
#  從machine_mean取得紀錄的x,y,z平均值
# 【Input】 ：tablename、dict(where條件)
# 【Output】：dataframe
# ===================================================================================
def get_mean(cursor, tablename, dic):
    sql = f"""select * from {tablename} where
	device_no = '{dic['device_no']}'
	and station_no = '{dic['station_no']}'
	and robot_no = '{dic['robot_no']}'"""

    cursor.execute(sql)
    res = cursor.fetchall()
    if res == None:
        return res
    else:
        colnames = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(res, columns=colnames)
        return df

# ===================================================================================
#  計算傳入資料違反哪些rule、moving range
# 【Input】 ：x,y資料
# 【Output】：dic_ichart、dic_mrchart{id：違反的rule string}、dic_mr {id：mr value}
# ===================================================================================
def get_err(ary_id, ary_x, ary_y, col, subgroup=1, dic_no=None):
    import numpy as np
    import matplotlib.pyplot as plt
    logger.info(dic_no)
    df_mean = get_mean(cursor, 'machine_mean', dic_no)
    e2 = float(df_mean.at[0, 'e2'])
    d4 = float(df_mean.at[0, 'd4'])
    x_bar = float(df_mean.at[0, 'mean_x'])
    mr_bar = float(df_mean.at[0, 'mean_mr_x'])

    dic_err_i_unit = {}
    dic_err_mr_unit = {}
    dic_mr_unit = {}
    lst_mr = []
    lst_mr = [abs(v - u) if not (u is None and v is None) else None  for u, v in zip(ary_y, ary_y[1:])] # |N(i-1) - N(i)| moving range目前的值與前一點的值差距
    logger.info(lst_mr)
    mr = np.array(lst_mr)
    dic_mr_unit['id'] = list(ary_id[1:])
    dic_mr_unit['mr'] = lst_mr
    # E2 = [2.660, 2.660, 1.772, 1.457, 1.290, 1.184, 1.109, 1.054, 1.010, 0.975]
    # D4 = [3.268, 3.268, 2.574, 2.282, 2.114, 2.004, 1.924, 1.864, 1.816, 1.777]
    # e2 = E2[subgroup-1]
    # d4 = D4[subgroup-1]

    # x_bar = ary_y.mean()   
    # mr_bar = mr.mean()  # moving range平均值

    # I chart
    # CL：資料平均值，UCL：CL+E2*avg(MR)，point：資料點
    x_ucl = x_bar + e2 * mr_bar
    x_ucl_c = x_bar + e2 * mr_bar * (1/3)
    x_ucl_b = x_bar + e2 * mr_bar * (2/3)
    x_lcl = x_bar - e2 * mr_bar
    x_lcl_c = x_bar - e2 * mr_bar * (1/3)
    x_lcl_b = x_bar - e2 * mr_bar * (2/3)

    # ======================================================
    # moving range變化作圖，moving range皆>0，所以LCL=0, UCL=D4*avg(MR)，point：moving range，mr變化大-->2點資料值變化大，不穩定?
    # CL：avg(MR)
    mr_ucl = d4 * mr_bar
    mr_ucl_c = mr_bar + (mr_ucl - mr_bar) * (1/3)
    mr_ucl_b = mr_bar + (mr_ucl - mr_bar) * (2/3)
    mr_lcl = 0
    # 原文此處計算錯誤 mr_lcl_c = mr_bar - (mr_ucl - mr_bar) * (1/3)
    mr_lcl_c = mr_bar - abs(mr_lcl - mr_bar) * (1/3)
    mr_lcl_b = mr_bar - abs(mr_lcl - mr_bar) * (2/3)
    if mr_lcl_c < 0:
        mr_lcl_c = 0
    if mr_lcl_b < 0:
        mr_lcl_b = 0

    # 8 rules
    # I chart，CL：avg(資料點)、UCL：CL+avg(MR)
    lst_exe = list(range(1, 9))
    dic_xofc = spc_8rules.rules(ary_id, ary_y, ary_x, x_bar,
                                x_ucl, x_ucl_b, x_ucl_c, x_lcl, x_lcl_b, x_lcl_c, lst_exe)
    dic_mrofc = spc_8rules.rules(ary_id[1:], mr, ary_x[1:], mr_bar,
                                 mr_ucl, mr_ucl_b, mr_ucl_c, mr_lcl, mr_lcl_b, mr_lcl_c, lst_exe)

    dic_err_i_unit = get_dicerr(dic_xofc)
    dic_err_mr_unit = get_dicerr(dic_mrofc)
    return dic_err_i_unit, dic_err_mr_unit, dic_mr_unit


try:
    connection = psycopg2.connect(database=s_database, user=s_user, password=s_password,host=s_host, port=s_port)	
    # print('Opened database successfully')
    cursor = connection.cursor()
    count = 1
    ori_date = datetime.now()

    while True:
        cur = datetime.now()
        if count > count_limit:
            break
        if (cur - ori_date).seconds % update_second == 0:
            ori_date = cur
            msg = 'update ' + str(count) + '：' + cur.strftime('%Y-%m-%d_%H:%M:%S')
            logger.info(msg)
            count += 1
            time.sleep(1)
    # ============ insert table ============
            json_lst_time = None
            lst_finish = []
            set_day = set()
            for f in os.listdir(from_folder):
                msg = 'open json file：' + f
                logger.info(msg)
                set_day.add(f.split('_')[0])
                filename = from_folder + f
                insert_table(cursor, filename, tablename)
                lst_finish.append(f)
                logger.info('Start commit')
                connection.commit()
                logger.info('finish commit')
    # ======================= 8 rules analysis ================================================
            if json_lst_time is not None:
                tz = timezone(timedelta(hours=+8))
                from_time = (json_lst_time - timedelta(seconds=4000+get_pre_second)
                             ).strftime('%Y-%m-%d %H:%M:%S')
                msg = 'json_lst_time:'+ json_lst_time.strftime('%Y-%m-%d %H:%M:%S') + ', from DB time：', from_time
                logger.info(msg)

                df = getdata(cursor, from_time)
                dic_no = {'device_no': df.loc[0, 'device_no'],
                        'station_no': df.loc[0, 'station_no'],
                        'robot_no': df.loc[0, 'robot_no']}
                ary_id = df.loc[:, 'id'].to_numpy()
                ary_time = df.loc[:, 'report_time'].to_numpy()
                ary_x = df.loc[:, 'wafer_center_x'].astype('float').to_numpy()
                ary_y = df.loc[:, 'wafer_center_y'].astype('float').to_numpy()
                ary_z = df.loc[:, 'wafer_center_z'].astype('float').to_numpy()
                logger.info('ary_z')
                logger.info(type(ary_z))
                logger.info(ary_z[0])
                ary_z = np.where(np.isnan(ary_z), None, ary_z)
                logger.info(type(ary_z))
                logger.info(ary_z[0])
                msg = '取得資料筆數:' + str(ary_x.shape)
                logger.info(msg)
                # logger.info(ary_time)

                dic_col = {'x': ary_x, 'y': ary_y, 'z':ary_z}
                # dic_col = {'x': ary_x}
                dic_err_i = {}
                dic_err_mr = {}
                dic_mr = {}
                # ======================= x, y, z 都要跑 8 rules 分析 ================================================
                for k, ary_value in dic_col.items():
                    dic_err_i[k], dic_err_mr[k], dic_mr[k] = get_err(ary_id, ary_time, ary_value, k, dic_no=dic_no)
                    # dic_err_i, dic_err_mr = get_err(ary_time, ary_x, dic_no=dic_no)
                    msg = k + ' i_chart error：' + str(len(dic_err_i[k].keys())) + ', mr_chart error：' + str(len(dic_err_mr[k]))
                    logger.info(msg)
                    colname = 'i_chart_' + k
                    logger.info(colname)
                    # id = list(dic_err_i[k].keys())
                    id = [int(k) for k in dic_err_i[k].keys()]
                    value = [','.join(sorted(i)) for i in dic_err_i[k].values()]
                    update_sql(cursor, colname, id, value)

                    colname = 'mr_chart_' + k
                    logger.info(colname)
                    # id = list(dic_err_mr[k].keys())
                    id = [int(k) for k in dic_err_mr[k].keys()]
                    value = [','.join(sorted(i)) for i in dic_err_mr[k].values()]
                    
                    update_sql(cursor, colname, id, value)

                    colname = 'mr_' + k
                    logger.info(colname)
                    value = dic_mr[k]['mr']
                    id = dic_mr[k]['id']
                    print(type(id[0]))
                    id = [int(k) for k in dic_mr[k]['id']]
                    value[0] = 1
                    
                    update_sql(cursor, colname, id, value)
                logger.info('Start Commit')    
                connection.commit()
                logger.info('Finish commit')
            for folder_day in set_day:
                if not os.path.exists(to_folder + folder_day):
                    os.mkdir(to_folder + folder_day)
                for f in lst_finish:
                    msg = 'move json file：' + from_folder + f
                    logger.info(msg)
                    shutil.move(from_folder + f, to_folder + folder_day)

except(Exception) as e:
    logger.error(get_err_dtl(e))
finally:
    if (connection):
        cursor.close()
        connection.close()
        logger.warning('PostgreSQL connection is closed')
    t_end = time.time()
    msg = 'Time：' + str(timedelta(seconds=t_end-t_start))
    logger.warning(msg)
