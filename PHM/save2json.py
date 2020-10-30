import json
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import math
from decimal import Decimal, ROUND_HALF_UP, ROUND_UP
import pandas as pd
import copy
import datetime
from datetime import datetime, timedelta
import time
import os
from logger import create_logger, get_err_dtl

# 【建立log檔】
workfilename = os.path.splitext(os.path.basename(__file__))[0]
logname = workfilename + '_' + \
    datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.log'
logger = create_logger(logname)  
logger.info(logname)
t_start = time.time()

# 【從設定檔讀取路徑】
with open('.\info.txt', 'r', encoding='utf8') as f:
    dic_info = json.load(f)
from_folder = dic_info['ori_json_folder']
to_folder = dic_info['saved_json_folder']
logfolder = dic_info['log_folder']
samplefile = '.\data_sample.json'
update_second = dic_info['update_second']  # 每隔幾秒更新一次資料
count_limit = dic_info['count_limit'] # 模擬一個log檔含有多筆時間資料
# 若目錄不存在則新建
if not os.path.exists(from_folder):
    os.makedirs(from_folder)
if not os.path.exists(to_folder):
    os.makedirs(to_folder)

def floatformat(num):
    num = Decimal(str(num)).quantize(Decimal('.00'), ROUND_HALF_UP)
    return float(num)

try:
    num = update_second

    # 【生成常態分佈資料】
    # mean 跟 cov 長度需一致，產生shape是size * len(mean)，此例是 num * 2
    mean = [0, 0] 
    cov = [[2, 0], [0, 2]]
    x, y = np.random.multivariate_normal(mean, cov, num).T
    # print(type(x), x.mean(), y.mean())

    df_center = pd.DataFrame(data=x, columns=["xvalue"])
    df_center["yvalue"] = y
    df_center['radius'] = df_center.apply(lambda row: math.sqrt(row.xvalue**2 + row.yvalue**2), axis=1)
    # print(df_center['xvalue'].mean(), df_center['yvalue'].mean(), df_center['radius'].mean())
    # print(df_center['xvalue'].std(), df_center['yvalue'].std(), df_center['radius'].std())

    # 【讀取範例檔，做出num筆資料】
    x_mean = 1920
    y_mean = 1373
    r_mean = 1233
    ori_date = datetime.now()
    logger.info('open json file')
    with open(samplefile, 'r') as f:
        lst_data = json.load(f)
        dic_sample = lst_data[0]

    count = 1
    while True:
        cur = datetime.now()
        if count > count_limit:
            break
        if (cur - ori_date).seconds % update_second == 0:
            ori_date = cur
            msg = 'update ' + str(count) + '：' + cur.strftime('%Y-%m-%d_%H:%M:%S')
            logger.info(msg)
            count += 1
            # print(count, cur.strftime('%Y-%m-%d_%H:%M:%S'))
            time.sleep(1)
            lst_datas = []
            # 取 ~now時間點 num筆的資料，若Now:9:00:10, num=10-->取9:00:01~9:00:10的資料
            for i in range(num):
                dic_data = copy.deepcopy(dic_sample)
                addsec = num-i-1
                next_time = ori_date - timedelta(seconds=addsec)
                s_next_time = next_time.strftime('%Y-%m-%d_%H:%M:%S')
                timename = s_next_time.replace(':', '-')
                dic_data['machine_para']['report_time'] = s_next_time
                dic_data['machine_para']['wafer_center_x'] = floatformat(
                    x_mean + df_center.at[i, 'xvalue'])
                dic_data['machine_para']['wafer_center_y'] = floatformat(
                    y_mean + df_center.at[i, 'yvalue'])
                dic_data['machine_para']['wafer_radius'] = floatformat(
                    r_mean + df_center.at[i, 'radius'])
                lst_datas.append(dic_data)
            with open(from_folder + timename + '.json', 'w') as f:
                json.dump(lst_datas, f, indent=4)

except Exception as e:
    logger.error(get_err_dtl(e))
finally:
    t_end = time.time()
    msg = 'Time：' + str(timedelta(seconds=t_end-t_start))
    logger.warning(msg)
