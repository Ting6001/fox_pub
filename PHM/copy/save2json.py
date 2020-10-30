import json
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import math
from decimal import Decimal, ROUND_HALF_UP, ROUND_UP
import pandas as pd
import copy
import datetime
import time
import os

with open('.\info.txt', 'r', encoding='utf8') as f:
	dic_info = json.load(f)
from_folder = dic_info['ori_json_folder']
to_folder = dic_info['saved_json_folder']
logfolder = dic_info['log_folder']
samplefile = '.\data_sample.json'

def floatformat(num):
  num = Decimal(str(num)).quantize(Decimal('.00'), ROUND_HALF_UP)
  return float(num)

logname = logfolder + 'save2json_' + datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.log'
try:
  print(os.path.basename(__file__))
  with open(logname, 'w', encoding='utf8') as f:
    f.write('Start')
  num = 3000
  mean = [0, 0]
  cov = [[2, 0], [0, 2]]
  x, y = np.random.multivariate_normal(mean, cov, num).T
  # print(type(x), x.mean(), y.mean())

  df_center = pd.DataFrame(data=x, columns=["xvalue"])
  df_center["yvalue"] = y
  df_center['radius'] = df_center.apply(lambda row: math.sqrt(row.xvalue**2 + row.yvalue**2), axis=1)
  # print(df_center['xvalue'].mean(), df_center['yvalue'].mean(), df_center['radius'].mean())
  # print(df_center['xvalue'].std(), df_center['yvalue'].std(), df_center['radius'].std())

  # 模擬一個log檔含有多筆時間資料
  num = 10
  count_limit = 10


  # filename = from_folder + 'datas_sample'
  # sdate = "2020-10-06_10:00:01"
  # ori_date = datetime.datetime.strptime(sdate, "%Y-%m-%d_%H:%M:%S")
  ori_date = datetime.datetime.now()
  with open(samplefile, 'r') as f:
    lst_data = json.load(f)
    dic_sample = lst_data[0]

  count = 0
  while True:
      cur = datetime.datetime.now()
      if count > count_limit:
        break
      if (cur - ori_date).seconds % num == 0:
        ori_date = cur
        count += 1
        print(count, cur)
        time.sleep(1)
        lst_datas = []
        for i in range(num):
          dic_data = copy.deepcopy(dic_sample)
          addsec = num-i-1
          next_time = ori_date - datetime.timedelta(seconds=addsec)
          s_next_time = next_time.strftime('%Y-%m-%d_%H:%M:%S')
          timename = s_next_time.replace(':','-')
          dic_data['machine_para']['report_time'] = s_next_time
          dic_data['machine_para']['wafer_center_x'] = floatformat(1920 + df_center.at[i, 'xvalue'])
          dic_data['machine_para']['wafer_center_y'] = floatformat(1374 + df_center.at[i, 'yvalue'])
          dic_data['machine_para']['wafer_radius'] = floatformat(1233 + df_center.at[i, 'radius'])
          lst_datas.append(dic_data)
        with open(from_folder + timename + '.json', 'w') as f:
            json.dump(lst_datas, f, indent=4)

except Exception as e:
  with open(logname, 'w', encoding='utf8') as f:
    f.write(str(e))
finally:
  print('Finish')