import logging
from logging.config import fileConfig
import os
from datetime import datetime
import sys
import traceback

dir_path = './logs/'
# filename = "{:%Y-%m%d-%H%M%S}".format(datetime.now()) + '.log' # 設定檔名

# 層級順序：debug、info、warning、error、critical
# 若將logging 的 level 設為 logging.INFO 的時候，所有debug()的訊息將會被自動忽略，其餘顯示
# 若將logging 的 level 設成 logging.ERROR 的時候，所有 debug(), info(), warning()的訊息將會被忽略，error、critical會顯示
# 若無設定，logging 的 level 預設會是 logging.WARNING
 
# fileConfig('logging_config.ini')
# logger = logging.getLogger('MainLogger')

# fh = logging.FileHandler('{:%Y-%m%d-%H%M}.log'.format(datetime.now()))
# formatter = logging.Formatter(fmt='%(asctime)s | %(levelname)-8s | %(lineno)04d | %(message)s',
# 								datefmt='%Y-%m-%d %H:%M')

# fh.setFormatter(formatter)
# logger.addHandler(fh) # 新增FileHandler

# logger = logging.getLogger()
# logger.debug('often makes a very good meal of %s', 'visiting tourists')

# logging.basicConfig(level=logging.DEBUG,
#                     format='%(asctime)s %(levelname)s %(message)s',
#                     datefmt='%Y-%m-%d %H:%M',
#                     handlers=[logging.FileHandler('my.log', 'w', 'utf-8'), ])

def create_logger(filename):
	# config
	logging.captureWarnings(True) # 捕捉 py waring message
	formatter = logging.Formatter(fmt='%(asctime)s | %(levelname)-8s | %(lineno)04d | %(message)s',
								datefmt='%Y-%m-%d %H:%M:%S')
	my_logger = logging.getLogger('py.warnings') # 捕捉 py waring message
	my_logger.setLevel(logging.INFO)

	# 若不存在目錄則新建
	if not os.path.exists(dir_path):
		os.makedirs(dir_path)

	# file handler
	fileHandler = logging.FileHandler(dir_path+'/'+filename, 'w', 'utf-8')
	fileHandler.setFormatter(formatter)
	my_logger.addHandler(fileHandler)

	# console handler
	consoleHandler = logging.StreamHandler()
	consoleHandler.setLevel(logging.DEBUG)
	consoleHandler.setFormatter(formatter)
	my_logger.addHandler(consoleHandler)

	return my_logger
	#################################################################

def get_err_dtl(e):
    error_class = e.__class__.__name__ #取得錯誤類型
    detail = e.args[0] #取得詳細內容
    cl, exc, tb = sys.exc_info() #取得Call Stack
    # print('Error class Name:', e.__class__.__name__)
    # print('Error detail:', detail)

    # ======= 紀錄Trace Error發生各節點，最外層>內層，所以error真正發生位置在[-1] =======
    lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
    fileName = lastCallStack[0] #取得發生的檔案名稱
    lineNum = lastCallStack[1]  #取得發生的行號
    funcName = lastCallStack[2] #取得發生的函數名稱
    errMsg = "File \"{filename}\", at line {linenum}, in Function {funcname}, Error Msg: [{error_class}] {detail}".format(
        filename=fileName, linenum=lineNum, funcname=funcName, error_class=error_class, detail=detail)
    # print(errMsg)
    return errMsg