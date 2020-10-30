import sys
import traceback
import test

try:
    test.run()
except Exception as e:
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
    errMsg = "File \"{filename}\", line {linenum}, in {funcname}: [{error_class}] {detail}".format(
        filename=fileName, linenum=lineNum, funcname=funcName, error_class=error_class, detail=detail)
    print(errMsg)