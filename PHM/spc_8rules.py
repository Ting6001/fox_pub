import numpy as np

# 【rule 1】1個點落在A區(3 sigma)外
def rule_1(ids, data, obs, cl, ucl, ucl_b, ucl_c, lcl, lcl_b, lcl_c, sec=0):
    print('rule_1')
    ofc_ind = []
    for i in range(len(data)):
        d = data[i]
        if d != None and d > ucl and d< lcl:
            ofc_ind.append(i)
    return (ids[ofc_ind], obs[ofc_ind], data[ofc_ind])


# 【rule 2】連續3點中有2點落在中心線同一側的Zone B(2 sigma)以外
def rule_2(ids, data, obs, cl, ucl, ucl_b, ucl_c, lcl, lcl_b, lcl_c, sec=0):
    ofc_ind = []
    ind = np.array(range(len(data)))
    # k = 3
    for i in range(len(data)-2):  # 遞迴到倒數第二個(可取出-2,-1,0)
        d = data[i:i+3]    # 一次取出3個點
        index = ind[i:i+3]
        if not None in d:
            # 2個array可以比較每個元素的大小並加總結果(ary1>ary2).sum()
            # array比大小，return True/False array
            # list.extend(lst)會把lst裡的元素拆開一個個append進list
            if ((d > ucl_b).sum() == 2) | ((d < lcl_b).sum() == 2):
                ofc_ind.extend(index[(d > ucl_b) | (d < lcl_b)])
    return (ids[ofc_ind], obs[ofc_ind], data[ofc_ind])


# 【rule 3】連續5點有4點落在中心線同一側的Zone C(1 sigma)以外
def rule_3(ids, data, obs, cl, ucl, ucl_b, ucl_c, lcl, lcl_b, lcl_c, sec=0):
    ofc_ind = []
    ind = np.array(range(len(data)))
    for i in range(len(data)-4):
        d = data[i:i+5]  # 一次取出5個點
        index = ind[i:i+5]
        if not None in d:
            if ((d > ucl_c).sum() == 4) | ((d < lcl_c).sum() == 4):
                ofc_ind.extend(index[(d > ucl_c) | (d < lcl_c)])
    return (ids[ofc_ind], obs[ofc_ind], data[ofc_ind])


# 【rule 4】連續9個以上的點落在中心線同一側(Zone C以外)
def rule_4(ids, data, obs, cl, ucl, ucl_b, ucl_c, lcl, lcl_b, lcl_c, sec=0):
    ofc_ind = []
    ind = np.array(range(len(data)))
    for i in range(len(data)-8):
        d = data[i:i+9]
        index = ind[i:i+9]
        if not None in d:
            if ((d > cl).sum() == 9) | ((d < cl).sum() == 9):
                ofc_ind.extend(index)
    return (ids[ofc_ind], obs[ofc_ind], data[ofc_ind])


# 【rule 5】連續7點遞增or遞減
def rule_5(ids, data, obs, cl, ucl, ucl_b, ucl_c, lcl, lcl_b, lcl_c, sec=0):
    ofc_ind = []
    ind = np.array(range(len(data)))
    for i in range(len(data)-6):
        d = data[i:i+7]
        index = ind[i:i+7]
        # all()只要有一個是false就return false
        if not None in d:
            if all(u <= v for u, v in zip(d, d[1:])) | all(u >= v for u, v in zip(d, d[1:])):
                ofc_ind.extend(index)
    return (ids[ofc_ind], obs[ofc_ind], data[ofc_ind])


# 【rule 6】連續8點皆無落在Zone C(1 sigma)
def rule_6(ids, data, obs, cl, ucl, ucl_b, ucl_c, lcl, lcl_b, lcl_c, sec=0):
    ofc_ind = []
    ind = np.array(range(len(data)))
    for i in range(len(data)-7):
        d = data[i:i+8]
        index = ind[i:i+8]
        if not None in d:
            if (all(d > ucl_c) | all(d < lcl_c)):
                ofc_ind.extend(index)
    return (ids[ofc_ind], obs[ofc_ind], data[ofc_ind])


# 【rule 7】連續15點落在中心線二側的Zone C(1 sigma)內
def rule_7(ids, data, obs, cl, ucl, ucl_b, ucl_c, lcl, lcl_b, lcl_c, sec=0):
    ofc_ind = []
    ind = np.array(range(len(data)))
    for i in range(len(data)-14):
        d = data[i:i+15]
        index = ind[i:i+15]
        if not None in d:
            if all(d > lcl_c) and all(d < ucl_c):
                ofc_ind.extend(index)
    return (ids[ofc_ind], obs[ofc_ind], data[ofc_ind])


# 【rule 8】連續14點相鄰交替上下
def rule_8(ids, data, obs, cl, ucl, ucl_b, ucl_c, lcl, lcl_b, lcl_c, sec=0):
    ofc_ind = []
    ind = np.array(range(len(data)))
    for i in range(len(data)-13):
        d = data[i:i+14]
        index = ind[i:i+14]
        if not None in d:
            diff = list(v-u for u, v in zip(d, d[1:]))
            if all(u*v < 0 for u, v in zip(diff, diff[1:])):
                ofc_ind.extend(index)
    return (ids[ofc_ind], obs[ofc_ind], data[ofc_ind])


def rules(ids, data, obs, cl, ucl, ucl_b, ucl_c, lcl, lcl_b, lcl_c, lst_exe=list(range(1, 9)), sec=0):
    lst_rules = [rule_1, rule_2, rule_3,
                 rule_4, rule_5, rule_6, rule_7, rule_8]
    dic_ofc = {}
    for i in range(1, 9):
        dic_ofc[i] = ([],)*3

    for i in lst_exe:
        print('rule_', i)
        fun = lst_rules[i-1]
        dic_ofc[i] = fun(ids, data, obs, cl, ucl, ucl_b,
                         ucl_c, lcl, lcl_b, lcl_c)
    return dic_ofc
