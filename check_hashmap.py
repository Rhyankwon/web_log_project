# See the LICENSE file for license rights and limitations.

# CREATED BY RHYANKWON 20220408

import pdb
import pickle

with open("dates_info.pickle", "rb") as fr1:
    info = pickle.load(fr1)

DAY, MONTH, YEAR = 1, 7, 31

plans = [DAY, MONTH, YEAR]

for plan in plans:
    check = 0
    sum_info = {}
    for i in info:
        if int(i) >= int('0110'): #이 날부터 확인
            check += 1
            # for category in info[i]: #평균
            #     if category in sum_info:
            #         sum_info[category] += (info[i][category])/plan
            #     else :
            #         sum_info[category] = info[i][category]/plan
            for category in info[i]: #누적 합
                if category in sum_info:
                    sum_info[category] += (info[i][category])
                else :
                    sum_info[category] = info[i][category]
        if check == plan:
            print(i)
            print(sum_info)
            break