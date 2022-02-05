import datetime
import schedule
import time

import redis


def filterNePM():
    '''Lay ra sanh sach cac Ne khong gui update time
    1. Loc ds set1 - qua han va set2 - co update
    2. Quet set1
    3. Quet set2
    '''
    t1 = datetime.datetime.now()
    expiredTime = 3  # giả sử
    currentTime = 3  # giả sử
    r = redis.Redis(host="localhost", port=6379)
    # 1. lay ra 2 set Ne
    set1 = r.zrangebyscore(
        name="ne_pm_update",
        min=0,
        max=0,
        withscores=True
    )
    set2 = r.zrangebyscore(
        name="ne_pm_update",
        min=1,
        max=9,
        withscores=True
    )
    print(len(set1))
    print(len(set2))
    t2 = datetime.datetime.now()
    print(f"Time select 2 sets taken: {t2 - t1}")
    # 2. Quet set1
    for e in set1:
        pass
    # 3. Quet set2


# ------------------------START--------------------------
# schedule.every(1).minute.do(filterNePM)
# while True:
#     schedule.run_pending()
#     time.sleep(1)

filterNePM()
