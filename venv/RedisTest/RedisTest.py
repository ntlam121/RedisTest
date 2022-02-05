import datetime
import random
import redis
import threading

def testConnectServer():
    r = redis.Redis(host='192.168.1.13', port=6379)
    print(r.ping())

# insert data to redis
def insertData(thrName, fromNo, toNo) -> None:
    t1 = datetime.datetime.now()

    r = redis.Redis(host="localhost", port=6379, db=0)
    with r.pipeline() as pipe:
        for i in range(fromNo, toNo):
            lastTime = random.randint(0, 9)
            insertHash_ne(i, pipe, 'pm', lastTime)
            # insertHash_ne(i, pipe, 'fm', lastTime)
            # insertSortedSet_ne_update(i, pipe, 'pm', lastTime)
            # insertSortedSet_ne_update(i, pipe, 'fm', lastTime)
        pipe.execute()
    t2 = datetime.datetime.now()
    print(f"Time taken {thrName}: {t2 - t1}")
    # r.bgsave()


# insert data for hashmap ne_pm or ne_fm
def insertHash_ne(i, pipe, pm_or_fm, lastTime) -> None:
    ne_map = {
        "last_time": lastTime,
        "alarm_init": "",
        "alarm_trigger": ""
    }
    for k, v in ne_map.items():
        pipe.hset(f"ne_{pm_or_fm}:{i}", k, v)


# insert data for sorted set ne_update (pm or fm)
def insertSortedSet_ne_update(i, pipe, pm_or_fm, lastTime) -> None:
    pipe.zadd(f"ne_{pm_or_fm}_update", {lastTime: f"ne_{pm_or_fm}:{i}"})

def getNeHasNotUpdate():
    '''Lay ra sanh sach cac Ne khong gui update time
    1. Loc ds set1 - qua han va set2 - co update
    2. Quet set1
    3. Quet set2
    '''
    t1 = datetime.datetime.now()
    expiredTime = 3  # giả sử
    currentTime = 3  # giả sử
    r = redis.Redis(host="localhost", port=6379)
    p = r.pipeline()
    set1 = p.zrangebyscore(
        name="ne_pm_update",
        min=0,
        max=(currentTime - expiredTime),
        withscores=True
    )
    p.execute()
    p.close()
    print(len(set1))
    t2 = datetime.datetime.now()
    print(f"Time taken: {t2 - t1}")

# ====================START=============================
# insertData("thread1", 1, 100000)
# try:
#     t1 = threading.Thread(target=insertData, args=("thread1", 1, 50000))
#     t2 = threading.Thread(target=insertData, args=("thread2", 50001, 100000))
#     t1.start()
#     t2.start()
#     t1.join()
#     t2.join()
# except:
#    print("Error: unable to start thread")
# ====================END===============================
# getNeHasNotUpdate()
testConnectServer()