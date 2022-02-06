import datetime
import schedule
import time
import logging
import redis

hostStr = "localhost"
# hostStr = "192.168.1.13"
logging.basicConfig(level=logging.DEBUG, filename='runtime.log', filemode='w')
lstA = []  # list các ne quá hạn lọc sau mỗi x phút (từ sorted set ne_pm_update)
lstB = []  # list các ne đã gửi alarm (từ hash ne_pm_alarm)
lstAdiffB = []  # list A-B => những cái vi phạm chưa gửi alarm => gửi alarm new
lstAintersectionB = []  # list A giao B => những cái vi phạm đã gửi => gửi alarm change
lstBdiffA = []  # list B-A => những cái đã gửi alarm nhưng không còn vi phạm nữa => gửi alarm clear


def getListA():
    '''Set cac ne co thoi gian vi pham
    Lay tu SortedSet ne_xx_update theo truong thoi gian update'''
    try:
        t1 = datetime.datetime.now()
        r = redis.Redis(host=hostStr, port=6379)
        lst = r.zrangebyscore(name="ne_pm_update", min=0, max=2, withscores=True)
        t2 = datetime.datetime.now()
        new_lst = [l[0].decode('utf-8') for l in lst]
        logging.info(f"\tTime getListA: {t2 - t1}")
        return new_lst
    except Exception as e:
        logging.critical(e, exc_info=True)


def getListB():
    '''Set các ne đã gửi alarm
    Lấy từ hashmap ne_xx_alarm'''
    try:
        t1 = datetime.datetime.now()
        r = redis.Redis(host=hostStr, port=6379)
        lst_dict = r.hgetall(name="ne_pm_alarm")
        lst = list(lst_dict.items())
        t2 = datetime.datetime.now()
        new_lst = [l[0].decode('utf-8') for l in lst]
        logging.info(f"\tTime getListB: {t2 - t1}")
        return new_lst
    except Exception as e:
        logging.critical(e, exc_info=True)


def lstAdiffB_processing(lstAdiffB):
    try:
        t1 = datetime.datetime.now()
        if (len(lstAdiffB) > 0):
            r = redis.Redis(host=hostStr, port=6379)
            pipe = r.pipeline()
            for ne in lstAdiffB:
                # logging.info(f"Send alarm NEW: {ne}")
                pipe.hset(name="ne_pm_alarm", key=ne, value=3)
            pipe.execute()
        t2 = datetime.datetime.now()
        logging.info(f"\tTime lstAdiffB_processing: {t2 - t1}")
    except Exception as e:
        logging.critical(e, exc_info=True)


def lstAintersectionB_processing(lstAintersectionB):
    try:
        t1 = datetime.datetime.now()
        if (len(lstAintersectionB) > 0):
            r = redis.Redis(host=hostStr, port=6379)
            pipe = r.pipeline()
            for ne in lstAintersectionB:
                # logging.info(f"Send alarm CHANGE: {ne}")
                pipe.hset(name="ne_pm_alarm", key=ne, value=3)
            pipe.execute()
        t2 = datetime.datetime.now()
        logging.info(f"\tTime lstAintersectionB_processing: {t2 - t1}")
    except Exception as e:
        logging.critical(e, exc_info=True)


def lstBdiffA_processing(lstBdiffA):
    try:
        t1 = datetime.datetime.now()
        if (len(lstBdiffA) > 0):
            r = redis.Redis(host=hostStr, port=6379)
            pipe = r.pipeline()
            for ne in lstBdiffA:
                # logging.info(f"Send alarm CLEAR: {ne}")
                pipe.hdel("ne_pm_alarm", f"{ne}")
            pipe.execute()
        t2 = datetime.datetime.now()
        logging.info(f"\tTime lstBdiffA_processing: {t2 - t1}")
    except Exception as e:
        logging.critical(e, exc_info=True)


def intersection(lst1, lst2):
    # Use of hybrid method
    temp = set(lst2)
    lst3 = [value for value in lst1 if value in temp]
    return lst3


def main1():
    try:
        global lstA, lstB, lstAdiffB, lstBdiffA, lstAintersectionB
        t1 = datetime.datetime.now()
        lstA = getListA()
        logging.info(f"lstA: {len(lstA)}")
        lstB = getListB()
        logging.info(f"lstB: {len(lstB)}")
        ###Bước 1: lấy lstAdiffB (2 cách, đo xem cái nào nhanh hơn thì chọn)
        # Cách 1: đổi về set và thực hiện trừ
        set_difference = set(lstA) - set(lstB)
        lstAdiffB = list(set_difference)
        # Cách 2:
        # lstAdiffB = [item for item in lstA if item not in lstB]
        logging.info(f"lstAdiffB: {len(lstAdiffB)}")

        ###Bước 2: lấy lstBdiffA
        # lstBdiffA = [item for item in lstB if item not in lstA]
        set_difference1 = set(lstB) - set(lstA)
        lstBdiffA = list(set_difference1)
        logging.info(f"lstBdiffA: {len(lstBdiffA)}")

        ###Bước 3: lấy lstAintersectionB
        if (len(lstA) == len(lstB) and len(lstAdiffB) == 0 and len(lstBdiffA) == 0):
            lstAintersectionB = lstA
        else:
            lstAintersectionB = intersection(lstA, lstB)
        logging.info(f"lstAintersectionB: {len(lstAintersectionB)}")

        ###Bước 4: quét lstAdiffB và gửi new + cập nhật vào hash ne_pm_alarm
        lstAdiffB_processing(lstAdiffB)

        ###Bước 5: quét lstAintersectionB và gửi change + cập nhật vào hash ne_pm_alarm
        lstAintersectionB_processing(lstAintersectionB)

        ###Bước 6: quét lstBdiffA và gửi clear + xóa các phần tử này khỏi hash ne_pm_alarm
        lstBdiffA_processing(lstBdiffA)
        t2 = datetime.datetime.now()
        logging.info(f"\tTime all process: {t2 - t1}")
    except Exception as e:
        logging.critical(e, exc_info=True)


# ------------------------START--------------------------
# schedule.every(1).minute.do(filterNePM)
# while True:
#     schedule.run_pending()
#     time.sleep(1)

main1()
