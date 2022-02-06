import random
import sys

# totalRecord = 10
totalRecord = 7000000


def gen_redis_proto(cmd):
    # break the command by spaces to get the number of tokens
    tokens = cmd.split(" ")
    proto = f"*{str(len(tokens))}\n"
    for token in tokens:
        proto += f"${str(len(token))}\n"
        proto += f"{token}\n"
    return proto


def createFile():
    with open("ne_command.txt", 'w', encoding='utf-8') as f:
        for i in range(1, totalRecord):
            #-----------pm
            lastTime = random.randint(0, 9)
            #hset hash
            f.writelines(gen_redis_proto(f"HSET ne_pm:{i} lasttime {lastTime}"))
            f.writelines(gen_redis_proto(f"HSET ne_pm:{i} alarm_init 0"))
            f.writelines(gen_redis_proto(f"HSET ne_pm:{i} alarm_trigger 0"))
            #zadd sorted set
            f.writelines(gen_redis_proto(f"ZADD ne_pm_update {lastTime} ne_pm:{i}"))

            #------------fm
            lastTime2 = random.randint(0, 9)
            #hset hash
            f.writelines(gen_redis_proto(f"HSET ne_fm:{i} lasttime {lastTime2}"))
            f.writelines(gen_redis_proto(f"HSET ne_fm:{i} alarm_init 0"))
            f.writelines(gen_redis_proto(f"HSET ne_fm:{i} alarm_trigger 0"))
            #zadd sorted set
            f.writelines(gen_redis_proto(f"ZADD ne_fm_update {lastTime2} ne_fm:{i}"))


createFile()
