import json
import redis
import time

ms = 1000

def creation(redis_client, data, create_time, select_time):
    start_time = time.time()
    redis_client.set("data_string", str(data))
    create_time.append((time.time() - start_time) * 1000)
    start_time = time.time()
    redis_client.hset("data_hset", mapping={
        "data" : str(data)
    })
    create_time.append((time.time() - start_time) * 1000)
    start_time = time.time()
    redis_client.zadd("data_zset", mapping={
        str(data) : 1
    })
    create_time.append((time.time() - start_time) * 1000)
    start_time = time.time()
    redis_client.lpush("data_list", str(data))
    create_time.append((time.time() - start_time) * 1000)

def selection(redis_client, data, create_time, select_time):
    start_time = time.time()
    redis_client.get("data_string")
    select_time.append(ms * (time.time() - start_time))
    start_time = time.time()
    redis_client.hgetall("data_hset")
    select_time.append(ms * (time.time() - start_time))
    start_time = time.time()
    redis_client.zrange("data_zset", 0, -1)
    select_time.append(ms * (time.time() - start_time))
    start_time = time.time()
    redis_client.lrange("data_list", 0, -1)
    select_time.append(ms * (time.time() - start_time))

def print_results(data_structures, create_time, select_time):
    print("Creation")
    for i in range(len(data_structures)):
       print(data_structures[i], create_time[i])

    print("\n")
    print("Selection")
    for i in range(len(data_structures)):
       print(data_structures[i], select_time[i])

def run():
    redis_client = redis.Redis(host='localhost', port=6385, decode_responses=True)
    with open("data.json", "r", encoding="utf-8") as file:
       data = json.load(file)

    create_time = []
    select_time = []
    data_structures = ["string", "hset", "zset", "list"]
    creation(redis_client, data, create_time, select_time)
    selection(redis_client, data, create_time, select_time)
    print_results(data_structures, create_time, select_time)
    redis_client.close()

run()