import json
import os

BASE_PATH = "data/queue"

def write_batch(topic,data):
    os.makedirs(os.path.dirname(BASE_PATH),exist_ok=True)

    file_path = f"{BASE_PATH}/{topic}.json"

    with open(file_path,"a") as f:
        f.write(json.dumps(data) + "\n")


def read_batches():
    if not os.path.exists(BASE_PATH):
        return []
    
    with open(BASE_PATH,"r") as f:
        lines = f.readlines()

    return [json.loads(line) for line in lines]    

def clear_queue():
    open(BASE_PATH,"w").close()