import json 
import os
from src.queue.offset_manager import get_offset,update_offset

BASE_PATH = "data/queue"

def read_new_batch(topic):
    file_path =os.path.join(BASE_PATH,f"{topic}.json")

    if not os.path.exists(file_path):
        return [], 0 
    
    offset = get_offset(topic)

    with open(file_path,"r") as f:
        lines = f.readlines()

    new_lines = lines[offset:]

    if not new_lines:
        return [], offset

    data = [json.loads(line) for line in new_lines]


    return data, offset