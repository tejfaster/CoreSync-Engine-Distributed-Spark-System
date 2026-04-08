import os

OFFSET_PATH = "data/offset"

def get_offset(topic):
    os.makedirs(OFFSET_PATH,exist_ok=True)
    file_path = os.path.join(OFFSET_PATH,f"{topic}.offset")

    if not os.path.exists(file_path):
        return 0 
    
    with open(file_path,"r") as f:
        return int(f.read().strip())
    
def update_offset(topic,new_offset):
    file_path = os.path.join(OFFSET_PATH,f"{topic}.offset")

    with open(file_path,"w") as f:
        f.write(str(new_offset))
