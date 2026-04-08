import psutil
import os
import time

def monitor_system():
    cpu = psutil.cpu_percent(interval=1)
    ram = psutil.virtual_memory()
    cores = psutil.cpu_count()

    return{
        "cpu_percent" : cpu,
        "ram_used_gb" : round(ram.used /(1024**3),2),
        "ram_total_gb" : round(ram.total /(1024**3),2),
        "ram_percent" : ram.percent,
        "cores_available" : cores
    }