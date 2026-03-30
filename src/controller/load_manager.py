import random 
from multiprocessing import cpu_count

TARGET = 100
TOTAL_DATA = 4000
RUNS = 4
TOTAL_CORES = cpu_count()
USABLE_CORES = TOTAL_CORES - 2


PER_RUN =TOTAL_DATA // RUNS

def generate_workload():
    usa = random.randint(0,PER_RUN)
    eu = PER_RUN - usa
    return usa , eu

def allocate_cores(usa,eu):
    total = usa + eu

    if total == 0:
        return 1,1

    usa_cores = max(1,int((usa/total) * USABLE_CORES))
    eu_cores = USABLE_CORES - usa_cores

    return usa_cores,eu_cores

# usa, eu = generate_workload()
# usa_cores,eu_cores = allocate_cores(usa,eu)



# print("usa work load",usa)
# print("eu work load",eu)
# print("usa core",usa_cores)
# print("eu core",eu_cores)