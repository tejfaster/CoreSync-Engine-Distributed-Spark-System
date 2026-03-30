import time
from src.controller.load_manager import allocate_cores, generate_workload
from src.pipeline.run_parallel import run_jobs , run_gold
from src.utils.profling import monitor_system

INTERVAL = 5 
# BUFFER = 10 * 60


def main():
    cycle = 0 
    while True:
        cycle += 1

        print("\n Running Cycle...\n",cycle)

        usa, eu = generate_workload()
        usa_cores,eu_cores = allocate_cores(usa,eu)

        # creating job
        jobs = [
            {"region":"usa","load":usa,"cores":usa_cores},
            {"region":"eu","load":eu,"cores":eu_cores}
        ]
        before = monitor_system()
        print(f"BEFORE -> CPU:{before['cpu_percent']}% . RAM:{before['ram_used_gb']}GB ")
        run_jobs(jobs)
        durings = monitor_system()
        # time.sleep(INTERVAL)
        
        print(f"During -> CPU:{durings['cpu_percent']}% . RAM:{durings['ram_used_gb']}GB ")
        # run_gold(jobs)
        # duringg = monitor_system()
        time.sleep(INTERVAL)
        # print(f"During -> CPU:{duringg['cpu_percent']}% . RAM:{duringg['ram_used_gb']}GB ")
        after = monitor_system()
        print(f"After -> CPU:{after['cpu_percent']}% . RAM:{after['ram_used_gb']}GB ")



if __name__ == "__main__":
    main()



