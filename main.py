import time
from src.controller.load_manager import allocate_cores, generate_workload
from src.pipeline.run_parallel import run_jobs


# 15 minutes
INTERVAL = 10

def main():
    while True:
        print("\n Running pipeline...\n")

        usa, eu = generate_workload()
        usa_cores,eu_cores = allocate_cores(usa,eu)

        # creating job
        jobs = [
            {"region":"usa","load":usa,"cores":usa_cores},
            {"region":"eu","load":eu,"cores":eu_cores}
        ]
        run_jobs(jobs)

        time.sleep(INTERVAL)



if __name__ == "__main__":
    main()



