import subprocess

def run_jobs(jobs):
    print("\n Starting parallel Silver jobs...\n")

    processes = []

    # starting job
    for job in jobs:
        p = subprocess.Popen([
            "python","src/processing/spark_job.py",
            job["region"],
            str(job["load"]),
            str(job["cores"])
            ])
        processes.append(p)

    #wait for all jobs
    for process in processes:
        process.wait() 

    print("\n Both jobs finished\n")


def run_gold(jobs):

    print("\n Starting parallel Gold jobs...\n")

    processes = []

    # starting job
    for job in jobs:
        p = subprocess.Popen([
            "python","src/processing/spark_job.py",
            job["region"],
            str(job["load"]),
            str(job["cores"])
            ])
        processes.append(p)

    #wait for all jobs
    for process in processes:
        process.wait() 

    print("\n Both jobs finished\n")