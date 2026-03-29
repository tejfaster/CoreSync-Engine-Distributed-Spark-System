import subprocess


def run_jobs(jobs):
    print("\n Starting parallel jobs...\n")

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
