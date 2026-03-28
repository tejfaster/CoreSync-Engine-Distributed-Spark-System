from src.pipeline.spark_job import start_spark
from multiprocessing import Queue

def start_job(job,region,queue: Queue):
    print("-"*75)
    print(f"Starting {region} pipeline....")

    spark = start_spark(job,region)
    
    # rdd
    # partition = job * 2
    # rdd = spark.sparkContext.parallelize(data,partition)
    # result = rdd.map(lambda f:(f"{region}",f[0])).count()

    while True:
        try:
            data = queue.get()

            if data == "STOP":
                break

            if not data:
                continue

            # dataframe
            size = len(data)
            partitions = min(50,max(job * 2,size // 200))
            df = spark.createDataFrame(data).repartition(partitions)
            print(f"{region} flight processed: {df.count()}")
            print("-"*75)
        except Exception as e:
            print(f"[ERROR] {region} worker faild: {e}")

    spark.stop()
    print(f"{region} worker stopped.")








