from src.pipeline.spark_job import start_spark

def start_job(job,region,data):
    print("-"*75)
    print(f"Starting {region} pipeline....")

    spark = start_spark(job,region)
    
    # rdd
    # partition = job * 2
    # rdd = spark.sparkContext.parallelize(data,partition)
    # result = rdd.map(lambda f:(f"{region}",f[0])).count()

    # dataframe
    df = spark.createDataFrame(data)
    print(f"{region} flight processed: {df.count()}")
    print("-"*75)
    spark.stop()








