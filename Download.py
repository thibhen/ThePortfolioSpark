from pyspark.sql import SparkSession
from pyspark.sql.functions import col,desc, from_csv,lag,format_string,lit, spark_partition_id,split,reverse
from pyspark.sql.functions import input_file_name
from pyspark.sql.types import StructType,StructField, StringType, DateType, LongType, DoubleType
from pyspark import SparkFiles
from pyspark import StorageLevel
import csv
import requests
import tempfile
import logging
import os
import multiprocessing 


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


"""

"""
def load(code):
    logger.info("============== Processing %s",code)
    url = "https://query1.finance.yahoo.com/v7/finance/download/{}?period1=0&period2=2018000000&interval=1d&events=history".format(code)
    logger.info("Downloading {}".format(url))
    response = requests.get( url )
    decoded_content = response.content.decode('utf-8')

    result = [ (code,*row.split(",")) for row in decoded_content.splitlines() ]

    return result


"""
    Load in Spark
"""
def download(showOutput = False):
    """
    Read All position(s) (unique) and load it in a set
    """
    securities = list() #set()
    with open('in/trades.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                line_count += 1
                securities.append(row[0])

    #load EUR/USD
    securities.append("eurusd=x")
    
    logger.info("Downloading: {}".format(securities))
    
    #securities = ["ACN"]     
    result = [ x for s in securities for x in load(s)]

  
    """
    processes = set()
    for code in securities:
        p = multiprocessing.Process(target=load, args=(code,spark,showOutput))
        p.start()
        processes.add(p)
    
    for p in processes:
        p.join()
    """

    logger.info("Creating Spark Context...")
    
    spark = SparkSession \
        .builder \
        .appName(__name__) \
        .getOrCreate()

    spark.sparkContext.setJobGroup(__name__, "Load Market data (Yahoo)")
    
    # Date,Open,High,Low,Close,Adj Close,Volume
    schema = StructType([       
    StructField('ID', StringType(), True),
    StructField('Date', StringType(), True),
    StructField('Open', StringType(), True),
    StructField('High', StringType(), True),
    StructField('Low', StringType(), True),
    StructField('Close', StringType(), True),
    StructField('Adj Close', StringType(), True),
    StructField('Volume', StringType(), True)
    ])

    df = spark.createDataFrame(data=result[1:], schema = schema) \
        .withColumn("Volume",col("Volume").cast("long")) \
        .withColumn("Date",col("Date").cast("date")) \
        .withColumn("Close",col("Close").cast("double")) \
        .withColumn("Low",col("Low").cast("double")) \
        .withColumn("High",col("High").cast("double")) \
        .withColumn("Open",col("Open").cast("double")) \
        .withColumn("Adj Close",col("Adj Close").cast("double")) \
        .orderBy(desc("Date")) \
        .filter("Date > date'2019-01-01'") \
        .drop("Adj Close")

    if showOutput :
        df.show()


    """        
    for security in securities:
        load(security)

        securities.map(load(showOutput = showOutput))

    """

    
if __name__ == "__main__":
    download(showOutput = True)