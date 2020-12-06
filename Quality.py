from logging import setLoggerClass
from py4j.java_gateway import CallbackConnection
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col,desc,asc,lag,lead,format_string,lit,split,reverse,when,row_number,bround,explode,date_format,udf
from pyspark.sql.types import StructType,StructField, StringType,DateType
from pyspark.sql.functions import input_file_name
from datetime import datetime

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
   

def quality(showOutput = False):
    """

    """

    logger.info("Creating Spark context")

    spark = SparkSession \
        .builder \
        .appName(__name__) \
        .getOrCreate()


    logger.info("Load Market Data")
    spark.sparkContext.setJobGroup(__name__, "Load Market Data from disk")
    
    marketData = spark.read.parquet("in/market-data-parquet/*") \
        .withColumn("PATH", input_file_name()) \
        .withColumn("PATH_SPLIT", split("PATH","/")) \
        .withColumn("ID", reverse("PATH_SPLIT").getItem(1)) \
        .select("ID","Date","Close")

    marketData.collect()

    if showOutput : 
        marketData.show()

    # Handle missing Prices.

    from pyspark.sql.functions import dayofweek

    logger.info("Create a Calendar (without week ends)")
    spark.sparkContext.setJobGroup(__name__, "Create a Calendar (without week ends)")
    calandar = spark.sql("SELECT sequence(to_date('2019-01-01'), to_date(now()), interval 1 day) as Date") \
        .withColumn("Date", explode(col("Date"))) \
        .withColumn('weekDay', dayofweek(col("Date"))) \
        .filter(col("weekDay")>1) \
        .filter(col("weekDay")<7 ) \
        .drop("weekDay") \
        .orderBy(desc("Date"))
    
    calandar.collect()

    logger.info("Identify all distinct securities")
    codes = marketData.select(col("ID")).distinct()
    
    codes.collect()

    logger.info("Outer Join Sec ID with Calendar, and join with Market data")
    spark.sparkContext.setJobGroup(__name__, "Outer Join Sec ID with Calendar, and join with Market data")
    all = calandar.join(codes) \
        .repartition(col("ID"))  \
        .join(marketData, ['Date','ID'],'outer') \
        .orderBy(desc("Date"))
    
    all.collect()

    if showOutput : 
        all.show()

    logger.info("Complement missing values (e.g. market closed)")
    spark.sparkContext.setJobGroup(__name__, "Complement missing values (e.g. market closed)")
    all = all.withColumn("Prev", \
        lag(marketData.Close).over( \
            Window.partitionBy("ID").orderBy("Date"))) \
        .withColumn("Close", when(col("Close").isNull(), col("Prev")).otherwise(col("Close"))) \
        .drop("Prev") \
        .orderBy(desc("Date"))

    all.collect()


    """
    # DEBUG
    all.filter(col("ID")=='ACN').show()
    """

    logger.info("Store in a clean data file")
    spark.sparkContext.setJobGroup(__name__, "Store in a clean data file")
    all.repartition(1) \
        .write \
        .mode('overwrite') \
        .parquet('in/market-data-all-clean-parquet')


if __name__ == "__main__":
    quality(showOutput = True)