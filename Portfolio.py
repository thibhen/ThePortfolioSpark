from logging import setLoggerClass
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col,desc,asc,lag,lead,format_string,lit,split,reverse,when,row_number,bround
from pyspark.sql.types import StructType,StructField, StringType,DateType
from pyspark.sql.functions import input_file_name

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def portfolio(showOutput = False):


    """

    """
    logger.info("Create SparkSession")
    spark = SparkSession \
        .builder \
        .appName(__name__) \
        .getOrCreate()


    """
    +-------+----------+----------+
    |     ID|      Date|     Close|
    +-------+----------+----------+
    |  VK.PA|2020-11-27|    30.805|
    | RNO.PA|2020-11-27| 34.115002|
    |  AI.PA|2020-11-27|138.100006|
    | LVC.PA|2020-11-27|     19.99|
    |    ACN|2020-11-27|250.119995|
    |SESG.PA|2020-11-27|       7.9|
    | LQQ.PA|2020-11-27|     522.0|
    |  LI.PA|2020-11-27| 19.379999|
    |SESG.PA|2020-11-26|     7.948|
    """

    logger.info("Load Clean Market Data")
    spark.sparkContext.setJobGroup(__name__, "Load Clean Market Data from DISK")
    marketData = spark.read.parquet("in/market-data-all-clean-parquet")
    if showOutput:
        marketData.show()

    """
        Load EUR/USD
    """

    logger.info("Load EUR/USD change")
    spark.sparkContext.setJobGroup(__name__, "Load EUR/USD change")
    fx = spark.read.parquet("in/market-data-parquet/eurusd=x") \
        .select("Date","Close") \
        .withColumnRenamed('Close', 'EUR_USD') \
        .withColumnRenamed('Date', 'fxDate')
        
    if showOutput:
        fx.show()

    """
        Load the CSV, cas each column in appropriate type
        Group by ID

    +-------+------+
    |     ID|Amount|
    +-------+------+
    |SESG.PA|   500|
    |  AI.PA|   199|
    |    ACN|   323|
    |  VK.PA|   120|


    """

    logger.info("Load all trades")
    spark.sparkContext.setJobGroup(__name__, "Load all trades")
    trades = spark.read.format("csv").option("header", "true").load("in/trades.csv") \
        .withColumn("Amount",col("Amount").cast("long")) \
        .withColumn("OpenDate",col("OpenDate").cast("date")) 


    if showOutput:
        trades.show()

    logger.info("Group all positions")
    spark.sparkContext.setJobGroup(__name__, "Group all positions")
    portfolio = trades.groupBy("ID","Currency") \
        .sum("Amount") \
        .withColumnRenamed('sum(Amount)', 'Amount') \
        .withColumnRenamed('ID', 'ID_PTF')
        
        
    if showOutput:
        portfolio.show()



    """
        Let's join the 2 tables and get historical valo by pos

    +-----+----------+-------+-----+------+-----------------+
    |   ID|      Date|  Close|   ID|Amount|             Valo|
    +-----+----------+-------+-----+------+-----------------+
    |LI.PA|1992-07-09|11.4997|LI.PA|   500|          5749.85|
    |LI.PA|1992-07-10|11.3533|LI.PA|   500|5676.650000000001|
    |LI.PA|1992-07-13|   null|LI.PA|   500|             null|
    |LI.PA|1992-07-14|   null|LI.PA|   500|             null|
    |LI.PA|1992-07-15|11.4021|LI.PA|   500|          5701.05|
    |LI.PA|1992-07-16|11.9063|LI.PA|   500|          5953.15|


    """
    logger.info("Join the 2 tables and get historical valo by pos")
    spark.sparkContext.setJobGroup(__name__, "Join the 2 tables and get historical valo by pos")
    
    position_history = marketData.join(portfolio, portfolio.ID_PTF == marketData.ID) \
        .join(fx,fx.fxDate == marketData.Date) \
        .withColumn("Valo", when(col("Currency") == "EUR", col("Amount")*col("Close")).otherwise(col("Amount")*col("Close")/col("EUR_USD"))) \
        .withColumn("Valo", bround(col("Valo"),2)) \
        .orderBy(desc("Date"))

    if showOutput:
        position_history.show()



    """"
        Let's group all pos

    +----------+------+
    |      Date|  Valo|
    +----------+------+
    |2020-11-27|xxxxxx|
    |2020-11-26|xxxxxx|
    |2020-11-25|xxxxxx|
    |2020-11-24|xxxxxx|
    |2020-11-23|xxxxxx|
    |2020-11-20|xxxxxx|
    |2020-11-19|xxxxxx|

    """
    logger.info("Let's group all pos to see portfolio evolution")
    spark.sparkContext.setJobGroup(__name__, "group all pos to see portfolio evolution")
    
    valo = position_history.groupBy("Date") \
        .sum("Valo") \
        .withColumnRenamed('sum(Valo)', 'Valo') \
        .withColumn("Valo", bround(col("Valo"),2)) \
        .orderBy(desc("Date"))


    if showOutput:
        valo.show()


    """
        Show Valo per ID

    valo_per_id = position_history \
        .orderBy(desc("Date"),desc("Valo")) \
        .withColumn("Valo",col("Valo").cast("long")) \
        .limit(8)

    valo_per_id.show()
    valo_per_id.repartition(1).write.mode('overwrite').csv('out/valo_per_id.csv')
    """

    logger.info("Show the latest valorization per ID")

    spark.sparkContext.setJobGroup(__name__, "Show the latest valorization per ID")
    valo_per_id = position_history \
        .withColumn("rn", row_number() \
            .over(Window.partitionBy("ID") \
            .orderBy(col("Date").desc()))) \
        .filter(col("rn") == 1) \
        .drop("rn","fxDate","ID_PTF","EUR_USD") \
        .orderBy(desc("Valo")) 
        
    if showOutput:
        valo_per_id.show()
    spark.sparkContext.setJobGroup(__name__, "valo_per_id : Store result on disk ")
    valo_per_id.repartition(1).write.mode('overwrite').csv('out/valo_per_id.csv')


    """
        Include Previous Value
    """
    logger.info("Calculate historical evolution of the portfolio")

    spark.sparkContext.setJobGroup(__name__, "Calculate historical evolution of the portfolio")
    f = valo.withColumn("Prev", \
        lag(valo.Valo).over( \
            Window.partitionBy().orderBy("Date"))) \
        .withColumn("Diff",bround(col("Valo") - col("Prev"),2)) \
        .withColumn("Evol",bround(100*col("Diff") / col("Prev"),2)) \
        .drop("Prev") \
        .orderBy(desc("Date"))

    if showOutput:
        f.show()
    
    spark.sparkContext.setJobGroup(__name__, "valo_history : store result on disk")
    f.repartition(1).write.mode('overwrite').csv('out/valo_history.csv')


if __name__ == "__main__":
    portfolio(showOutput = True)