from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


def execute(snowflake_credentials):

    print(">>>> config")
    input = "data/input/t_fdev_athletes"
    # output = "/data/output"
    otputTable = "data_t_fdev_athletes"

    print(">>>> session")
    spark = SparkSession.builder \
        .appName("move data into snowflake") \
        .config('spark.jars', "jars/snowflake-jdbc-3.13.24.jar,jars/spark-snowflake_2.12-2.11.1-spark_3.3.jar") \
        .getOrCreate()

    dfInput = spark.read.parquet(input)
    dfInput.show()

    print(">>>> transform")
    dfOutput = dfInput.withColumn("newCol", lit(1))

    print(">>>> output")
    # dfOutput.write.mode("overwrite").parquet(output)

    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

    dfOutput.write.format(SNOWFLAKE_SOURCE_NAME)\
        .options(**snowflake_credentials) \
        .option("dbtable", otputTable) \
        .saveAsTable(otputTable)


    df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
        .options(**snowflake_credentials) \
        .option("query", "select * from "+otputTable+" limit 10") \
        .load()
    df.show()

