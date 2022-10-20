from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit

def main():
    print(">>>> config")
    input = "/data/input/t_fdev_athletes"
    output = "/data/output"

    print(">>>> session")
    spark = SparkSession.builder\
    .appName("example")\
    .getOrCreate()

    print(">>>> config")
    dfInput = spark.read.parquet(input)
    dfInput.show()

    print(">>>> transform")
    dfOutput = dfInput.withColumn("newCol", lit(1))

    print(">>>> output")
    dfOutput.write.mode("overwrite").parquet(output)



if __name__ == "__main__":
    main()