from pyspark.sql.functions import col, trim
from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext("local", "TrimExample")

sqlContext = SQLContext(sc)

data = [("   John  ", 25), ("   Alice  ", 30), ("   Bob  ", 28)]
columns = ["name", "age"]

df = sqlContext.createDataFrame(data, columns)

def custom_trim(col_name):
    return trim(col(col_name)).alias(col_name)


trimmed_df = df.withColumn("name", custom_trim("name"))

trimmed_df.show()

sc.stop()
