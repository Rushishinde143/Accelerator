from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row

sc = SparkContext("local", "TimestampExtractionExample")

sqlContext = SQLContext(sc)

data = [("2019-09-20 10:10:10.1",)]
rdd = sc.parallelize(data).map(lambda x: Row(timestamp=x[0]))

df = sqlContext.createDataFrame(rdd)

df = df.selectExpr("second(to_timestamp(timestamp, 'yyyy-MM-dd HH:mm:ss.S')) as seconds")

df.show()

sc.stop()
