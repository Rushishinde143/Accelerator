from pyspark import SparkContext
from pyspark.sql import SQLContext



sc = SparkContext("local", "PercentileApproxExample")
sqlContext = SQLContext(sc)


data = [(1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E")]
df = sqlContext.createDataFrame(data, ["value", "category"])


percentiles = [0.25, 0.5, 0.75]
percentile_values = df.select("value").approxQuantile("value", percentiles, 0.1)


for p, value in zip(percentiles, percentile_values):
    print(f"{p}-th percentile: {value}")


sc.stop()