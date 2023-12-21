from pyspark import SparkContext
from pyspark.sql import SQLContext

# Initialize SparkContext
sc = SparkContext("local", "UnionAllExample")

# Initialize SQLContext
sqlContext = SQLContext(sc)

# Create two DataFrames
df1 = sqlContext.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "value"])
df2 = sqlContext.createDataFrame([(4, "D"), (5, "E"), (6, "F")], ["id", "value"])

# Perform a unionAll operation
unioned_df = df1.unionAll(df2)

# Show the result
unioned_df.show()

# Stop the SparkContext
sc.stop()
