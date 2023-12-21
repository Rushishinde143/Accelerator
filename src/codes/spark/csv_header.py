from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("CSVSchemaExample").getOrCreate()
spark.sparkContext.setLogLevel("OFF")

file_path ="C:/Users/PycharmProjects/Accelerator/Media/user_input/"

df = spark.read.option("header",True ).option("inferSchema", True).csv(file_path)
print("The input csv files are:")
df.show()
print("The data type of columns is:")
df.printSchema()
