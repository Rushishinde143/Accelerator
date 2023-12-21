from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import avg
from pyspark.sql import DataFrame

conf = SparkConf().setAppName("GroupBy_Example")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

data = [("James", "Sales", "NY", 90000, 34, 10000),
        ("Michael", "Sales", "NY", 86000, 56, 20000),
        ("Robert", "Sales", "CA", 81000, 30, 23000),
        ("Maria", "Finance", "CA", 90000, 24, 23000),
        ("Raman", "Finance", "CA", 99000, 40, 24000),
        ("Scott", "Finance", "NY", 83000, 36, 19000),
        ("Jen", "Finance", "NY", 79000, 53, 15000),
        ("Jeff", "Marketing", "CA", 80000, 25, 18000),
        ("Kumar", "Marketing", "NY", 91000, 50, 21000)
        ]

schema = ["employee_name", "department", "state", "salary", "age", "bonus"]

df = sqlContext.createDataFrame(data, schema)
result = df.groupBy("department").agg({'salary': 'sum', 'age': 'avg'})
result.show()
sc.stop()
