import re

def union_convert_spark2x_to_spark3x(spark2x_code):
    # Remove comments from the code
    #spark2x_code = re.sub(r'#.*\n', '\n', spark2x_code)

    # Replace relevant parts of the code
    spark3x_code = spark2x_code.replace(
        "from pyspark import SparkContext",
        "from pyspark.sql import SparkSession"
    )
    spark3x_code = spark3x_code.replace(
        "from pyspark.sql import SQLContext",
        ""
    )
    spark3x_code = spark3x_code.replace(
        "sc = SparkContext(\"local\", \"UnionAllExample\")",
        "spark = SparkSession.builder.appName(\"UnionAllExample\").getOrCreate()"
    )

    spark3x_code = spark3x_code.replace(
        "sqlContext = SQLContext(sc)",
        "data1 = [(1, \"A\"), (2, \"B\"), (3, \"C\")]\n\ndata2 = [(4, \"D\"), (5, \"E\"), (6, \"F\")]\n\ncolumns = ["
        "\"id\", \"value\"]"
    )


    # Update the DataFrame creation
    spark3x_code = spark3x_code.replace(
        "df1 = sqlContext.createDataFrame([(1, \"A\"), (2, \"B\"), (3, \"C\")], [\"id\", \"value\"])",
        "df1 = spark.createDataFrame(data1, columns)"
    )
    spark3x_code = spark3x_code.replace(
        "df2 = sqlContext.createDataFrame([(4, \"D\"), (5, \"E\"), (6, \"F\")], [\"id\", \"value\"])",
        "df2 = spark.createDataFrame(data2, columns)"
    )
    spark3x_code = spark3x_code.replace(
        "unioned_df = df1.unionAll(df2)",
        "unioned_df = df1.union(df2)"
    )

    spark3x_code = spark3x_code.replace(
        "sc.stop()",
        "spark.stop()"
    )

    return spark3x_code

