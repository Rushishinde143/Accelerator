import re


def extract_convert_spark2x_to_spark3x(spark2x_code):
    # Remove comments from the code
    # spark2x_code = re.sub(r'#.*\n', '\n', spark2x_code)

    # Replace relevant parts of the code
    spark3x_code = spark2x_code.replace(
        "from pyspark import SparkContext",
        "from pyspark.sql import SparkSession\n\nfrom pyspark.sql.functions import expr\n\nspark = SparkSession.builder.appName(\"TimestampExtractionExample\").getOrCreate()"
    )

    # spark3x_code = spark3x_code.replace(
    #     "from pyspark.sql import row",
    #     ""
    # )
    spark3x_code = re.sub(r"from pyspark\.sql import\s+Row", "", spark3x_code)
    spark3x_code = spark3x_code.replace(
        "sc = SparkContext(\"local\", \"TimestampExtractionExample\")",
        ""
    )

    spark3x_code = spark3x_code.replace(
        "from pyspark.sql import SQLContext",
        ""
    )
    spark3x_code = spark3x_code.replace(
        "sqlContext = SQLContext(sc)",
        ""
    )
    spark3x_code = spark3x_code.replace(
        "sc.stop()",
        "spark.stop()"
    )
    spark3x_code = spark3x_code.replace(
        "rdd = sc.parallelize(data).map(lambda x: Row(timestamp=x[0]))",
        " "
    )

    # Update the DataFrame creation
    spark3x_code = spark3x_code.replace(
        "df = sqlContext.createDataFrame(rdd)",
        "df = spark.createDataFrame(data, [\"timestamp\"])",
    )

    # Update the timestamp extraction part using replace
    spark3x_code = spark3x_code.replace(
        "df = df.selectExpr(\"second(to_timestamp(timestamp, 'yyyy-MM-dd HH:mm:ss.S')) as seconds\")",
        # "df = df.selectExpr(\"second(to_timestamp('timestamp', 'yyyy-MM-dd HH:mm:ss.S')) as seconds\")",
        "df = df.select(expr(\"extract(second from to_timestamp(timestamp, 'yyyy-MM-dd HH:mm:ss.S')) as seconds\"))"
    )
    #spark3x_code += "\n"

    return spark3x_code
