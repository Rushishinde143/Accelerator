import re


def trim_convert_spark_2x_to_3x(spark2x_code):
    spark3x_code = spark2x_code.replace(
        "from pyspark.sql.functions import col, trim",
        "from pyspark.sql.functions import rtrim"
    )
    spark3x_code = spark3x_code.replace(
        "from pyspark import SparkContext",
        "from pyspark.sql import SparkSession\n\nspark = SparkSession.builder.appName("
        "'TrimExample').getOrCreate()"
    )
    spark3x_code = spark3x_code.replace(
        "sc = SparkContext(\"local\", \"TrimExample\")",
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
        "df = sqlContext.createDataFrame(data, columns)",
        "df = spark.createDataFrame(data, columns)"
    )

    spark3x_code = spark3x_code.replace(
        "return trim(col(col_name)).alias(col_name)",
        "return rtrim(col_name).alias(col_name)"
    )
    spark3x_code = spark3x_code.replace(
        "sc.stop()",
        "spark.stop()"
    )

    return spark3x_code


def remove_comments(code):
    # Remove comments from the code
    code = re.sub(r'#.*\n', '\n', code)
    return code
