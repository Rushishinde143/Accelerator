import re


def convert_spark_2x_to_3x(spark2x_code):
    # Replace SparkContext with SparkSession
    spark3x_code = spark2x_code.replace(
        "from pyspark import SparkContext",
        "from pyspark.sql import SparkSession\n\nspark = SparkSession.builder.appName("
        "'WordCount').getOrCreate()"
    )
    spark3x_code = spark3x_code.replace(
        "sc = SparkContext(\"local\", \"WordCount\")",
        ""
    )
    spark3x_code = spark3x_code.replace(
        "words = text_file.flatMap(lambda line: line.split(\" \"))",
        ""
    )
    spark3x_code = spark3x_code.replace(
        "word_counts = words.map(lambda word: (word, 1))",
        ""
    )

    # Replace text_file line
    spark3x_code = spark3x_code.replace(
        'text_file = sc.textFile("path_to_text_file.txt")',
        "text_file = spark.read.text('path_to_text_file.txt')"
    )

    # Replace reduceByKey with groupBy + agg
    spark3x_code = spark3x_code.replace(
        "word_counts = word_counts.reduceByKey(lambda a, b: a + b)",
        "word_counts = word_counts.groupBy('word').agg({'word': 'count'}).withColumnRenamed('count(word)', 'count')"
    )

    # Replace SparkContext stop with SparkSession stop
    spark3x_code = spark3x_code.replace(
        "sc.stop()",
        "spark.stop()"
    )

    return spark3x_code


def remove_comments(code):
    # Remove comments from the code
    code = re.sub(r'#.*\n', '\n', code)
    return code
