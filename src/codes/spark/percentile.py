import re

def percent_convert_spark2x_to_spark3x(spark2x_code):
    # Remove comments from the code
    # spark2x_code = re.sub(r'#.*\n', '\n', spark2x_code)

    # Replace relevant parts of the code
    spark3x_code = spark2x_code.replace(
        "from pyspark import SparkContext",
        "from pyspark.sql import SparkSession"
    )
    spark3x_code = spark3x_code.replace(
        "from pyspark.sql import SQLContext",
        "from pyspark.sql.functions import expr"
    )

    spark3x_code = spark3x_code.replace(
        "sc = SparkContext(\"local\", \"PercentileApproxExample\")",
        "spark = SparkSession.builder.appName(\"PercentileApproxExample\").getOrCreate()"
    )

    spark3x_code = spark3x_code.replace(
        "sqlContext = SQLContext(sc)",
        ""
    )
    spark3x_code = spark3x_code.replace(
        "sc.stop()",
        "spark.stop()"
    )

    # Update the DataFrame creation
    spark3x_code = spark3x_code.replace(
        "df = sqlContext.createDataFrame(data, [\"value\", \"category\"])",
        "df = spark.createDataFrame(data, [\"value\", \"category\"])",
    )

    # Update the percentile calculation using replace
    spark3x_code = spark3x_code.replace(
        "percentile_values = df.select(\"value\").approxQuantile(\"value\", percentiles, 0.1)",
        "percentile_values = df.select(*[expr(f\"percentile_approx(value, {{p}}) as {{p * 100}}%_percentile\") for p in percentiles])"
    )

    # Display the results using a loop
    spark3x_code = spark3x_code.replace(
        "for p, value in zip(percentiles, percentile_values):",
        "for p, value in zip(percentiles, percentile_values.collect()[0]):"
    )

    return spark3x_code


