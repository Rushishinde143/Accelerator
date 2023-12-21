import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

file_counter = 0


class SparkSession:
    pass


def process_avro_file(input_path, output_path, new_prefix, old_prefix):
    global file_counter, filename

    spark = SparkSession.builder.appName("AvroProcessing").config("spark.jars.packages",
                                                                  "org.apache.spark:spark-avro_2.12:3.4.1").getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    try:
        for filename in os.listdir(input_path):
            if filename.endswith('.avro'):
                file_path = os.path.join(input_path, filename)

                df = spark.read.format("avro").load(file_path)

                #date_columns = [col_name for col_name in df.columns if "date" in col_name.lower()]
                date_columns = [col_name for col_name, col_type in df.dtypes if col_type in ('timestamp', 'date')]

                if len(date_columns) != 0:
                    for date_col in date_columns:
                        df1 = df.withColumn(date_col, F.date_format(df[date_col], "yyyy-MM-dd"))
                        df2 = df1.withColumn(date_col + '_add_months',
                                             F.date_format(F.add_months(F.col(date_col), 1), 'yyyy-MM-dd'))
                        current_timestamp = F.current_timestamp()
                        df2 = df2.withColumn('current_timestamp', current_timestamp)

                        timestamp_str = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

                        new_filename = f"{new_prefix}_{filename[len(old_prefix):-5]}_{timestamp_str}.avro"

                        new_file_path = os.path.join(output_path, new_filename)

                        df2.write.mode("overwrite").format("avro").save(new_file_path)

                        file_counter += 1

                        processed_file_name = f"File {file_counter}: {filename}"
                        processed_filename = f"File {file_counter}: {new_filename}"
                        print(f"Processed : {processed_file_name}")
                        print(f"Stored: {processed_filename}")
                        print()

                else:
                    print(f"No date columns found in {filename}, skipping...")

        spark.stop()

    except Exception as e:
        logging.error(f"Error processing {filename}: {str(e)}")





# -------------------------------------------------------------






# from pyspark.sql import SparkSession
# import pyspark.sql.functions as F
#
# spark = SparkSession.builder.appName("CSV to Avro and Parquet") \
#     .paths("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.4.1") \
#     .getOrCreate()
# spark.sparkContext.setLogLevel("OFF")
#
# csv_folder_path = "C:/Users/lyekollu/PycharmProject_Updated/Accelerator/Media/user_input/"
#
# output_folder = "C:/Users/lyekollu/PycharmProject_Updated/Accelerator/Media/destination_folder/new_empdata.avro"
#
# df = spark.read.format("avro").load(csv_folder_path)
#
# # Add 1 month to the 'dateofjoining' column and format the result as a date
# df = df.withColumn('dateofjoining_add_months', F.date_format(F.add_months(df['dateofjoining'], 1), 'yyyy-MM-dd'))
#
# df1 = df.withColumn("current_timestamp", F.current_timestamp())
#
# df1.write.mode("overwrite").format("avro").save(output_folder)
# df1.show()