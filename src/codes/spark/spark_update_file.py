import logging
import os
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

file_counter = 0


def process_csv(input_path, output_path, new_prefix, old_prefix, file_name):
    global file_counter
    spark = SparkSession.builder.appName("DynamicProcessing").getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    try:
        file_path = os.path.join(input_path, file_name)

        # Read the CSV file with automatic schema inference
        df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)

        # Identify date columns dynamically
        date_columns = [col_name for col_name, col_type in df.dtypes if col_type in ('timestamp', 'date')]

        if len(date_columns) != 0:
            for date_col in date_columns:
                df = df.withColumn(date_col + '_add_months', F.add_months(df[date_col], 1))

                # Add a new column with the current timestamp
                current_timestamp = F.current_timestamp()
                df_new = df.withColumn('current_timestamp', current_timestamp)

            timestamp_str = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

            new_filename = f"{new_prefix}_{file_name[len(old_prefix):-4]}_{timestamp_str}.csv"

            new_file_path = os.path.join(output_path, new_filename)

            # Write the DataFrame without header and overwrite mode
            df_new.write.option("header", True).csv(new_file_path)

            file_counter += 1

            # Append the file number to the processed file name
            processed_file_name = f"File {file_counter}: {file_name}"
            processed_filename = f"File {file_counter}: {new_filename}"
            print(f"Processed : {processed_file_name}")
            print(f"Stored: {processed_filename}")
            print()

        else:
            print(f"No date columns found in {file_name}, skipping...")

        # Stop the SparkSession
        spark.stop()

    except Exception as e:
        # Log the error
        logging.error(f"Error processing {file_name}: {str(e)}")





# import os
# from datetime import datetime
#
# from pyspark.sql import SparkSession
# import pyspark.sql.functions as F
#
# file_counter = 0
#
#
# def process_csv(input_path, output_path, new_prefix, old_prefix):
#     global file_counter
#     spark = SparkSession.builder.appName("DynamicProcessing").getOrCreate()
#     spark.sparkContext.setLogLevel("OFF")
#
#     # # Create the output directory if it doesn't exist
#     # if not os.path.exists(output_path):
#     #     os.makedirs(output_path)
#
#     for filename in os.listdir(input_path):
#         if filename.endswith('.csv'):
#             file_path = os.path.join(input_path, filename)
#
#             # Read the CSV file with automatic schema inference
#             df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)
#             # df.printSchema()
#
#             # Identify date columns dynamically
#             date_columns = [col_name for col_name, col_type in df.dtypes if col_type in ('timestamp', 'date')]
#
#             if len(date_columns) != 0:
#                 for date_col in date_columns:
#                     df = df.withColumn(date_col + '_add_months', F.add_months(df[date_col], 1))
#
#                     # Add a new column with the current timestamp
#                     current_timestamp = F.current_timestamp()
#                     df_new = df.withColumn('current_timestamp', current_timestamp)
#                     # df_new.show()
#
#                 timestamp_str = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
#
#                 new_filename = f"{new_prefix}_{filename[len(old_prefix):-4]}_{timestamp_str}.csv"
#
#                 new_file_path = os.path.join(output_path, new_filename)
#
#                 # Write the DataFrame without header and overwrite mode
#                 df_new.write.option("header", True).csv(new_file_path)
#
#                 file_counter += 1
#
#                 # Append the file number to the processed file name
#                 processed_file_name = f"File {file_counter}: {filename}"
#                 processed_filename = f"File {file_counter}: {new_filename}"
#                 print(f"Processed : {processed_file_name}")
#                 print(f"Stored: {processed_filename}")
#                 print()
#
#             else:
#                 print(f"No date columns found in {filename}, skipping...")
#
#             # remove item
#             # os.remove(filename)
#     # Stop the SparkSession
#     spark.stop()
