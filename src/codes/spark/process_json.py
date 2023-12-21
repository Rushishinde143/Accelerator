from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import TimestampType
import os
from datetime import datetime
import pyspark.sql.functions as F

file_counter = 0


def process_json_file(input_path, output_path, new_prefix, old_prefix):
    global file_counter
    # Initialize a Spark session
    spark = SparkSession.builder.appName("JsonProcessing").getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    for filename in os.listdir(input_path):
        if filename.endswith('.json'):
            input_paths = os.path.join(input_path, filename)

            # Read input JSON data
            input_data = spark.read.json(input_paths)

            # Cache the DataFrame before filtering out corrupt records
            input_data = input_data.cache()

            # Get all date columns dynamically
            # date_columns = [col_name for col_name, col_type in input_data.dtypes if col_type == 'date']
            date_columns = [col_name for col_name in input_data.columns if "date" in col_name.lower()]
            if len(date_columns) != 0:
                for date_col in date_columns:
                    # input_data = input_data.withColumn(date_col + '_add_months', F.expr(f"add_months({date_col}, 1)"))
                    input_data = input_data.withColumn(date_col + '_add_months',
                                                       F.date_format(F.add_months(input_data[date_col], 1),
                                                                     'yyyy-MM-dd'))

            # Add a current timestamp column
            input_data = input_data.withColumn("current_timestamp", current_timestamp().cast(TimestampType()))

            # Create the output file name with a timestamp
            timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
            output_file_name = f"{new_prefix}_{filename[len(old_prefix):-5]}_{timestamp_str}.json"

            # Write the result to the output folder as JSON
            output_json_path = os.path.join(output_path, f"{output_file_name}")
            input_data.write.mode("overwrite").json(output_json_path)

            # Unpersist the cached DataFrame
            input_data.unpersist()
            file_counter += 1

            # Append the file number to the processed file name
            processed_file_name = f"File {file_counter}: {filename}"
            processed_filename = f"File {file_counter}: {output_file_name}"
            print(f"Processed : {processed_file_name}")
            print(f"Stored: {processed_filename}")
            print()

        else:
            print(f"No date columns found in {filename}, skipping...")


    spark.stop()

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, current_timestamp
# from pyspark.sql.types import TimestampType
# import pyspark.sql.functions as F
# import os
# from datetime import datetime
#
#
# def process_json_file(input_path, output_path, new_prefix, old_prefix):
#     # Initialize a Spark session
#     spark = SparkSession.builder.appName("DynamicProcessing").getOrCreate()
#
#     # Read input JSON data
#     input_data = spark.read.json(input_path)
#
#     # Cache the DataFrame before filtering out corrupt records
#     input_data = input_data.cache()
#
#     # Get all date columns dynamically
#     date_columns = [col_name for col_name, col_type in input_data.dtypes if col_type == 'date']
#
#     if len(date_columns) != 0:
#         for date_col in date_columns:
#             input_data = input_data.withColumn(date_col + '_add_months', F.expr(f"add_months({date_col}, 1)"))
#
#     # Add a current timestamp column
#     input_data = input_data.withColumn("current_timestamp", current_timestamp().cast(TimestampType()))
#
#     # Extract the input file name without the extension
#     file_name_without_extension = os.path.splitext(os.path.basename(input_path))[0]
#
#     # Modify the file name to incorporate new_prefix and old_prefix
#     file_name_modified = f"{new_prefix}_{file_name_without_extension[len(old_prefix):]}"
#
#     # Create the output file name with a timestamp
#     timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
#     output_file_name = f"{file_name_modified}_{timestamp_str}.json"
#
#     # # Extract the input file name without the extension
#     # file_name = os.path.splitext(os.path.basename(input_path))[0]
#     #
#     # # Create the output file name with a timestamp
#     # timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
#     # output_file_name = f"{new_prefix}_{file_name[len(old_prefix):]}_{timestamp_str}.json"
#
#     # Write the result to the output folder as JSON
#     output_json_path = os.path.join(output_path, output_file_name)
#     input_data.write.mode("overwrite").json(output_json_path)
#
#     # Unpersist the cached DataFrame
#     input_data.unpersist()
#
#     # Stop the Spark session
#     spark.stop()
