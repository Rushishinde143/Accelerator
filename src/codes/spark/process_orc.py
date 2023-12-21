from pyspark.sql import SparkSession
from pyspark.sql.functions import col, add_months, current_timestamp
from pyspark.sql.types import TimestampType
import os
from datetime import datetime

file_counter = 0


def process_orc_file(input_path, output_path, new_prefix, old_prefix):
    global file_counter

    # Initialize a Spark session
    spark = SparkSession.builder.appName("OrcProcessing").getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    for filename in os.listdir(input_path):
        if filename.endswith('.orc'):
            file_path = os.path.join(input_path, filename)

            # Read input ORC data
            input_data = spark.read.orc(file_path)

            # Cache the DataFrame before any transformations
            input_data = input_data.cache()

            # Get all date columns dynamically
            date_columns = [col_name for col_name, col_type in input_data.dtypes if col_type == 'date']

            if len(date_columns) != 0:
                for date_col in date_columns:
                    input_data = input_data.withColumn(date_col + '_add_months', add_months(col(date_col), 1))

            # Add a current timestamp column
            input_data = input_data.withColumn("current_timestamp", current_timestamp().cast(TimestampType()))

            # Create the output file name with a timestamp
            timestamp_str = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            output_file_name = f"{new_prefix}_{filename[len(old_prefix):-4]}_{timestamp_str}.orc"

            # Write the result to the output folder as ORC
            output_orc_path = os.path.join(output_path, f"{output_file_name}")
            input_data.write.mode("overwrite").orc(output_orc_path)

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
            print(f"Skipping file {filename} as the format is not supported.")

    # Stop the Spark session
    spark.stop()












# 2nd one --------------------------------------------------------------------

# import os
# import logging
# from datetime import datetime
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, add_months, current_timestamp
# from pyspark.sql.types import TimestampType
#
# file_counter = 0
# spark = SparkSession.builder.appName("OrcProcessing").getOrCreate()
# spark.sparkContext.setLogLevel("OFF")
#
#
# def process_orc_file(input_path, output_path, new_prefix, old_prefix):
#     global file_counter, filename
#
#     try:
#         for filename in os.listdir(input_path):
#             if filename.endswith('.orc'):
#                 file_path = os.path.join(input_path, filename)
#
#                 # Read input ORC data
#                 input_data = spark.read.orc(file_path)
#
#                 # Cache the DataFrame before any transformations
#                 input_data = input_data.cache()
#
#                 # Get all date columns dynamically
#                 date_columns = [col_name for col_name, col_type in input_data.dtypes if col_type == 'date']
#
#                 if len(date_columns) != 0:
#                     for date_col in date_columns:
#                         input_data = input_data.withColumn(date_col + '_add_months', add_months(col(date_col), 1))
#
#                 # Add a current timestamp column
#                 input_data = input_data.withColumn("current_timestamp", current_timestamp().cast(TimestampType()))
#
#                 # Create the output file name with a timestamp
#                 timestamp_str = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
#                 output_file_name = f"{new_prefix}_{filename[len(old_prefix):-4]}_{timestamp_str}.orc"
#
#                 # Write the result to the output folder as ORC
#                 output_orc_path = os.path.join(output_path, output_file_name)
#                 input_data.write.mode("overwrite").orc(output_orc_path)
#
#                 # Unpersist the cached DataFrame
#                 input_data.unpersist()
#
#                 file_counter += 1
#                 # Append the file number to the processed file name
#                 processed_file_name = f"File {file_counter}: {filename}"
#                 processed_filename = f"File {file_counter}: {output_file_name}"
#                 print(f"Processed : {processed_file_name}")
#                 print(f"Stored: {processed_filename}")
#                 print()
#
#             else:
#                 print(f"Skipping file {filename} as the format is not supported.")
#
#     except Exception as e:
#         # Log the error
#         logging.error(f"Error processing {filename}: {str(e)}")
#
#     finally:
#         # Stop the Spark session
#         spark.stop()


# 3rd-------------------------------------

# import logging
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, add_months, current_timestamp
# from pyspark.sql.types import TimestampType
# import os
# from datetime import datetime
#
# file_counter = 0
# processed_files = set()  # Keep track of processed files
#
# # Configure logging
# logging.basicConfig(filename='error.log', level=logging.ERROR)
#
#
# def process_orc_file(input_path, output_path, new_prefix, old_prefix):
#     global file_counter
#
#     # Initialize a Spark session
#     spark = SparkSession.builder.appName("OrcProcessing").getOrCreate()
#     spark.sparkContext.setLogLevel("OFF")
#
#     for filename in os.listdir(input_path):
#         try:
#             if filename.endswith('.orc') and filename not in processed_files:
#                 file_path = os.path.join(input_path, filename)
#
#                 # Read input ORC data
#                 input_data = spark.read.orc(file_path)
#
#                 # Cache the DataFrame before any transformations
#                 input_data = input_data.cache()
#
#                 # Get all date columns dynamically
#                 date_columns = [col_name for col_name, col_type in input_data.dtypes if col_type == 'date']
#
#                 if len(date_columns) != 0:
#                     for date_col in date_columns:
#                         input_data = input_data.withColumn(date_col + '_add_months', add_months(col(date_col), 1))
#
#                 # Add a current timestamp column
#                 input_data = input_data.withColumn("current_timestamp", current_timestamp().cast(TimestampType()))
#
#                 # Create the output file name with a timestamp
#                 timestamp_str = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
#                 output_file_name = f"{new_prefix}_{filename[len(old_prefix):-4]}_{timestamp_str}"
#
#                 # Check if the output file already exists
#                 output_orc_path = os.path.join(output_path, f"{output_file_name}.orc")
#                 if not os.path.exists(output_orc_path):
#                     # Write the result to the output folder as ORC
#                     input_data.write.mode("overwrite").orc(output_orc_path)
#
#                     # Unpersist the cached DataFrame
#                     input_data.unpersist()
#
#                     file_counter += 1
#                     # Append the file number to the processed file name
#                     processed_file_name = f"File {file_counter}: {filename}"
#                     processed_filename = f"File {file_counter}: {output_file_name}"
#                     print(f"Processed : {processed_file_name}")
#                     print(f"Stored: {processed_filename}")
#                     print()
#
#                     # Add the processed filename to the set
#                     processed_files.add(filename)
#                 else:
#                     print(f"Output file {output_file_name}.orc already exists. Skipping.")
#
#             else:
#                 print(f"Skipping file {filename} as the format is not supported.")
#         except Exception as e:
#             # Log the exception
#             logging.error(f"Error processing file {filename}: {str(e)}")
#
#     # Stop the Spark session
#     spark.stop()
