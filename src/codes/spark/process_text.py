import logging
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

file_counter = 0


def process_text_file(input_path, output_path, new_prefix, old_prefix):
    global file_counter
    # Create a SparkSession
    spark = SparkSession.builder.appName("DynamicProcessing").getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    for filename in os.listdir(input_path):
        if filename.endswith('.txt'):
            try:
                file_path = os.path.join(input_path, filename)

                # Read the TXT file with appropriate delimiter (e.g., tab)
                df = spark.read.option("delimiter", "\t").csv(file_path, header=True, inferSchema=True)

                # Identify date columns dynamically
                date_columns = [col_name for col_name, col_type in df.dtypes if col_type == 'date']

                if len(date_columns) != 0:
                    for date_col in date_columns:
                        df = df.withColumn(date_col + '_add_months', F.add_months(df[date_col], 1))

                    # Add a new column with the current timestamp
                    current_timestamp = F.current_timestamp()
                    df_new = df.withColumn('current_timestamp', current_timestamp)

                    timestamp_str = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

                    new_filename = f"{new_prefix}_{filename[len(old_prefix):-4]}_{timestamp_str}.csv"

                    new_file_path = os.path.join(output_path, new_filename)

                    # Write the DataFrame to a text file with header
                    df_new.write.option("delimiter", "\t").option("header", "true").csv(new_file_path)

                    file_counter += 1

                    # Append the file number to the processed file name
                    processed_file_name = f"File {file_counter}: {filename}"
                    processed_filename = f"File {file_counter}: {new_filename}"
                    print(f"Processed : {processed_file_name}")
                    print(f"Stored: {processed_filename}")
                    print()

                else:
                    print(f"No date columns found in {filename}, skipping...")

            except Exception as e:
                # Log the error
                logging.error(f"Error processing {filename}: {str(e)}")

    # Stop the SparkSession
    spark.stop()






# import re
# from datetime import datetime, timedelta
# import os
#
#
#
#
# def process_text_file(input_path, output_path, new_prefix, old_prefix):
#
#     # List all files in the input folder
#     file_list = os.listdir(input_path)
#
#     # Regular expression pattern to match 'YYYY-MM-DD' date-like values
#     date_pattern = r'\d{4}-\d{2}-\d{2}'
#
#     # Define the number of months to add
#     months_to_add = 1
#
#     # Process each file in the input folder
#     for file_name in file_list:
#         input_paths = os.path.join(input_path, file_name)
#
#         # Determine the format based on file extension
#         file_extension = file_name.split(".")[-1].lower()
#
#         if file_extension == "txt":
#             # Read the text file
#             with open(input_paths, 'r') as file:
#                 lines = file.readlines()
#
#             # Define the column name for the timestamp
#             timestamp_column_name = "current_timestamp"
#
#             # Process each line in the text file
#             modified_lines = []
#             for line in lines:
#                 # Use regular expression to find and modify date-like values
#                 matches = re.findall(date_pattern, line)
#                 modified_line = line
#                 for match in matches:
#                     year, month, day = map(int, match.split('-'))
#                     original_date = datetime(year, month, day)
#                     new_date = original_date + timedelta(days=30 * months_to_add)  # Add months by adding days
#                     new_date_str = new_date.strftime("%Y-%m-%d")
#
#                     # Append the original date and the result of add_months as new columns
#                     modified_line = modified_line.replace(match, f"{match}\t{new_date_str}")
#
#                 # Append the current timestamp as a new column at the end of each line
#                 timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                 modified_line = f"{modified_line.strip()}\t{timestamp_str}\n"
#
#                 modified_lines.append(modified_line)
#
#             # Create the output file name with a timestamp
#             timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
#             output_file_name = f"{new_prefix}_{file_name[len(old_prefix):-4]}_{timestamp_str}.txt"  # Modify the output file name here
#
#             # Write the modified lines to the output folder
#             output_paths = os.path.join(output_path, output_file_name)
#             with open(output_paths, 'w') as output_file:
#                 output_file.writelines(modified_lines)
#             print(f"Processed and Stored:{file_name}")
#             print(f"Processed and Stored:{output_file_name}")
#             print()
#         else:
#             print(f"Skipping file {file_name} as the format is not supported.")