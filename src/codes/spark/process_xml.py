import logging
import os
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

file_counter = 0


def process_xml_file(input_path, output_path, new_prefix, old_prefix):
    global file_counter, filename
    spark = SparkSession.builder.appName("XmlProcessing").config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.13.0").getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    try:
        for filename in os.listdir(input_path):
            if filename.endswith('.xml'):
                file_path = os.path.join(input_path, filename)

                # Read the XML file
                df = spark.read.format("xml").option("rowTag", "row").load(file_path)

                # Get all date columns
                date_columns = [col_name for col_name, col_type in df.dtypes if col_type in ('timestamp', 'date')]

                if len(date_columns) != 0:
                    for date_col in date_columns:
                        new_col_name = f"{date_col}_{new_prefix}"  # Create unique new column names
                        df = df.withColumn(new_col_name, F.add_months(df[date_col], 1))

                    # Add a new column with the current timestamp
                    current_timestamp = F.current_timestamp()
                    df_new = df.withColumn('current_timestamp', current_timestamp)

                    # Get the current timestamp in a string format
                    timestamp_str = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

                    # Create the output file name with a timestamp
                    new_filename = f"{new_prefix}_{filename[len(old_prefix):-4]}_{timestamp_str}.xml"

                    new_file_path = os.path.join(output_path, new_filename)

                    # Write the DataFrame as XML
                    df_new.write.format("xml").option("rootTag", "data").option("rowTag", "row").save(new_file_path)

                    file_counter += 1

                    # Append the file number to the processed file name
                    processed_file_name = f"File {file_counter}: {filename}"
                    processed_filename = f"File {file_counter}: {new_filename}"
                    print(f"Processed : {processed_file_name}")
                    print(f"Stored: {processed_filename}")
                    print()

                else:
                    print(f"No date columns found in {filename}, skipping...")

        # Stop the SparkSession
        spark.stop()

    except Exception as e:
        # Log the error
        logging.error(f"Error processing {filename}: {str(e)}")







