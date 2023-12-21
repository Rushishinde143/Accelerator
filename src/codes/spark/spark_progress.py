import os
import logging

from paths.config_reader import read_input, write_output
from .process_avro import process_avro_file
from .process_json import process_json_file
from .process_orc import process_orc_file
from .process_parquet import process_parquet_file
from .process_xml import process_xml_file
from .spark_update_file import process_csv
from .new_spark import convert_spark_code


from .process_text import process_text_file

# Configure the logging
logging.basicConfig(filename='processing.log', level=logging.DEBUG)


def process_file(file_name):
    if file_name is None:
        logging.error("Received None as file name. Skipping...")
        return

    old_prefix = 'old'
    new_prefix = 'new'
    # input_path = "C:/Users/PycharmProject/Accelerator/data/Media/user_input/"
    # output_path = "C:/Users/PycharmProject/Accelerator/data/Media/destination_folder/"
    input_path = read_input()
    output_path = write_output()
    os.chdir(input_path)

    try:
        file_path = os.path.join(input_path, file_name)

        # Rest of your processing code...
        if file_path.endswith('.csv'):
            process_csv(input_path, output_path, new_prefix, old_prefix, file_name)
            # process_spark(input_path, output_path, new_prefix, old_prefix)
        elif file_path.endswith('.txt'):
            process_text_file(input_path, output_path, new_prefix, old_prefix)
        elif file_path.endswith('.xml'):
            process_xml_file(input_path, output_path, new_prefix, old_prefix)
        elif file_path.endswith('.orc'):
            process_orc_file(input_path, output_path, new_prefix, old_prefix)
        elif file_path.endswith('.avro'):
            process_avro_file(input_path, output_path, new_prefix, old_prefix)
        elif file_path.endswith('.json'):
            process_json_file(input_path, output_path, new_prefix, old_prefix)
        elif file_path.endswith('.parquet'):
            process_parquet_file(input_path, output_path, new_prefix, old_prefix)
        elif file_path.endswith('.py'):
            convert_spark_code(input_path, output_path, new_prefix, old_prefix)

        else:
            print(f"Skipping unsupported file: {file_name}")

    except Exception as e:
        # Log the error
        logging.error(f"Error processing {file_name}: {str(e)}")

















# import os
# import re
#
# # from Accelerator.CDH2CDP.internal_code.spark import spark_new_path, spark_update_file, process_text
# from . import spark_new_path, spark_update_file, process_text
# from .process_text import process_text_file
#
#
# def progress(file_path, file_name=None):
#     old_prefix = 'old'
#     new_prefix = 'new'
#     input_path = "C:/Users/GLIN/PycharmProject_Updated/Accelerator/Media/user_input/"
#     output_path = "C:/Users/GLIN/PycharmProject_Updated/Accelerator/Media/destination_folder/"
#     os.chdir(input_path)
#
#     with open(file_path, mode="r+") as file:
#         spark_script = file.read()
#
#         for i in spark_script:
#             spark_script = re.sub(' +', ' ', spark_script)
#             spark_script = re.sub('\n', '\n', spark_script)
#         script_updated_path = spark_new_path.update_spark_path(spark_script)
#         file_extension = os.path.splitext(file_path)[1]
#
#         if file_extension == '.csv':
#             spark_update_file.process_csv(input_path, output_path, new_prefix, old_prefix, file_name)
#         elif file_extension == '.txt':
#             process_text.process_text_file(input_path, output_path, new_prefix, old_prefix)
#         else:
#             pass

# WORKING

# spark_progress.py
# import os
# import re
# import logging
# from datetime import datetime
#
# from .process_xml import process_xml_file
# from .spark_update_file import process_csv
# from pyspark.sql import SparkSession
# import pyspark.sql.functions as F
# from .process_text import process_text_file
#
# # Configure the logging
# logging.basicConfig(filename='processing.log', level=logging.DEBUG)
#
#
# def process_file(file_name):
#     if file_name is None:
#         logging.error("Received None as file name. Skipping...")
#         return
#
#     old_prefix = 'old'
#     new_prefix = 'new'
#     input_path = "C:/Users/GLIN/PycharmProject_Updated/Accelerator/Media/user_input/"
#     output_path = "C:/Users/GLIN/PycharmProject_Updated/Accelerator/Media/destination_folder/"
#     os.chdir(input_path)
#
#     try:
#         file_path = os.path.join(input_path, file_name)
#
#         # Rest of your processing code...
#         if file_path.endswith('.csv'):
#             process_csv(input_path, output_path, new_prefix, old_prefix, file_name)
#         elif file_path.endswith('.txt'):
#             process_text_file(input_path, output_path, new_prefix, old_prefix)
#         elif file_path.endswith(' .xml'):
#             process_xml_file(input_path, output_path, new_prefix, old_prefix)
#
#         else:
#             print(f"Skipping unsupported file: {file_name}")
#
#     except Exception as e:
#         # Log the error
#         logging.error(f"Error processing {file_name}: {str(e)}")