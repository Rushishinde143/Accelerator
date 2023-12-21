import asyncio
import os
import re
from datetime import datetime


from src.codes.hive.hive_new_path import changepath
from src.codes.hive.hive_rem_param import removeUnwantedParameters
from src.codes.hive.hive_update_table_name import updateTableName


from paths.config_reader import write_output

# Initialize a file counter
file_counter = 0

# progress.py



def format_timestamp():
    timestamp = datetime.now()
    formatted_timestamp = timestamp.strftime("%Y-%m-%d_%H-%M-%S")
    return formatted_timestamp


def progress(input_file):
    global result
    global file_counter  # Access the file counter
    # Extract the file name without the path
    file_name = os.path.basename(input_file)
    #new_path = 'C:/Users/PycharmProject/Accelerator/data/Media/destination_folder/'
    new_path = write_output()


    # Generate a formatted timestamp
    timestamp = format_timestamp()

    # Split the file name and extension
    base_name, file_extension = os.path.splitext(file_name)

    # Increment the file counter
    file_counter += 1

    # Append the file number to the processed file name
    processed_file_name = f"File {file_counter}: {file_name}"

    # Append the timestamp to the file name just before the extension with some space for time value
    new_item = f"{base_name.replace('old', 'new')}_{timestamp}{file_extension}"

    if os.path.isfile(input_file):
        with open(input_file, mode="r+") as file:
            script_data = file.read()

            # for line in script_data:
            script_data = re.sub(' +', ' ', script_data)
            script_data = re.sub('\n+', '\n', script_data)

            script_updated_path = changepath(script_data)
            script_updated_param = removeUnwantedParameters(script_updated_path)
            script_updated_table = updateTableName(script_updated_param)

            file.close()

            print(f"Processed: {processed_file_name}")  # Print the processed file name with file number
            with open(os.path.join(new_path, new_item), 'w') as file:
                file.write(script_updated_table)
                print(f"Stored: {new_item}")  # Print the stored file name

            #os.remove(input_file)
        print()

    #await asyncio.sleep(1)





















# import asyncio
# import os
# import re
# from datetime import datetime
#
# from .hive_new_path import changepath
# from .hive_rem_param import removeUnwantedParameters
# from .hive_update_table_name import updateTableName
#
#
# def progress(input_file):
#     global result;
#     # Extract the file name without the path
#     file_name = os.path.basename(input_file)
#     new_path = 'C:/Users/lyekollu/PycharmProject_Updated/Accelerator/Media/destination_folder/'
#     # Generate a timestamp
#     timestamp = datetime.now().strftime("%Y-%m-%d%H:%M:%S")
#     new_item = str(input_file).split("/")[-1].replace("old", f"new_{timestamp}", 1)
#     #new_item = str(input_file).split("/")[-1].replace("old", "new", 1)
#
#     # print(new_item)
#
#     if os.path.isfile(input_file):
#         with open(input_file, mode="r+") as file:
#             script_data = file.read()
#
#             # for line in script_data:
#             script_data = re.sub(' +', ' ', script_data)
#             script_data = re.sub('\n+', '\n', script_data)
#
#             script_updated_path = changepath(script_data)
#             script_updated_param = removeUnwantedParameters(script_updated_path)
#             script_updated_table = updateTableName(script_updated_param)
#
#             file.close()
#
#             with open(new_path + new_item, 'w') as file:
#                 file.write(script_updated_table)
#                 print(f"Processed and stored: {file_name}")
#
#             os.remove(input_file)
#     # await asyncio.sleep(1)
