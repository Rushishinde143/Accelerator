import os
import threading
import time

from Accelerator.src.code.main import main

from Accelerator.paths.config_reader import read_input

folder_path = 'C:/Users/PycharmProject/Accelerator/data/Media/user_input/'
folder_path = read_input()

os.chdir(folder_path)
keys = []
values = []
i = 1
with os.scandir(folder_path) as directory:
    for item in directory:
        file_path = os.path.join(folder_path, item)
        absolute_path = os.path.abspath(file_path)
        values.append(absolute_path)
        keys.append(i)
        i= i+1

        # print(absolute_path)
# print(keys)
# print(values)

items = os.listdir(folder_path)

files = [item for item in items if os.path.isfile(os.path.join(folder_path, item))]
file_count = len(files)
print("Number of files:", file_count)

filepath_dict = {}
for i in range(file_count):
    key = keys[i]
    value = values[i]
    filepath_dict[key] = value

print(filepath_dict)

threads = []
for i in range(len(filepath_dict)+1):
    thread = threading.Thread(target=main(filepath_dict[i]))
    threads.append(thread)
    thread.start()
    time.sleep(2)

for thread in threads:
    thread.join()

# Objective : Achieve parallel batch processing:
#
# Parse the input directory for all the files uploaded
# Prepare a dict with key:value as {1:'abs file path for file1', 2:'abs file path for file2'} and so on
# Once the dynamic dict is created, parse the dict in a loop from 1 till length of the dict and create threads dynamically and assign each dict items value to process that file in that thread.
# Test with a small batch of 3 and then check if it works for 1 file as well as multiple files