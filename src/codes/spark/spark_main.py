import os
import threading
import asyncio

from paths.config_reader import read_input
#from Accelerator.CDH2CDP.internal_code.spark.spark_progress import process_file


from .spark_progress import process_file


async def main():
    #folder_path = 'C:/Users/PycharmProject/Accelerator/data/Media/user_input'
    folder_path = read_input()

    file_names = [filename for filename in os.listdir(folder_path) if
                  filename.endswith(('.csv', '.txt', '.xml', '.json', '.orc', '.avro', '.parquet', '.py'))]
    print("file_names===\n")
    print(file_names)
    threads = []

    for file_name in file_names:
        thread = threading.Thread(target=process_file, args=(file_name,))
        print(thread)
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete before moving on
    for thread in threads:
        thread.join()

    # Remove input files after all threads have completed
    for file_name in file_names:
        file_path = os.path.join(folder_path, file_name)
        os.remove(file_path)

    await asyncio.sleep(1)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()

# ---------------------------------------------------------------------------------
# import os
# import threading
# import asyncio
# import time
#
# from . import spark_progress
#
#
# #from . import spark_progress
#
#
# async def main():
#     folder_path = 'C:/Users/lyekollu/PycharmProject_Updated/Accelerator/Media/user_input'
#
#     # List of filenames in the folder ending with '.csv'
#     # file_names = [filename for filename in os.listdir(folder_path) if filename.endswith('.txt')]
#
#     # List of filenames in the folder with valid extensions
#     file_names = [filename for filename in os.listdir(folder_path) if filename.endswith(('.csv', '.txt', '.xml', '.json', '.orc'))]
#
#     # Create and start threads to process files
#     threads = []
#
#     # Loop through each file and create a thread for processing
#     for i in range(len(file_names)):
#         file_name = file_names[i]
#         thread = threading.Thread(target=spark_progress.progress, args=(file_name,))
#         # thread = threading.Thread(target=spark_progress.progress, args=(folder_path + "/" + file_names[i],))
#         threads.append(thread)
#         thread.start()
#         print("Thread", i + 1, " started")
#         thread.join()
#         print("Thread", i + 1, " terminated")
#         print("---------------------")
#         # time.sleep(1)
#
#     # Remove input files after all threads have completed
#     for file_name in file_names:
#         file_path = os.path.join(folder_path, file_name)
#         os.remove(file_path)
#
#     await asyncio.sleep(1)
#
#
# if __name__ == "__main__":
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(main())
#     loop.close()
