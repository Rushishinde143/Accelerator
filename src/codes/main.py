import os
import threading
import asyncio

from .hive import progress
from paths.config_reader import read_input



async def main():
    # print("Hi")
    #folder_path = 'C:/Users/PycharmProject/Accelerator/data/Media/user_input/'
    folder_path = read_input()

    os.chdir(folder_path)
    # keys = []
    # values = []
    # i = 1
    # with os.scandir(folder_path) as directory:
    #     for item in directory:
    #         file_path = os.path.join(folder_path, item).replace("\\", "/")
    #         print("File path ", i, file_path)
    #         print("\n")
    #         # absolute_path = os.path.abspath(file_path)
    #         values.append(file_path)
    #         keys.append(i)
    #         i = i + 1

    items = os.listdir(folder_path)

    files = [item for item in items if os.path.isfile(os.path.join(folder_path, item).replace("\\", "/"))]
    file_count = len(files)
    # print("Number of files:", file_count)

    for i in range(len(files)):
        t = threading.Thread(target=progress.progress, args=(folder_path + "/" + files[i],))
        t.start()

        t.join()

    await asyncio.sleep(1)

# if __name__ =="__main__":
#     asyncio.run(main())
