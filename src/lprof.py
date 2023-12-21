import os

 

def get_file_info(directory):
    total_size = 0
    file_count = 0

 

    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            total_size += os.path.getsize(file_path)
            file_count += 1

 

    return file_count, total_size

 

if __name__ == '__main__':
    directory_path = 'C:/Users/PycharmProjects/Accelerator/Media/destination_folder'

 

    if os.path.exists(directory_path) and os.path.isdir(directory_path):
        file_count, total_size = get_file_info(directory_path)
        print(f"Number of Files: {file_count}")
        print(f"Total Size (in bytes): {total_size} bytes")
        print(f"Total Size (in kilobytes): {total_size / 1024:.2f} KB")
        print(f"Total Size (in megabytes): {total_size / (1024 * 1024):.2f} MB")
    else:
        print("The specified directory does not exist.")
