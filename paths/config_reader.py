import json



def read_config():
    with open('C:/Users/PycharmProjects/Accelerator/paths/config.json', 'r') as f:
        data = json.load(f)
    return data

def read_input():

    data = r'C:/Users/PycharmProjects/Accelerator/data/Media/user_input/'
    return data

def write_output():
    new_path = r'C:/Users/PycharmProjects/Accelerator/data/Media/destination_folder/'
    return new_path


def zip_files():
    zipped_files = r'C:/Users/PycharmProjects/Accelerator/data/Media/zip_files/'
    return zipped_files


def unzip_files():
    unzipped_files = r'C:/Users/PycharmProjects/Accelerator/data/Media/unzipped_files/'
    return unzipped_files