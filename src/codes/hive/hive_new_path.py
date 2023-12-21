import re
import json

from paths.config_reader import read_config
result = 0


def changepath(script_data):
    global result
   # with open(read_config, 'r') as f:
    data = read_config()

    regex = data["hive"]["update"]["old_input_hdfs_cdh_path"]
    replace_with = data["hive"]["update"]["new_input_hdfs_cdp_path"]
    result = re.sub(regex, replace_with, script_data)
    # print(result)
    # print('----change path method done')
    return result






