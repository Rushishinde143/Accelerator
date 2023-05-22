import re
import json

result=0

def changepath(script_data):
    global result
    with open('C:/Users/SCHILLAL/PycharmProjects/Accelerator/CDH2CDP/code/config.json', 'r') as f:
        data = json.load(f)

    regex = data["hive"]["update"]["old_input_hdfs_cdh_path"]
    replace_with = data["hive"]["update"]["new_input_hdfs_cdp_path"]
    result = re.sub(regex, replace_with, script_data)
    # print(result)
    # print('----change path method done')
    return result



