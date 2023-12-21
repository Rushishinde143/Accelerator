import json
import re
from config.config_reader import read_config

def update_spark_path(spark_script):
    global result
    #with open('C:/Users/PycharmProject/Accelerator/paths/paths.json', 'r') as f:
    data = read_config()

    regex = data["spark"]["update"]["spark_old_path"]
    replace_with = data["spark"]["update"]["spark_new_path"]
    result = re.sub(regex, replace_with, spark_script)

    return result
