import re
from paths.config_reader import read_config

script_updated_table = 0


def updateTableName(script_updated_param):
    global script_updated_table

  #  with open('C:/Users/PycharmProject/Accelerator/paths/paths.json', 'r') as f:
    data = read_config()

    regex3 = r"(?<=TABLE )([\w.]+)"

    script_updated_table = re.sub(regex3, lambda m: "'" + m.group(1).replace('.', "'.'") + "'", script_updated_param)

    return script_updated_table
