import re
import json

# with open('old_cdh_hive_ddl.txt', 'r') as file:
#     data = file.read()

script_updated_table = 0
def updateTableName(script_updated_param):
    global script_updated_table

    with open('C:/Users/SCHILLAL/PycharmProjects/Accelerator/CDH2CDP/code/config.json', 'r') as f:
        data = json.load(f)

    #regex3 = r"(CREATE TABLE ')([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)(')"
    # script_updated_table = re.sub(regex3, r"CREATE TABLE '\2'.'\3'", script_updated_param)
    # regex3 = r"(?<=CREATE TABLE )([\w.]+)"
    regex3 = r"(?<=TABLE )([\w.]+)"

    script_updated_table =re.sub(regex3, lambda m: "'" + m.group(1).replace('.', "'.'") + "'", script_updated_param)

    return script_updated_table
