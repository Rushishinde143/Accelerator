import re
from paths.config_reader import read_config


script_updated_param = 0


def removeUnwantedParameters(script_updated_path):
    global script_updated_param

  #  with open('C:/Users/PycharmProject/Accelerator/paths/paths.json', 'r') as f:
    data = read_config()

    # regex2 = r'^set.*\n'
    # regex2 = r'^\s*set\s.*(\s\n.*)$'
    # regex2 = "^set.*[\n\\s]",

    # regex2 = data["hive"]["remove"]["unwanted_param"]
    replace_with = data["hive"]["remove"]["replace_with"]
    old_hive_engine = data["hive"]["update"]["old_hive_engine"]
    new_hive_engine = data["hive"]["update"]["new_hive_engine"]

    remove = data['hive']['remove']
    keys = remove.keys()
    script_updated_param = script_updated_path
    for key in keys:
        if isinstance(remove[key], bool):
            if remove[key]:
                # print(key)
                script_updated_path = re.sub("\n" + key + ".*", replace_with, script_updated_path, flags=re.MULTILINE)
            else:
                pass

    # script_updated_param = re.sub(regex2, replace_with, script_updated_path, flags=re.MULTILINE)

    script_updated_param2 = re.sub(old_hive_engine, new_hive_engine, script_updated_path, flags=re.MULTILINE)
    return script_updated_param2
