import json
def update_spark_path(spark_script):
    with open('C:/Users/SCHILLAL/PycharmProjects/cdh_to_cdp/code/config.json', 'r') as f:
        data = json.load(f)

    spark_old_path=data["spark"]["update"]["spark_old_path"]
    spark_new_path = data["spark"]["update"]["spark_new_path"]

    new_spark_script = spark_script.replace(spark_old_path, spark_new_path)

    return new_spark_script
