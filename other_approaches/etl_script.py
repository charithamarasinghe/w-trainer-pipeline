import snowflake.connector
import json
import configparser
import os

base_path = os.getcwd()

config_params = configparser.ConfigParser()
config_params.read(base_path + "/config.ini")

with open('course_data.json') as f:
    data = json.load(f)

# Connect to your Snowflake account

ctx = snowflake.connector.connect(
    account=config_params["DATABASE"]["account"],
    user=config_params["DATABASE"]["user"],
    password=config_params["DATABASE"]["password"],
    database='DB_COURSE_PUBLICATIONS_STAGE',
    schema='SC_COURSE_PUBLICATIONS'
)
cs = ctx.cursor()


def get_sql_commands():
    file = open(base_path + "/other_approaches/required_queries.sql", 'r')
    sql_file = file.read()
    file.close()

    return [command.replace("\n", "") for command in sql_file.split(';')]


try:
    cs.execute("create or replace file format c_p_stage type = json")
    cs.execute("create or replace temporary stage T_COURSE_DATA file_format = c_p_stage")
    cs.execute("put file://{base_path}/data/course_data.json @T_COURSE_DATA".format(base_path=base_path))

    commands = get_sql_commands()
    for sql_command in commands:
        cs.execute(sql_command)


finally:
    cs.close()
ctx.close()
