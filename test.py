import json
import snowflake.connector as snow
import logging
import configparser
import sys
from dateutil.parser import parse

base_path = "G:/Projects/w-trainer/w-trainer-pipeline"
sys.path.insert(1, base_path)

from commons import snowflake_connection, execute_select, execute_insert

logging.basicConfig(filename=base_path + '/logs/pipeline.log',
                    filemode='a+',
                    format="%(asctime)s %(levelname)s: %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")

logger = logging.getLogger()
logger.setLevel(logging.WARNING)

config = configparser.ConfigParser()
config.read(base_path + "/config.ini")

file = open('data/course_data.json')
data = json.load(file)

conn = snowflake_connection()


#############################################################
def update_pub_time_dim(connection, data_json):
    for item in data_json:
        load_pub_time_data(connection=connection, data_item=item)


def check_pub_time_availability(connection, timestamp):
    rows = get_timestamp_sk(connection, timestamp)
    if len(rows) > 0:
        return True
    else:
        return False


def date_suffix(day):
    return 'th' if 11 <= day <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th')


def insert_timestamp(connection, timestamp):
    datetime = parse(timestamp)
    date = datetime.strftime("%Y-%m-%d")

    params = {
        "date": datetime.strftime("%Y-%m-%d"),
        "day": date.day,
        "month": date.month,
        "year": date.year,
        "suffix": date_suffix(date.day),
        "week_of_year": int(date.strftime("%V")),
        "quarter": (date.month - 1) // 3 + 1,
        "day_of_week": date.strftime('%A'),
        "y_mm": date.strftime("%Y-%m")
    }

    query = """INSERT INTO "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."PUBLISHED_TIMESTAMP_DIM" 
                                            (PT_TIMESTAMP, 
                                             PT_DATE, 
                                             PT_DAY, 
                                             PT_MONTH, 
                                             PT_YEAR, 
                                             PT_DAY_SUFFIX, 
                                             PT_WEEK_OF_YEAR, 
                                             PT_QUARTER_OF_YEAR,
                                             PT_DAY_OF_WEEK, 
                                             PT_YYYY_MM) 
               VALUES (%(datetime)s, %(date)s, %(day)s, %(month)s, %(year)s, 
                         %(suffix)s, %(week_of_year)s, %(quarter)s, %(day_of_week)s, %(y_mm)s);"""

    execute_insert(query=query, params=params, connection=connection, function="insert_author")


def load_pub_time_data(connection, data_item):
    if "published_timestamp" in data_item:
        for key, value in data_item["published_timestamp"].items():
            if not check_pub_time_availability(connection=connection, timestamp=value):
                insert_timestamp(connection=connection, timestamp=str(value))


#############################################################
def update_author_dim(connection, data_json):
    for item in data_json:
        load_pub_time_data(connection=connection, data_item=item)


def check_author_availability(connection, author_data_str):
    query = """SELECT 
                    A_AUTHOR_ID 
                FROM 
                    "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."AUTHOR_DIM" 
                WHERE 
                    A_AUTHOR_ID = %(author_id)s;"""
    rows = execute_select(query=query, params={"author_id": author_data_str},
                          connection=connection, function="check_author_availability")
    if len(rows):
        return True
    else:
        return False


def insert_author(connection, author_data_str):
    author_data = author_data_str.split("_")
    params = {
        "author_id": author_data_str,
        "f_name": author_data[0],
        "l_name": author_data[1],
        "code": author_data[2]
    }

    query = """INSERT INTO "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."AUTHOR_DIM"  
                               (A_AUTHOR_ID, 
                                A_AUTHOR_F_NAME, 
                                A_AUTHOR_L_NAME, 
                                A_AUTHOR_CODE) 
               VALUES (%(author_id)s, %(f_name)s, %(l_name)s, %(code)s);"""
    execute_insert(query=query, params=params, connection=connection, function="insert_author")


def load_author_data(connection, data_item):
    if "author" in data_item:
        for key, value in data_item["author"].items():
            if not check_author_availability(connection, value):
                insert_author(connection, author_data_str=str(value))


#############################################################

def check_subject_availability(connection, subject_str):
    query = """SELECT 
                    S_SUBJECT 
                FROM 
                    "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."SUBJECT_DIM" 
                WHERE 
                    S_SUBJECT = %(subject)s;"""
    rows = execute_select(query=query, params={"subject": subject_str},
                          connection=connection, function="check_subject_availability")
    if len(rows):
        return True
    else:
        return False


def insert_subject(connection, subject_str):
    params = {
        "subject": subject_str
    }

    query = """INSERT INTO "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."SUBJECT_DIM" 
                           (S_SUBJECT) 
                    VALUES (%(subject)s;"""
    execute_insert(query=query, params=params, connection=connection, function="insert_subject")


def update_subject_dim(connection, data_json):
    for item in data_json:
        load_subject_data(connection=connection, data_item=item)


def load_subject_data(connection, data_item):
    if "subject" in data_item:
        for key, value in data_item["subject"].items():
            if not check_subject_availability(connection=connection, subject_str=value):
                insert_subject(connection=connection, subject_str=str(value))


#############################################################
def check_level_availability(connection, level_str):
    query = """SELECT 
                    L_LEVEL 
                FROM 
                    "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."LEVEL_DIM" 
                WHERE 
                    L_LEVEL = %(level)s;"""
    rows = execute_select(query=query, params={"level": level_str},
                          connection=connection, function="check_level_availability")
    if len(rows):
        return True
    else:
        return False


def insert_level(connection, level_str):
    params = {
        "level": level_str
    }

    query = """INSERT INTO "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."LEVEL_DIM" 
                           (L_LEVEL) 
                    VALUES (%(level)s;"""
    execute_insert(query=query, params=params, connection=connection, function="insert_level")


def update_level_dim(connection, data_json):
    for item in data_json:
        load_level_data(connection=connection, data_item=item)


def load_level_data(connection, data_item):
    if "level" in data_item:
        for key, value in data_item["level"].items():
            if not check_level_availability(connection=connection, level_str=value):
                insert_level(connection=connection, level_str=value)


#############################################################



def insert_course_publication(connection, course_publication):
    query = """INSERT INTO "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."COURSE_PUBLICATIONS"  
                            (CP_COURSE_ID, CP_AUTHOR_SK, CP_PUBLISHED_TIMESTAMP_SK, CP_SUBJECT_SK,
                             CP_IS_PAID, CP_PRICE, CP_NUM_LECTURES, CP_NUM_REVIEWS, CP_NUM_SUBS,
                             CP_CONTENT_DURATION, CP_LEVEL_SK ,CP_COURSE_TITLE) 
                         VALUES 
                             (%(course_id)s, %(author_sk)s, %(timestamp_sk)s, %(subject_sk)s, %(is_paid)s, %(price)s, 
                             %(num_lectures)s, %(num_reviews)s, %(num_subscribers)s, %(cont_dur)s, %(level_sk)s,
                              %(course_title)s)"""
    execute_insert(query=query, params=course_publication, connection=connection, function="insert_course_publication")


def update_course_publications(connection, data_json):
    for key, value in data_json["course_id"].items():
        course_data = {
            "course_id": str(value),
            "author_sk": get_author_sk(connection=connection, author_id_str=data_json["author"][key])[0][0],
            "timestamp_sk": get_timestamp_sk(connection=connection,
                                             timestamp_str=data_json["published_timestamp"][key])[0][0],
            "subject_sk": get_subject_sk(connection=connection, subject_str=data_json["subject"][key])[0][0],
            "is_paid": data_json["is_paid"][key],
            "price": data_json["price"][key],
            "num_lectures": data_json["num_lectures"][key],
            "num_reviews": data_json["num_reviews"][key],
            "num_subscribers": data_json["num_subscribers"][key],
            "cont_dur": data_json["content_duration"][key],
            "level_sk": get_level_sk(connection=connection, level_str=data_json["level_sk"][key])[0][0],
            "course_title": data_json["course_title"][key]
        }

        insert_course_publication(course_publication=course_data, connection=connection)


import snowflake.connector as snow

from main import config, logger


def create_snowflake_connection():
    try:
        connection = snow.connect(user=config["DATABASE"]["user"],
                                  password=config["DATABASE"]["password"],
                                  account=config["DATABASE"]["account"])
        return connection
    except Exception as error:
        logger.error("create_snowflake_connection: {error}".format(error=str(error)))
        return None


def snowflake_connection(connection=None):
    if connection is None:
        return create_snowflake_connection()
    elif connection.is_closed():
        return create_snowflake_connection()
    else:
        return connection


def execute_insert(query, params, connection, function):
    try:
        connection = snowflake_connection(connection=connection)
        cursor = connection.cursor()
        cursor.execute(query, params)
        cursor.close()
    except Exception as error:
        logger.error("{function}: {error}".format(function=function, error=str(error)))


def execute_select(query, params, connection, function):
    try:
        connection = snowflake_connection(connection=connection)
        cursor = connection.cursor()
        cursor.execute(query, params)
        query_data = cursor.fetchall()
        cursor.close()
        return query_data
    except Exception as error:
        logger.error("{function}: {error}".format(function=function, error=str(error)))
        return []


def get_timestamp_sk(connection, timestamp_str):
    query = """SELECT 
                    PT_PUBLISHED_TIMESTAMP_SK 
                FROM 
                    "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."PUBLISHED_TIMESTAMP_DIM" 
                WHERE 
                    PT_TIMESTAMP = %(timestamp)s;"""

    return execute_select(query=query, params={"timestamp": timestamp_str},
                          connection=connection, function="get_timestamp_sk")


def get_author_sk(connection, author_id_str):
    query = """SELECT
                    A_AUTHOR_SK 
                FROM 
                    "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."AUTHOR_DIM" 
                WHERE 
                    A_AUTHOR_ID = %(author_id)s;"""

    return execute_select(query=query, params={"author_id": author_id_str},
                          connection=connection, function="get_author_sk")


def get_subject_sk(connection, subject_str):
    query = """SELECT 
                    S_SUBJECT_SK 
                FROM 
                    "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."SUBJECT_DIM" 
                WHERE 
                    S_SUBJECT = %(subject)s;"""

    return execute_select(query=query, params={"subject": subject_str},
                          connection=connection, function="get_subject_sk")


def get_level_sk(connection, level_str):
    query = """SELECT 
                    L_LEVEL_SK 
                FROM 
                    "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."LEVEL_DIM" 
                WHERE 
                    L_LEVEL = %(level)s;"""

    return execute_select(query=query, params={"level": level_str},
                          connection=connection, function="get_level_sk")

#############################################################
conn = snowflake_connection()
cur = conn.cursor()

sql = "USE ROLE SYSADMIN"
cur.execute(sql)

cur.execute(
    """SELECT A_AUTHOR_ID from "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."AUTHOR_DIM" where A_AUTHOR_ID = %(author_id)s;""",
    {"author_id": "abc"})
data1 = cur.fetchall()
cur.close()
