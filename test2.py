from dateutil.parser import parse

from commons import snowflake_connection, execute_insert
from loader_commons import get_timestamp_sk, get_author_sk, get_subject_sk, get_level_sk

connection = snowflake_connection()


def date_suffix(day):
    return 'th' if 11 <= day <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th')


def check_pub_time_availability(timestamp):
    rows = get_timestamp_sk(connection, timestamp)
    if len(rows) > 0:
        return True
    else:
        return False


def process_timestamp(timestamp):
    datetime = parse(timestamp)

    return {
        "datetime": timestamp,
        "date": datetime.strftime("%Y-%m-%d"),
        "day": datetime.day,
        "month": datetime.month,
        "year": datetime.year,
        "suffix": date_suffix(datetime.day),
        "week_of_year": int(datetime.strftime("%V")),
        "quarter": (datetime.month - 1) // 3 + 1,
        "day_of_week": datetime.strftime('%A'),
        "y_mm": datetime.strftime("%Y-%m")
    }


def insert_timestamp(new_entries):
    params = [process_timestamp(entry) for entry in new_entries]

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

    execute_insert(query=query, params=params, connection=connection, function="insert_timestamp")


def load_pub_time_data(data_json):
    if "published_timestamp" in data_json:
        new_entries = []
        for key, value in data_json["published_timestamp"].items():
            if not check_pub_time_availability(timestamp=value):
                new_entries.append(str(value))
            if len(new_entries) == 10:
                break

        insert_timestamp(new_entries=new_entries)


###########################################################################################

def check_author_availability(author_data_str):
    rows = get_author_sk(connection, author_data_str)
    if len(rows) > 0:
        return True
    else:
        return False


def process_author(author_data_str):
    author_data = author_data_str.split("_")
    return {
        "author_id": author_data_str,
        "f_name": author_data[0],
        "l_name": author_data[1],
        "code": author_data[2]
    }


def insert_author(new_entries):
    params = [process_author(entry) for entry in new_entries]

    query = """INSERT INTO "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."AUTHOR_DIM"  
                               (A_AUTHOR_ID, 
                                A_AUTHOR_F_NAME, 
                                A_AUTHOR_L_NAME, 
                                A_AUTHOR_CODE) 
               VALUES (%(author_id)s, %(f_name)s, %(l_name)s, %(code)s);"""
    execute_insert(query=query, params=params, connection=connection, function="insert_author")


def load_author_data(data_json):
    if "author" in data_json:
        new_entries = []
        for key, value in data_json["author"].items():
            if not check_author_availability(author_data_str=value):
                new_entries.append(str(value))

            if len(new_entries) == 10:
                break
        insert_author(new_entries=new_entries)


#########################################################################
def check_subject_availability(subject_str):
    rows = get_subject_sk(connection, subject_str)
    if len(rows) > 0:
        return True
    else:
        return False


def insert_subject(new_entries):
    params = [{"subject": entry} for entry in new_entries]

    query = """INSERT INTO "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."SUBJECT_DIM" 
                           (S_SUBJECT) 
                    VALUES (%(subject)s);"""
    execute_insert(query=query, params=params, connection=connection, function="insert_subject")


def load_subject_data(data_json):
    if "subject" in data_json:
        new_entries = []
        for key, value in data_json["subject"].items():
            if not check_subject_availability(subject_str=value):
                new_entries.append(str(value))

                if len(new_entries) == 10:
                    break
            insert_subject(new_entries=new_entries)

##############################################################################
def check_level_availability(level_str):
    rows = get_level_sk(connection, level_str)
    if len(rows) > 0:
        return True
    else:
        return False

def insert_level(new_entries):
    params = [{"level": entry} for entry in new_entries]

    query = """INSERT INTO "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."LEVEL_DIM" 
                           (L_LEVEL) 
                    VALUES (%(level)s);"""
    execute_insert(query=query, params=params, connection=connection, function="insert_level")


def load_level_data(data_json):
    if "level" in data_json:
        new_entries = []
        for key, value in data_json["level"].items():
            if not check_level_availability(level_str=value):
                new_entries.append(str(value))

                if len(new_entries) == 10:
                    break

        insert_level(new_entries=new_entries)

##########################################################################
def insert_course_publication(new_entries):
    query = """INSERT INTO "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."COURSE_PUBLICATIONS"  
                            (CP_COURSE_ID, CP_AUTHOR_SK, CP_PUBLISHED_TIMESTAMP_SK, CP_SUBJECT_SK,
                             CP_IS_PAID, CP_PRICE, CP_NUM_LECTURES, CP_NUM_REVIEWS, CP_NUM_SUBS,
                             CP_CONTENT_DURATION, CP_LEVEL_SK ,CP_COURSE_TITLE) 
                         VALUES 
                             (%(course_id)s, %(author_sk)s, %(timestamp_sk)s, %(subject_sk)s, %(is_paid)s, %(price)s, 
                             %(num_lectures)s, %(num_reviews)s, %(num_subscribers)s, %(cont_dur)s, %(level_sk)s,
                              %(course_title)s)"""
    execute_insert(query=query, params=new_entries, connection=connection,
                   function="insert_course_publication")

def update_course_publications(data_json):
    new_entries = []
    for key, value in data_json["course_id"].items():
        course_data = {
            "course_id": str(value),
            "author_sk": get_author_sk(connection=connection, author_id_str=data_json["author"][str(key)])[0][0],
            "timestamp_sk": get_timestamp_sk(connection=connection,
                                             timestamp_str=data_json["published_timestamp"][str(key)])[0][0],
            "subject_sk": get_subject_sk(connection=connection, subject_str=data_json["subject"][str(key)])[0][0],
            "is_paid": data_json["is_paid"][str(key)],
            "price": data_json["price"][str(key)],
            "num_lectures": data_json["num_lectures"][str(key)],
            "num_reviews": data_json["num_reviews"][str(key)],
            "num_subscribers": data_json["num_subscribers"][str(key)],
            "cont_dur": data_json["content_duration"][str(key)],
            "level_sk": get_level_sk(connection=connection, level_str=data_json["level"][str(key)])[0][0],
            "course_title": data_json["course_title"][str(key)]
        }
        new_entries.append(course_data)
        if len(new_entries) == 5:
            break

    insert_course_publication(new_entries=new_entries)





import json

file = open('data/course_data.json')
data = json.load(file)
load_pub_time_data(data_json=data)
load_author_data
