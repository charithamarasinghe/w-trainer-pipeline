from dateutil.parser import parse

from commons import snowflake_connection, execute_insert
from loader_commons import get_timestamp_sk, get_author_sk, get_subject_sk, get_level_sk


def date_suffix(day):
    return 'th' if 11 <= day <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th')


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


def process_author(author_data_str):
    author_data = author_data_str.split("_")
    return {
        "author_id": author_data_str,
        "f_name": author_data[0],
        "l_name": author_data[1],
        "code": author_data[2]
    }


class DimensionLoader:
    def __init__(self, connection):
        self.connection = snowflake_connection(connection=connection)

    def check_pub_time_availability(self, timestamp):
        rows = get_timestamp_sk(self.connection, timestamp)
        if len(rows) > 0:
            return True
        else:
            return False

    def insert_timestamp(self, new_entries):
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

        execute_insert(query=query, params=params, connection=self.connection, function="insert_timestamp")

    def load_pub_time_data(self, data_json):
        if "published_timestamp" in data_json:
            new_entries = []
            for key, value in data_json["published_timestamp"].items():
                if not self.check_pub_time_availability(timestamp=value):
                    new_entries.append(str(value))
            self.insert_timestamp(new_entries=new_entries)

    def check_author_availability(self, author_data_str):
        rows = get_author_sk(self.connection, author_data_str)
        if len(rows) > 0:
            return True
        else:
            return False

    def insert_author(self, new_entries):
        params = [process_author(entry) for entry in new_entries]

        query = """INSERT INTO "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."AUTHOR_DIM"  
                                   (A_AUTHOR_ID, 
                                    A_AUTHOR_F_NAME, 
                                    A_AUTHOR_L_NAME, 
                                    A_AUTHOR_CODE) 
                   VALUES (%(author_id)s, %(f_name)s, %(l_name)s, %(code)s);"""
        execute_insert(query=query, params=params, connection=self.connection, function="insert_author")

    def load_author_data(self, data_json):
        if "author" in data_json:
            new_entries = []
            for key, value in data_json["author"].items():
                if not self.check_author_availability(author_data_str=value):
                    new_entries.append(str(value))

            self.insert_author(new_entries=new_entries)

    def check_subject_availability(self, subject_str):
        rows = get_subject_sk(self.connection, subject_str)
        if len(rows) > 0:
            return True
        else:
            return False

    def insert_subject(self, new_entries):
        params = [{"subject": entry} for entry in new_entries]

        query = """INSERT INTO "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."SUBJECT_DIM" 
                               (S_SUBJECT) 
                        VALUES (%(subject)s);"""
        execute_insert(query=query, params=params, connection=self.connection, function="insert_subject")

    def load_subject_data(self, data_json):
        if "subject" in data_json:
            new_entries = []
            for key, value in data_json["subject"].items():
                if not self.check_subject_availability(subject_str=value):
                    new_entries.append(str(value))
            self.insert_subject(new_entries=new_entries)

    def check_level_availability(self, level_str):
        rows = get_level_sk(self.connection, level_str)
        if len(rows) > 0:
            return True
        else:
            return False

    def insert_level(self, new_entries):

        params = [{"level": entry} for entry in new_entries]

        query = """INSERT INTO "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."LEVEL_DIM" 
                               (L_LEVEL) 
                        VALUES (%(level)s;"""
        execute_insert(query=query, params=params, connection=self.connection, function="insert_level")

    def load_level_data(self, data_json):
        if "level" in data_json:
            new_entries = []
            for key, value in data_json["level"].items():
                if not self.check_level_availability(level_str=value):
                    new_entries.append(str(value))

            self.insert_level(new_entries=new_entries)
