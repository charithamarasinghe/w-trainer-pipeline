from commons import snowflake_connection, execute_insert
from loader_commons import get_timestamp_sk, get_author_sk, get_subject_sk, get_level_sk


class FactLoader:
    def __init__(self, connection):
        self.connection = snowflake_connection(connection=connection)

    def insert_course_publication(self, new_entries):
        query = """INSERT INTO "DB_COURSE_PUBLICATIONS"."SC_COURSE_PUBLICATIONS"."COURSE_PUBLICATIONS"  
                                (CP_COURSE_ID, CP_AUTHOR_SK, CP_PUBLISHED_TIMESTAMP_SK, CP_SUBJECT_SK,
                                 CP_IS_PAID, CP_PRICE, CP_NUM_LECTURES, CP_NUM_REVIEWS, CP_NUM_SUBS,
                                 CP_CONTENT_DURATION, CP_LEVEL_SK ,CP_COURSE_TITLE) 
                             VALUES 
                                 (%(course_id)s, %(author_sk)s, %(timestamp_sk)s, %(subject_sk)s, %(is_paid)s, %(price)s, 
                                 %(num_lectures)s, %(num_reviews)s, %(num_subscribers)s, %(cont_dur)s, %(level_sk)s,
                                  %(course_title)s)"""
        execute_insert(query=query, params=new_entries, connection=self.connection,
                       function="insert_course_publication")

    def update_course_publications(self, data_json):
        new_entries = []
        for key, value in data_json["course_id"].items():
            course_data = {
                "course_id": str(value),
                "author_sk": get_author_sk(connection=self.connection,
                                           author_id_str=data_json["author"][str(key)])[0][0],
                "timestamp_sk": get_timestamp_sk(connection=self.connection,
                                                 timestamp_str=data_json["published_timestamp"][key])[0][0],
                "subject_sk": get_subject_sk(connection=self.connection,
                                             subject_str=data_json["subject"][str(key)])[0][0],
                "is_paid": data_json["is_paid"][str(key)],
                "price": data_json["price"][str(key)],
                "num_lectures": data_json["num_lectures"][str(key)],
                "num_reviews": data_json["num_reviews"][str(key)],
                "num_subscribers": data_json["num_subscribers"][str(key)],
                "cont_dur": data_json["content_duration"][str(key)],
                "level_sk": get_level_sk(connection=self.connection, level_str=data_json["level_sk"][str(key)])[0][0],
                "course_title": data_json["course_title"][str(key)]
            }
            new_entries.append(course_data)

        self.insert_course_publication(new_entries=new_entries)
