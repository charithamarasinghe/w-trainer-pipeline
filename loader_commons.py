from commons import execute_select


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
