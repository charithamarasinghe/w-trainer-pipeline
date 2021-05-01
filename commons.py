import snowflake.connector as snow
import os

from config import config_params, logger, base_path


def create_snowflake_connection():
    try:
        connection = snow.connect(user=config_params["DATABASE"]["user"],
                                  password=config_params["DATABASE"]["password"],
                                  account=config_params["DATABASE"]["account"])
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
        params = params if isinstance(params, list) else [params]

        connection = snowflake_connection(connection=connection)
        cursor = connection.cursor()
        cursor.executemany(query, params)
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


