
from config import base_path, logger
from commons import snowflake_connection


def get_sql_commands():
    file = open(base_path + "/resources/database/database.sql", 'r')
    sql_file = file.read()
    file.close()

    return [command.replace("\n", "") for command in sql_file.split(';')]


class ValidateSchema:
    def __init__(self, connection):
        self.connection = snowflake_connection(connection=connection)

    def validate_schema(self):
        commands = get_sql_commands()

        failed_trails_count = 0
        for command in commands:
            cursor = self.connection.cursor()
            try:
                cursor.execute(command)
            except Exception as error:
                failed_trails_count = failed_trails_count + 1
                logger.error("validate_schema: {error}".format(error=str(error)))
            cursor.close()

        print("Failed attempts: {attempts}".format(attempts=failed_trails_count))
