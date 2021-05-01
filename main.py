import json
import sys
import os

base_path = os.getcwd()
sys.path.insert(1, base_path)

from commons import snowflake_connection
from dimension_loader import DimensionLoader
from validate_schema_structure import ValidateSchema
from fact_loader import FactLoader


def main():
    file = open(base_path + '/data/course_data.json')
    data = json.load(file)

    conn = snowflake_connection()

    vs = ValidateSchema(conn)
    vs.validate_schema()

    dl = DimensionLoader(conn)
    dl.load_author_data(data_json=data)
    dl.load_level_data(data_json=data)
    dl.load_pub_time_data(data_json=data)
    dl.load_subject_data(data_json=data)

    fl = FactLoader(conn)
    fl.update_course_publications(data_json=data)


if __name__ == "__main__":
    main()
