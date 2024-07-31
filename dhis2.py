from dotenv import load_dotenv
import psycopg2
import os
load_dotenv()
PSQL_HOST = os.environ["PSQL_HOST"]
PSQL_DATABASE = os.environ["PSQL_DATABASE"]
PSQL_USERNAME = os.environ["PSQL_USERNAME"]
PSQL_PASSWORD = os.environ["PSQL_PASSWORD"]


class Dhis2:
    def __init__(self):
        self.connection = psycopg2.connect(
            f"dbname='{PSQL_DATABASE}' user='{PSQL_USERNAME}' host='{PSQL_HOST}' password='{PSQL_PASSWORD}'")

    def __generate_row(self, columns, rows):
        current_rows = []
        for row in rows:
            newRow = {}
            for i in range(len(columns)):
                newRow[columns[i]] = row[i]
            current_rows.append(newRow)
        return current_rows

    def populate_art_program_table(self, ch, table_name, columns):
        columns_string = ""
        for column in columns:
            columns_string += f"""\"{column}\","""
        columns_string = columns_string[:-1]
        limit = 100000
        offset = 0
        flag = True

        while flag:
            cursor = self.connection.cursor()
            cursor.execute(
                f"""SELECT {columns_string} FROM analytics_event_d4iyh8wuz3l where ps = 'czh5YyTTro8' ORDER BY psi LIMIT %s OFFSET %s;""", (limit, offset))
            result = cursor.fetchall()
            rows = self.__generate_row(columns, result)
            ch.insert_rows(table_name, columns, rows)
            if not result:
                flag = False
                cursor.close()
            else:
                offset += limit
                print(offset)
