import clickhouse_connect
import os
import datetime
import time
from clickhouse_connect import common
from dotenv import load_dotenv
load_dotenv()
CH_HOST = os.environ["CH_HOST"]
CH_DATABASE = os.environ["CH_DATABASE"]
CH_USERNAME = os.environ["CH_USERNAME"]
CH_PASSWORD = os.environ["CH_PASSWORD"]


class Clickhouse:
    def __init__(self, init):
        common.set_setting('autogenerate_session_id', False)
        if init == True:
            self.init_connection = clickhouse_connect.get_client(
                host=CH_HOST, username=CH_USERNAME, password=CH_PASSWORD, database=None)
            self.init_connection.command(
                f"DROP DATABASE IF EXISTS {CH_DATABASE}")
            self.init_connection.command(f"CREATE DATABASE {CH_DATABASE}")
            self.init_connection.close()
        self.connection = clickhouse_connect.get_client(
            host=CH_HOST, username=CH_USERNAME, password=CH_PASSWORD, database=CH_DATABASE)

    def init_table(self, table_name, columns):
        columns_string = ""
        for column in columns:
            if column == "tei":
                columns_string += f"{column} String,"
            elif column == "psi":
                columns_string += f"{column} String,"
            else:
                columns_string += f"{column} Nullable(String),"

        self.connection.command(f"DROP TABLE IF EXISTS {table_name}")
        command_query = f"""CREATE TABLE {table_name}
        ({columns_string})
        ENGINE MergeTree
        PRIMARY KEY psi
        """
        self.connection.command(command_query)

    def insert_rows(self, table_name, columns, rows):
        data = []
        for row in rows:
            newEvent = []
            values = list(row.values())
            for value in values:
                current_type = type(value)
                if current_type is datetime.datetime:
                    newEvent.append(value.strftime("%Y-%m-%d"))
                elif value is None:
                    newEvent.append(value)
                else:
                    newEvent.append(str(value))
            data.append(newEvent)
        self.connection.insert(table_name, data, columns)

    def close_connection(self):
        self.connection.close()
