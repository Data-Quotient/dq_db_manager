from ..base.connection_handler import BaseConnectionHandler
from .connection_details_parser import ConnectionDetailsParser
import mysql.connector

class MySQLConnectionHandler(BaseConnectionHandler):
    def __init__(self, connection_details):
        parser = ConnectionDetailsParser(connection_details)
        parsed_details = parser.parse()
        self.connection = None
        super().__init__(parsed_details)

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.connection_details)
            return self.connection
        except mysql.connector.Error as e:
            print(f"Error connecting to MySQL: {e}")
            raise

    def disconnect(self):
        if self.connection:
            self.connection.close()

    def test_connection(self):
        try:
            self.connect()
            return True
        except mysql.connector.Error as e:
            print(f"Error testing MySQL connection: {e}")
            return False
        finally:
            self.disconnect()

    def execute_query(self, query, params=None):
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, params)  
            results = cursor.fetchall()
            cursor.close()
            return results
        except mysql.connector.Error as e:
            print(f"Error executing MySQL query: {e}")
            return None
        finally:
            self.disconnect()
