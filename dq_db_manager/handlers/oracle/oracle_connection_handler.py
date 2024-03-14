from dq_db_manager.handlers.base.base_connection_handler import BaseConnectionHandler
from .oracle_connection_details_parser import ConnectionDetailsParser
import oracledb

class OracleConnectionHandler(BaseConnectionHandler):
    def __init__(self, connection_details):
        parser = ConnectionDetailsParser(connection_details)
        parsed_details = parser.parse()
        self.connection = None
        super().__init__(parsed_details)

    def connect(self):
        try:
            self.connection = oracledb.connect(**self.connection_details)
            print(f"Successfully connected to {self.connection_details}")
            return self.connection
        except oracledb.Error as e:
            print(f"Error connecting to PostgreSQL: {e}")
            return None

    def disconnect(self):
        if self.connection:
            self.connection.close()

    def test_connection(self):
        try:
            
            self.connect()
            return True
        except oracledb.Error as e:
            print(f"Error testing PostgreSQL connection: {e}")
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
        except oracledb.Error as e:
            print(f"Error executing PostgreSQL query: {e}")
            return None
        finally:
            self.disconnect()
