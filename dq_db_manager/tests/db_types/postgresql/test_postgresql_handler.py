import unittest
from unittest.mock import MagicMock, patch
from dq_db_manager.handlers.postgresql.db_handler import PostgreSQLDBHandler

class TestPostgreSQLDBHandler(unittest.TestCase):
    def setUp(self):
        # Set up connection details
        self.connection_details = {
            'host': 'localhost',
            'database': 'dq_api_test',  # Use a separate test database
            'user': 'bavid',
            'password': 'bavid_api_pwd',
            'port': 5432
        }

        # Initialize the PostgreSQLDBHandler
        self.db_handler = PostgreSQLDBHandler(self.connection_details)

    def test_connection(self):
        # Mock the connection_handler's connect method
        with patch('dq_db_manager.handlers.postgresql.postgresql_connection_handler.PostgreSQLConnectionHandler.connect') as mocked_connect:
            mocked_connect.return_value = True
            self.assertTrue(self.db_handler.test_connection(), "Connection test should return True")

    def test_extract_table_details(self):
        # Mock the metadata_extractor's extract_table_details method
        with patch('dq_db_manager.handlers.postgresql.postgresql_metadata_extractor.PostgreSQLMetadataExtractor.extract_table_details') as mocked_extract:
            mocked_extract.return_value = [{'table_name': 'test_table', 'table_type': 'BASE TABLE', 'engine': 'InnoDB'}]
            table_details = self.db_handler.extract_table_details()
            self.assertEqual(len(table_details), 1, "Should return 1 table")
            self.assertEqual(table_details[0]['table_name'], 'test_table', "Table name should be 'test_table'")

    # ... more test cases for other methods ...

    def tearDown(self):
        # Clean up and close connections if necessary
        pass

if __name__ == '__main__':
    unittest.main()
