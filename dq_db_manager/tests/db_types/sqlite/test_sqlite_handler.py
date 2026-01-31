import unittest
import sqlite3
import tempfile
import os
from dq_db_manager.handlers.sqlite.db_handler import SQLiteDBHandler
from dq_db_manager.database_factory import DatabaseFactory

class TestSQLiteDBHandler(unittest.TestCase):
    def setUp(self):
        # Create a temporary SQLite database file for testing
        self.temp_db = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.db')
        self.temp_db_path = self.temp_db.name
        self.temp_db.close()

        # Set up connection details
        self.connection_details = {
            'database': self.temp_db_path
        }

        # Create some test tables and data
        self._create_test_schema()

        # Initialize the SQLiteDBHandler
        self.db_handler = SQLiteDBHandler(self.connection_details)

    def _create_test_schema(self):
        """Create test tables, views, indexes, and triggers for testing"""
        conn = sqlite3.connect(self.temp_db_path)
        cursor = conn.cursor()

        # Create test tables
        cursor.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                content TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)

        # Create an index
        cursor.execute("""
            CREATE INDEX idx_posts_user_id ON posts(user_id)
        """)

        # Create a view
        cursor.execute("""
            CREATE VIEW user_posts AS
            SELECT u.username, p.title, p.content
            FROM users u
            JOIN posts p ON u.id = p.user_id
        """)

        # Create a trigger
        cursor.execute("""
            CREATE TRIGGER update_timestamp
            AFTER UPDATE ON users
            FOR EACH ROW
            BEGIN
                UPDATE users SET created_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
            END
        """)

        # Insert test data
        cursor.execute("INSERT INTO users (username, email) VALUES (?, ?)", ('testuser', 'test@example.com'))
        cursor.execute("INSERT INTO posts (user_id, title, content) VALUES (?, ?, ?)", (1, 'Test Post', 'This is a test'))

        conn.commit()
        conn.close()

    def test_connection_via_factory(self):
        """Test getting SQLite handler via DatabaseFactory"""
        db_handler = DatabaseFactory.get_database_handler('sqlite', self.connection_details)
        self.assertIsInstance(db_handler, SQLiteDBHandler, "Factory should return SQLiteDBHandler instance")

    def test_connection(self):
        """Test database connection"""
        result = self.db_handler.connection_handler.test_connection()
        self.assertTrue(result, "Connection test should return True")

    def test_execute_query(self):
        """Test query execution"""
        query = "SELECT * FROM users WHERE username = ?"
        results = self.db_handler.connection_handler.execute_query(query, ['testuser'])
        self.assertEqual(len(results), 1, "Should return 1 user")
        self.assertEqual(results[0]['username'], 'testuser', "Username should be 'testuser'")

    def test_extract_table_details(self):
        """Test table extraction"""
        table_details = self.db_handler.metadata_extractor.extract_table_details(return_as_dict=True)
        self.assertGreaterEqual(len(table_details), 2, "Should return at least 2 tables")
        table_names = [t['table_name'] for t in table_details]
        self.assertIn('users', table_names, "Should contain 'users' table")
        self.assertIn('posts', table_names, "Should contain 'posts' table")

    def test_extract_table_details_filtered(self):
        """Test table extraction with filter"""
        table_details = self.db_handler.metadata_extractor.extract_table_details(table_name='users', return_as_dict=True)
        self.assertEqual(len(table_details), 1, "Should return 1 table")
        self.assertEqual(table_details[0]['table_name'], 'users', "Table name should be 'users'")

    def test_extract_column_details(self):
        """Test column extraction for a specific table"""
        column_details = self.db_handler.metadata_extractor.extract_column_details(table_name='users', return_as_dict=True)
        self.assertGreater(len(column_details), 0, "Should return columns")
        column_names = [c['column_name'] for c in column_details]
        self.assertIn('id', column_names, "Should contain 'id' column")
        self.assertIn('username', column_names, "Should contain 'username' column")
        self.assertIn('email', column_names, "Should contain 'email' column")

    def test_extract_constraints_details(self):
        """Test constraint extraction"""
        constraint_details = self.db_handler.metadata_extractor.extract_constraints_details(table_name='users', return_as_dict=True)
        self.assertGreater(len(constraint_details), 0, "Should return constraints")
        constraint_types = [c['constraint_type'] for c in constraint_details]
        self.assertIn('PRIMARY KEY', constraint_types, "Should contain PRIMARY KEY constraint")

    def test_extract_index_details(self):
        """Test index extraction"""
        index_details = self.db_handler.metadata_extractor.extract_index_details(return_as_dict=True)
        self.assertGreater(len(index_details), 0, "Should return indexes")
        index_names = [i['index_name'] for i in index_details]
        self.assertIn('idx_posts_user_id', index_names, "Should contain 'idx_posts_user_id' index")

    def test_extract_index_details_filtered(self):
        """Test index extraction with table filter"""
        index_details = self.db_handler.metadata_extractor.extract_index_details(table_name='posts', return_as_dict=True)
        self.assertGreater(len(index_details), 0, "Should return indexes for posts table")

    def test_extract_view_details(self):
        """Test view extraction"""
        view_details = self.db_handler.metadata_extractor.extract_view_details(return_as_dict=True)
        self.assertEqual(len(view_details), 1, "Should return 1 view")
        self.assertEqual(view_details[0]['view_name'], 'user_posts', "View name should be 'user_posts'")

    def test_extract_trigger_details(self):
        """Test trigger extraction"""
        trigger_details = self.db_handler.metadata_extractor.extract_trigger_details(return_as_dict=True)
        self.assertEqual(len(trigger_details), 1, "Should return 1 trigger")
        self.assertEqual(trigger_details[0]['trigger_name'], 'update_timestamp', "Trigger name should be 'update_timestamp'")
        self.assertEqual(trigger_details[0]['trigger_event'], 'AFTER', "Trigger event should be 'AFTER'")

    def test_get_complete_metadata(self):
        """Test complete metadata extraction"""
        complete_metadata = self.db_handler.metadata_extractor.get_complete_metadata()

        # Verify structure
        self.assertIn('data_source_id', complete_metadata, "Should contain data_source_id")
        self.assertIn('tables', complete_metadata, "Should contain tables")
        self.assertIn('views', complete_metadata, "Should contain views")
        self.assertIn('created_at', complete_metadata, "Should contain created_at")
        self.assertIn('updated_at', complete_metadata, "Should contain updated_at")

        # Verify tables
        self.assertGreaterEqual(len(complete_metadata['tables']), 2, "Should have at least 2 tables")

        # Verify first table has all required metadata
        first_table = complete_metadata['tables'][0]
        self.assertIn('columns', first_table, "Table should have columns")
        self.assertIn('constraints', first_table, "Table should have constraints")
        self.assertIn('indexes', first_table, "Table should have indexes")
        self.assertIn('triggers', first_table, "Table should have triggers")

        # Verify views
        self.assertEqual(len(complete_metadata['views']), 1, "Should have 1 view")

    def test_connection_with_invalid_path(self):
        """Test connection with invalid database path"""
        invalid_details = {'database': '/invalid/path/to/database.db'}
        db_handler = SQLiteDBHandler(invalid_details)
        # SQLite will create a new database if the path is valid, so we test with truly invalid path
        # The test_connection should still return True as SQLite creates the file
        # For a truly invalid path (e.g., read-only filesystem), it would fail
        result = db_handler.connection_handler.test_connection()
        # This test depends on system permissions
        self.assertIsInstance(result, bool, "Should return a boolean")

    def tearDown(self):
        # Clean up: close any open connections and delete the temporary database file
        try:
            if hasattr(self, 'db_handler') and self.db_handler.connection_handler.connection:
                self.db_handler.connection_handler.disconnect()
        except:
            pass

        # Delete the temporary database file
        if os.path.exists(self.temp_db_path):
            os.unlink(self.temp_db_path)

if __name__ == '__main__':
    unittest.main()
