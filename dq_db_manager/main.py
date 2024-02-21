from dq_db_manager.handlers.postgresql.postgresql_db_handler import PostgreSQLDBHandler

connection_details = {
            'host': 'localhost',
            'database': 'dq_api',  # Use a separate test database
            'user': 'bavid',
            'password': 'bavid_api_pwd',
            'port': 5432
        }


handler = PostgreSQLDBHandler(connection_details)

handler = handler.connection_handler.test_connection()
