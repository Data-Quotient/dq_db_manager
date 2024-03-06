# from dq_db_manager.handlers.postgresql.postgresql_db_handler import PostgreSQLDBHandler
from dq_db_manager.handlers.s3.s3_client_handler import S3ClientHandler

connection_details = {
            'bucket_name': 'blank',
            'access_key_id': 'blank',  # Use a separate test database
            'secret_access_key': 'blanke',
            'region': 'blank',
        }


handler = S3ClientHandler(connection_details)

handler = handler.connection_handler.test_connection()
