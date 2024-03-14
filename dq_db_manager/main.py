from dq_db_manager.handlers.postgresql.postgresql_db_handler import PostgreSQLDBHandler
# from dq_db_manager.handlers.amazons3.s3_client_handler import S3ClientHandler

connection_details = {
            'port': '5432',
            'password': 'bavid_api_pwd',  # Use a separate test database
            'database': 'dq_api_db',
            'host': 'localhost',
            'user': 'bavid'
        }


handler = PostgreSQLDBHandler(connection_details)

print(handler.metadata_extractor.extract_table_details_v2())
print(handler.metadata_extractor.extract_column_details_v2())