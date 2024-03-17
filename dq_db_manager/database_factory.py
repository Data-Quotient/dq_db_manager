from dq_db_manager.handlers.postgresql.postgresql_db_handler import PostgreSQLDBHandler
from dq_db_manager.handlers.mysql.mysql_db_handler import MySQLDBHandler
from dq_db_manager.handlers.amazons3.s3_client_handler import S3ClientHandler
from dq_db_manager.handlers.oracle.oracle_db_handler import OracleDBHandler
from dq_db_manager.handlers.base import base_db_handler
# Import other DB handlers as needed

class DatabaseFactory:
    @staticmethod
    def get_database_handler(db_type: str, connection_details:dict) -> base_db_handler:

        db_type = db_type.lower()
        # Match the db_type and instantiate the corresponding DB handler
        if db_type == 'postgresql':
            return PostgreSQLDBHandler(connection_details)
        elif db_type == 'mysql':
            return MySQLDBHandler(connection_details)
        elif db_type == 'amazons3':
            return S3ClientHandler(connection_details)
        elif db_type == 'oracle':
            return OracleDBHandler(connection_details)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
