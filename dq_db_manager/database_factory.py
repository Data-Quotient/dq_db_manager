from dq_db_manager.handlers.postgresql.postgresql_db_handler import PostgreSQLDBHandler
from dq_db_manager.handlers.mysql.mysql_db_handler import MySQLDBHandler
# Import other DB handlers as needed

class DatabaseFactory:
    @staticmethod
    def get_database_handler(db_type, connection_details):
        # Match the db_type and instantiate the corresponding DB handler
        if db_type == 'postgresql':
            return PostgreSQLDBHandler(connection_details)
        elif db_type == 'mysql':
            return MySQLDBHandler(connection_details)
        # Add other database types here as elif statements
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
