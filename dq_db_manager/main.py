from dq_db_manager.handlers.oracle.oracle_db_handler import OracleDBHandler

connection_details = {
    'user': 'bavid',
    'password': 'password',
    'dsn': 'localhost/FREEPDB1'
}

handler = OracleDBHandler(connection_details)

handler = handler.connection_handler.test_connection()
