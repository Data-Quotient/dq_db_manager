from dq_db_manager.handlers.oracle.oracle_db_handler import OracleDBHandler

connection_details = {'user': 'test', 'password': 'password', 'dsn': '127.0.0.1:1522/XEPDB1'}
connection_details_1 = {'user': 'test', 'password': 'password', 'host':'localhost', 'port':1522, 'service_name':'XEPDB1'}

handler = OracleDBHandler(connection_details_1)

print(handler.metadata_extractor.extract_table_details())


