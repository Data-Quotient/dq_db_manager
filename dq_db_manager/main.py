from dq_db_manager.handlers.oracle.db_handler import OracleDBHandler

connection_details = {'host': 'localhost',
             'port': 1522,
             'user': 'new_user',
             'password': 'password',
             'service_name': 'XEPDB1'
             }
# connection_details = {'host': 'dune-wren-4130.7s5.aws-ap-south-1.cockroachlabs.cloud', 'port': 26257, 'user': 'test', 'password':'n9MKOp_tD4UURQfmvH50VQ', 'database': 'defaultdb'}

handler = OracleDBHandler(connection_details)
print(handler.metadata_extractor.get_complete_metadata())