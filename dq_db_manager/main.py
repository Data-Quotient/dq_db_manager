from dq_db_manager.handlers.vertica.db_handler import VerticaHandler

connection_details = {'host': 'localhost',
             'port': 5433,
             'user': 'dbadmin',
             'password': 'password',
             }
# connection_details = {'host': 'dune-wren-4130.7s5.aws-ap-south-1.cockroachlabs.cloud', 'port': 26257, 'user': 'test', 'password':'n9MKOp_tD4UURQfmvH50VQ', 'database': 'defaultdb'}

handler = VerticaHandler(connection_details)
print(handler.metadata_extractor.get_complete_metadata())