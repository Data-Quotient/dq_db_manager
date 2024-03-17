from dq_db_manager.handlers.vertica.vertica_db_handler import VerticaHandler

# jdbc:vertica://localhost:5433
connection_details = {'host': 'localhost',
             'port': 5433,
             'user': 'dbadmin',
             'password': 'password',
             }
# connection_details = {'host': 'dune-wren-4130.7s5.aws-ap-south-1.cockroachlabs.cloud', 'port': 26257, 'user': 'test', 'password':'n9MKOp_tD4UURQfmvH50VQ', 'database': 'defaultdb'}

handler = VerticaHandler(connection_details)

print(handler.metadata_extractor.extract_table_details())


