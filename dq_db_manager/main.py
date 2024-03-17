from dq_db_manager.handlers.cockroach.cockroach_db_handler import CockroachHandler

connection_details = {'host': 'dune-wren-4130.7s5.aws-ap-south-1.cockroachlabs.cloud', 'port': 26257, 'user': 'test', 'password':'n9MKOp_tD4UURQfmvH50VQ', 'database': 'defaultdb'}

handler = CockroachHandler(connection_details)

print(handler.metadata_extractor.extract_trigger_details())


