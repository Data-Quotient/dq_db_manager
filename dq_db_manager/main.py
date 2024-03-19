from dq_db_manager.handlers.cockroach.db_handler import CockroachHandler

connection_details = {'host': 'dyed-climber-4145.7s5.aws-ap-south-1.cockroachlabs.cloud',
             'port': 26257,
             'user': 'test',
             'password': 'cAiPK-JTnTK-6hP65Wj-7A',
             'database': 'defaultdb'
             }
# connection_details = {'host': 'dune-wren-4130.7s5.aws-ap-south-1.cockroachlabs.cloud', 'port': 26257, 'user': 'test', 'password':'n9MKOp_tD4UURQfmvH50VQ', 'database': 'defaultdb'}

handler = CockroachHandler(connection_details)

obj = handler.metadata_extractor.get_complete_metadata()
print(obj)
