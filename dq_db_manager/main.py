from dq_db_manager.handlers.postgresql.postgresql_db_handler import PostgreSQLDBHandler

connection_details = {'host': 'viaduct.proxy.rlwy.net',
             'port': 32580,
             'user': 'postgres',
             'password': 'rBzkWKsGyhvDrMXHWpnOuOvBbqZzfgEh',
             'database': 'railway'
             }
# connection_details = {'host': 'dune-wren-4130.7s5.aws-ap-south-1.cockroachlabs.cloud', 'port': 26257, 'user': 'test', 'password':'n9MKOp_tD4UURQfmvH50VQ', 'database': 'defaultdb'}

handler = PostgreSQLDBHandler(connection_details)

obj = handler.metadata_extractor.get_complete_metadata()
print(obj)
