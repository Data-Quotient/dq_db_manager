from dq_db_manager.handlers.mysql.db_handler import MySQLDBHandler

connection_details = {'host': 'viaduct.proxy.rlwy.net',
             'port': 56414,
             'user': 'root',
             'password': 'eYHRwZgoMeQEPDErMvMFGBmbEIJoKMAL',
             'database': 'railway'
             }
# connection_details = {'host': 'dune-wren-4130.7s5.aws-ap-south-1.cockroachlabs.cloud', 'port': 26257, 'user': 'test', 'password':'n9MKOp_tD4UURQfmvH50VQ', 'database': 'defaultdb'}

handler = MySQLDBHandler(connection_details)

obj = handler.metadata_extractor.get_complete_metadata()
print(obj)
