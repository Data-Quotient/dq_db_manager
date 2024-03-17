from dq_db_manager.handlers.maria.maria_db_handler import MariaDBHandler

connection_details = {'host': 'viaduct.proxy.rlwy.net', 'port': '32534', 'user': 'railway', 'password':'UkCllrY~cxeWYhXT0hR2-cY7IjbHOgH4', 'database': 'railway'}

handler = MariaDBHandler(connection_details)

print(handler.metadata_extractor.extract_trigger_details())


