from dq_db_manager.handlers.base.metadata_extractor import BaseMetadataExtractor
from dq_db_manager.handlers.postgresql.metadata_extractor import PostgreSQLMetadataExtractor

class CockroachMetadataExtractor(PostgreSQLMetadataExtractor):
    def __init__(self, connection_handler, models):
        super().__init__(connection_handler, models)