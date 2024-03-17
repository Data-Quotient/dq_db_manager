from .cockroach_connection_handler import CockroachConnectionHandler
from .cockroach_metadata_extractor import CockroachMetadataExtractor
from dq_db_manager.handlers.base.base_db_handler import BaseDBHandler

class CockroachHandler(BaseDBHandler):
    def __init__(self, connection_details):
        self.connection_handler : CockroachConnectionHandler = CockroachConnectionHandler(connection_details)
        self.metadata_extractor :  CockroachMetadataExtractor = CockroachMetadataExtractor(self.connection_handler)

    # ... Delegate connection and metadata extraction methods to connection_handler and metadata_extractor ...
