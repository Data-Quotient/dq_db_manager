from .connection_handler import OracleConnectionHandler
from .metadata_extractor import OracleMetadataExtractor
from dq_db_manager.handlers.base.db_handler import BaseDBHandler

class OracleDBHandler(BaseDBHandler):
    def __init__(self, connection_details):
        self.connection_handler : OracleConnectionHandler = OracleConnectionHandler(connection_details)
        self.metadata_extractor :  OracleMetadataExtractor = OracleMetadataExtractor(self.connection_handler)

    # ... Delegate connection and metadata extraction methods to connection_handler and metadata_extractor ...
