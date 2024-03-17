from .maria_connection_handler import MariaConnectionHandler
from .maria_metadata_extractor import MariaMetadataExtractor
from dq_db_manager.handlers.base.base_db_handler import BaseDBHandler

class MariaDBHandler(BaseDBHandler):
    def __init__(self, connection_details):
        self.connection_handler : MariaConnectionHandler = MariaConnectionHandler(connection_details)
        self.metadata_extractor :  MariaMetadataExtractor = MariaMetadataExtractor(self.connection_handler)

    # ... Delegate connection and metadata extraction methods to connection_handler and metadata_extractor ...
