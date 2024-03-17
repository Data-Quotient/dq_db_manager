from .vertica_connection_handler import VerticaConnectionHandler
from .vertica_metadata_extractor import VerticaMetadataExtractor
from dq_db_manager.handlers.base.base_db_handler import BaseDBHandler

class VerticaHandler(BaseDBHandler):
    def __init__(self, connection_details):
        self.connection_handler : VerticaConnectionHandler = VerticaConnectionHandler(connection_details)
        self.metadata_extractor :  VerticaMetadataExtractor = VerticaMetadataExtractor(self.connection_handler)

    # ... Delegate connection and metadata extraction methods to connection_handler and metadata_extractor ...
