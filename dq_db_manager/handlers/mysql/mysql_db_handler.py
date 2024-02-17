# MySQLDBHandler and PostgreSQLDBHandler would look similar
# Here's an example for MySQLDBHandler
from .mysql_connection_handler import MySQLConnectionHandler
from .mysql_metadata_extractor import MySQLMetadataExtractor
from ..base.base_db_handler import BaseDBHandler

class MySQLDBHandler(BaseDBHandler):
    def __init__(self, connection_details):
        self.connection_handler = MySQLConnectionHandler(connection_details)
        self.metadata_extractor = MySQLMetadataExtractor(self.connection_handler)
