from dq_db_manager.handlers.base.metadata_extractor import BaseMetadataExtractor
from typing import List, Union
from datetime import datetime
from dq_db_manager.handlers.base.metadata_extractor import BaseMetadataExtractor
from dq_db_manager.utils.RDBMSHelper import extract_details, add_table_to_query, add_index_to_query, add_view_to_query, add_trigger_to_query
from dq_db_manager.models.postgres import *

class PostgreSQLMetadataExtractor(BaseMetadataExtractor):

    def __init__(self, connection_handler, models):
        self.connection_handler = connection_handler
        self.models = models
    
    def extract_table_details(self, table_name=None, return_as_dict: bool = False) -> Union[List[TableDetail], List[dict]]:
        table_query = "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = current_schema()"
        params = []
        if table_name:
            table_query, params = add_table_to_query(query=table_query, params=params, table_name=table_name)
        return extract_details(self.connection_handler.execute_query, table_query, TableDetail, return_as_dict, *params)

    def extract_column_details(self, table_name=None, return_as_dict: bool = False) -> Union[List[ColumnDetail], List[dict]]:
        column_query = """
        SELECT column_name, data_type, column_default, is_nullable
        FROM information_schema.columns
        WHERE table_schema = current_schema()
        """
        params = []
        if table_name:
            column_query, params = add_table_to_query(query=column_query, params=params, table_name=table_name)
        return extract_details(self.connection_handler.execute_query, column_query, ColumnDetail, return_as_dict, *params)
    
    def extract_constraints_details(self, table_name=None, return_as_dict: bool = False):
        constraints_query = """
        SELECT constraint_name, table_name, constraint_type
        FROM information_schema.table_constraints
        WHERE table_schema = current_schema()
        """
        params = []
        if table_name:
            constraints_query, params = add_table_to_query(query=constraints_query, params=params, table_name=table_name)
        return extract_details(self.connection_handler.execute_query, constraints_query, ConstraintDetail, return_as_dict, *params)
    
    def extract_index_details(self, table_name=None, index_name=None, return_as_dict: bool = False):
        index_query = """
        SELECT indexname, tablename, indexdef
        FROM pg_indexes
        WHERE schemaname = current_schema()
        """
        params = []
        if table_name:
            index_query, params = add_table_to_query(query=index_query, params=params, table_name=table_name, alias="pg_indexes.tablename")
        if index_name:
            index_query, params = add_index_to_query(query=index_query, params=params, index_name=index_name)
        return extract_details(self.connection_handler.execute_query, index_query, IndexDetail, return_as_dict, *params)

    def extract_view_details(self, view_name=None, return_as_dict: bool = False):
        view_query = "SELECT table_name, view_definition FROM information_schema.views WHERE table_schema = current_schema()"
        params = []
        if view_name:
            view_query, params = add_view_to_query(query=view_query, params=params, view_name=view_name)
        return extract_details(self.connection_handler.execute_query, view_query, ViewDetail, return_as_dict, *params)

    def extract_trigger_details(self, trigger_name=None, table_name=None, return_as_dict: bool = False):
        trigger_query = """
        SELECT event_object_table, trigger_name, action_statement, action_timing, event_manipulation
        FROM information_schema.triggers
        WHERE event_object_schema = current_schema()
        """
        params = []
        if table_name:
            trigger_query, params = add_table_to_query(query=trigger_query, params=params, table_name=table_name, alias="event_object_table")
        if trigger_name:
            trigger_query, params = add_trigger_to_query(query=trigger_query, params=params, trigger_name=trigger_name)
        return extract_details(self.connection_handler.execute_query, trigger_query, TriggerDetail, return_as_dict, *params)

    def get_complete_metadata(self):
        # Extract all tables first
        tables = self.extract_table_details(return_as_dict=True)

        # For each table, enrich it with columns, constraints, indexes, and triggers
        for table in tables:
            table_name = table['table_name']
            
            # Extract and set columns for this table
            columns = self.extract_column_details(table_name=table_name, return_as_dict=True)
            table['columns'] = columns
            
            # Extract and set constraints for this table
            constraints = self.extract_constraints_details(table_name=table_name, return_as_dict=True)
            table['constraints'] = constraints
            
            # Extract and set indexes for this table
            indexes = self.extract_index_details(table_name=table_name, return_as_dict=True)
            table['indexes'] = indexes
            
            # Extract and set triggers for this table
            triggers = self.extract_trigger_details(table_name=table_name, return_as_dict=True)
            table['triggers'] = triggers

        # Extract views
        views = self.extract_view_details(return_as_dict=True)

        # Assemble the complete metadata
        complete_metadata = DataSourceMetadata(
            data_source_id=self.connection_handler.connection_details['database'],
            tables=tables,
            views=views,
            created_at=str(datetime.now()),
            updated_at=str(datetime.now())
        )

        # Serialize for MongoDB
        return dict(complete_metadata.model_dump())


