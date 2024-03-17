from dq_db_manager.handlers.base.base_metadata_extractor import BaseMetadataExtractor
from typing import List, Union
from datetime import datetime
from dq_db_manager.handlers.base.base_metadata_extractor import BaseMetadataExtractor
from .models import *

class PostgreSQLMetadataExtractor(BaseMetadataExtractor):

    def __init__(self, connection_handler, models):
        self.connection_handler = connection_handler
        self.models = models
    
    # Function to extract table details
    def extract_table_details(self, table_name=None):
        table_query = "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = current_schema()"
        params = []
        
        if table_name:
            table_query += " AND table_name = %s"
            params.append(table_name)

        return self.connection_handler.execute_query(table_query, tuple(params))
    
    def extract_table_details_v2(self, table_name=None, return_as_dict: bool = False) -> Union[List[TableDetail], List[dict]]:
        table_query = "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = current_schema()"
        params = []

        if table_name:
            table_query += " AND table_name = %s"
            params.append(table_name)

        raw_tables = self.connection_handler.execute_query(table_query, tuple(params))
        tables = [self.models.TableDetail(table_name=table[0], table_type=table[1]) for table in raw_tables]

        if return_as_dict:
            return [table.dict() for table in tables]
        else:
            return tables



    # Function to extract column details
    def extract_column_details(self, table_name=None):
        column_query = """
        SELECT table_name, column_name, data_type, column_default, is_nullable
        FROM information_schema.columns
        WHERE table_schema = current_schema()
        """
        params = []
        
        if table_name:
            column_query += " AND table_name = %s"
            params.append(table_name)
        
        return self.connection_handler.execute_query(column_query, tuple(params))
    
    def extract_column_details_v2(self, table_name=None, return_as_dict: bool = False) -> Union[List[ColumnDetail], List[dict]]:
        column_query = """
        SELECT table_name, column_name, data_type, column_default, is_nullable
        FROM information_schema.columns
        WHERE table_schema = current_schema()
        """
        params = []

        if table_name:
            column_query += " AND table_name = %s"
            params.append(table_name)

        raw_columns = self.connection_handler.execute_query(column_query, tuple(params))
        columns = [self.models.ColumnDetail(table_name=column[0], column_name=column[1], data_type=column[2], column_default=column[3], is_nullable=column[4]) for column in raw_columns]

        if return_as_dict:
            return [column.dict() for column in columns]
        else:
            return columns

    # Function to extract constraints details
    def extract_constraints_details(self, table_name=None, constraint_name=None):
        constraints_query = """
        SELECT constraint_name, table_name, constraint_type
        FROM information_schema.table_constraints
        WHERE table_schema = current_schema()
        """
        params = []

        if table_name:
            constraints_query += " AND table_name = %s"
            params.append(table_name)

        if constraint_name:
            constraints_query += " AND constraint_name = %s"
            params.append(constraint_name)

        return self.connection_handler.execute_query(constraints_query, tuple(params))
    
    def extract_constraints_details_v2(self, table_name=None, return_as_dict: bool = False):
        constraints_query = """
        SELECT constraint_name, table_name, constraint_type
        FROM information_schema.table_constraints
        WHERE table_schema = current_schema()
        """
        params = []
        if table_name:
            constraints_query += " AND table_name = %s"
            params.append(table_name)

        raw_constraints = self.connection_handler.execute_query(constraints_query, tuple(params))
        constraints = [self.models.ConstraintDetail(constraint_name=constraint[0], table_name=constraint[1], constraint_type=constraint[2]) for constraint in raw_constraints]

        if return_as_dict:
            return [constraint.dict() for constraint in constraints]
        else:
            return constraints


    # Function to extract index details
    def extract_index_details(self, table_name=None, index_name=None):
        index_query = """
        SELECT indexname, tablename, indexdef
        FROM pg_indexes
        WHERE schemaname = current_schema()
        """
        params = []

        if table_name:
            index_query += " AND tablename = %s"
            params.append(table_name)

        if index_name:
            index_query += " AND indexname = %s"
            params.append(index_name)

        return self.connection_handler.execute_query(index_query, tuple(params))
    
    def extract_index_details_v2(self, table_name=None, index_name=None, return_as_dict: bool = False):
        index_query = """
        SELECT indexname, tablename, indexdef
        FROM pg_indexes
        WHERE schemaname = current_schema()
        """
        params = []
        if table_name:
            index_query += " AND tablename = %s"
            params.append(table_name)
        if index_name:
            index_query += " AND indexname = %s"
            params.append(index_name)

        raw_indexes = self.connection_handler.execute_query(index_query, tuple(params))
        indexes = [self.models.IndexDetail(index_name=index[0], table_name=index[1], index_definition=index[2]) for index in raw_indexes]

        if return_as_dict:
            return [index.dict() for index in indexes]
        else:
            return indexes


    # Function to extract view details
    def extract_view_details(self, view_name=None):
        view_query = "SELECT table_name, view_definition FROM information_schema.views WHERE table_schema = current_schema()"
        params = []

        if view_name:
            view_query += " AND table_name = %s"
            params.append(view_name)

        return self.connection_handler.execute_query(view_query, tuple(params))
    
    def extract_view_details_v2(self, view_name=None, return_as_dict: bool = False):
        view_query = "SELECT table_name, view_definition FROM information_schema.views WHERE table_schema = current_schema()"
        params = []
        if view_name:
            view_query += " AND table_name = %s"
            params.append(view_name)

        raw_views = self.connection_handler.execute_query(view_query, tuple(params))
        views = [self.models.ViewDetail(view_name=view[0], view_definition=view[1]) for view in raw_views]

        if return_as_dict:
            return [view.dict() for view in views]
        else:
            return views


    # Function to extract trigger details
    def extract_trigger_details(self, trigger_name=None):
        trigger_query = """
        SELECT event_object_table, trigger_name, action_statement, action_timing, event_manipulation
        FROM information_schema.triggers
        WHERE event_object_schema = current_schema()
        """
        params = []

        if trigger_name:
            trigger_query += " AND trigger_name = %s"
            params.append(trigger_name)

        return self.connection_handler.execute_query(trigger_query, tuple(params))
    
    def extract_trigger_details_v2(self, table_name=None, return_as_dict: bool = False):
        trigger_query = """
        SELECT event_object_table, trigger_name, action_statement, action_timing, event_manipulation
        FROM information_schema.triggers
        WHERE event_object_schema = current_schema()
        """
        params = []
        if table_name:
            trigger_query += " AND event_object_table = %s"
            params.append(table_name)

        raw_triggers = self.connection_handler.execute_query(trigger_query, tuple(params))
        triggers = [self.models.TriggerDetail(trigger_name=trigger[1], table_name=trigger[0], trigger_event=trigger[2], trigger_condition=trigger[3], trigger_operation=trigger[4]) for trigger in raw_triggers]

        if return_as_dict:
            return [trigger.dict() for trigger in triggers]
        else:
            return triggers
            
    def get_complete_metadata(self):
        # Extract all tables first
        tables = self.extract_table_details_v2(return_as_dict=True)

        # For each table, enrich it with columns, constraints, indexes, and triggers
        for table in tables:
            table_name = table['table_name']
            
            # Extract and set columns for this table
            columns = self.extract_column_details_v2(table_name=table_name, return_as_dict=True)
            table['columns'] = columns
            
            # Extract and set constraints for this table
            constraints = self.extract_constraints_details_v2(table_name=table_name, return_as_dict=True)
            table['constraints'] = constraints
            
            # Extract and set indexes for this table
            indexes = self.extract_index_details_v2(table_name=table_name, return_as_dict=True)
            table['indexes'] = indexes
            
            # Extract and set triggers for this table
            triggers = self.extract_trigger_details_v2(table_name=table_name, return_as_dict=True)
            table['triggers'] = triggers

        # Extract views
        views = self.extract_view_details_v2(return_as_dict=True)

        # Assemble the complete metadata
        complete_metadata = self.models.DataSourceMetadata(
            data_source_id=self.connection_handler.connection_details['database'],
            tables=tables,
            views=views,
            created_at=str(datetime.now()),
            updated_at=str(datetime.now())
        )

        # Serialize for MongoDB
        return complete_metadata.dict()


