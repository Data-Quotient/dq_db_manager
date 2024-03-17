from dq_db_manager.handlers.base.base_metadata_extractor import BaseMetadataExtractor

class CockroachMetadataExtractor(BaseMetadataExtractor):

    def __init__(self, connection_handler):
        self.connection_handler = connection_handler
    
    # Function to extract table details
    def extract_table_details(self, table_name=None):
        table_query = "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = current_schema()"
        params = []
        
        if table_name:
            table_query += " AND table_name = %s"
            params.append(table_name)

        return self.connection_handler.execute_query(table_query, tuple(params))

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

    # Function to extract view details
    def extract_view_details(self, view_name=None):
        view_query = "SELECT table_name, view_definition FROM information_schema.views WHERE table_schema = current_schema()"
        params = []

        if view_name:
            view_query += " AND table_name = %s"
            params.append(view_name)

        return self.connection_handler.execute_query(view_query, tuple(params))

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

    # ... other metadata extraction met
