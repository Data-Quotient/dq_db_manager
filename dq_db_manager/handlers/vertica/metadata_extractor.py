from dq_db_manager.handlers.base.metadata_extractor import BaseMetadataExtractor

class VerticaMetadataExtractor(BaseMetadataExtractor):

    def __init__(self, connection_handler):
        self.connection_handler = connection_handler
    
    # Function to extract table details
    def extract_table_details(self, table_name=None):
        table_query = "SELECT table_name, 'BASE TABLE' AS table_type FROM v_catalog.tables"
        params = []
        
        if table_name:
            table_query += " AND table_name = %s"
            params.append(table_name)

        return self.connection_handler.execute_query(table_query, tuple(params))

    # Function to extract column details
    def extract_column_details(self, table_name=None):
        column_query = """
        SELECT table_name, column_name, data_type, column_default, is_nullable
        FROM v_catalog.columns
        """
        params = []
        
        if table_name:
            column_query += " AND table_name = %s"
            params.append(table_name)
        
        return self.connection_handler.execute_query(column_query, tuple(params))

    # Function to extract constraints details
    def extract_constraints_details(self, table_name=None, constraint_name=None):
        constraints_query = """
        SELECT cc.constraint_name, cc.table_name, cc.constraint_type
        FROM v_catalog.constraint_columns cc
        JOIN v_catalog.tables t ON cc.table_id = t.table_id
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
        SELECT p.projection_name AS indexname,
            t.table_name AS tablename,
            'Super Projection' AS indexdef
        FROM v_catalog.projections p
        JOIN v_catalog.tables t ON p.anchor_table_id = t.table_id
        WHERE p.is_super_projection = 't'
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
        view_query = """
        SELECT table_name, view_definition 
        FROM v_catalog.views
        """
        params = []

        if view_name:
            view_query += " AND table_name = %s"
            params.append(view_name)

        return self.connection_handler.execute_query(view_query, tuple(params))

    # Function to extract trigger details

    # TODO: In Vertica, there isn't a direct equivalent to 
    # PostgreSQL's information_schema.triggers view as Vertica doesn't 
    # support triggers in the same way. Instead, Vertica provides a mechanism 
    # called Flex Tables for capturing external events and processing them asynchronously.

    # def extract_trigger_details(self, trigger_name=None):
    #     trigger_query = """

    #     """
    #     params = []

    #     if trigger_name:
    #         trigger_query += " AND trigger_name = %s"
    #         params.append(trigger_name)

    #     return self.connection_handler.execute_query(trigger_query, tuple(params))

    # ... other metadata extraction met
