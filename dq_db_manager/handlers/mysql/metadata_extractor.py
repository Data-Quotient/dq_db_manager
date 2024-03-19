from ..base.metadata_extractor import BaseMetadataExtractor

class MySQLMetadataExtractor(BaseMetadataExtractor):
    def extract_table_details(self, table_name=None):
        table_query = "SELECT table_name, table_type, engine FROM information_schema.tables WHERE table_schema = %s"
        params = [self.connection_handler.db_name]
        
        if table_name:
            table_query += " AND table_name = %s"
            params.append(table_name)

        return self.connection_handler.execute_query(table_query, tuple(params))



    def extract_column_details(self, table_name=None):
        column_query = """
        SELECT table_name, column_name, data_type, column_type
        FROM information_schema.columns
        WHERE table_schema = %s
        """
        params = [self.connection_handler.db_name]
        
        if table_name:
            column_query += " AND table_name = %s"
            params.append(table_name)
        
        return self.connection_handler.execute_query(column_query, tuple(params))



    def extract_constraints_details(self, table_name=None, constraint_name=None):
        constraints_query = """
        SELECT table_name, column_name, constraint_name, referenced_table_name, referenced_column_name
        FROM information_schema.key_column_usage
        WHERE table_schema = %s
        """
        params = [self.connection_handler.db_name]

        if table_name:
            constraints_query += " AND table_name = %s"
            params.append(table_name)

        if constraint_name:
            constraints_query += " AND constraint_name = %s"
            params.append(constraint_name)

        return self.connection_handler.execute_query(constraints_query, tuple(params))

    def extract_index_details(self, table_name=None, index_name=None):
        index_query = """
        SELECT table_name, index_name, non_unique, column_name
        FROM information_schema.statistics
        WHERE table_schema = %s
        """
        params = [self.connection_handler.db_name]

        if table_name:
            index_query += " AND table_name = %s"
            params.append(table_name)

        if index_name:
            index_query += " AND index_name = %s"
            params.append(index_name)

        return self.connection_handler.execute_query(index_query, tuple(params))

    def extract_view_details(self, view_name=None):
        view_query = "SELECT table_name, view_definition FROM information_schema.views WHERE table_schema = %s"
        params = [self.connection_handler.db_name]

        if view_name:
            view_query += " AND table_name = %s"
            params.append(view_name)

        return self.connection_handler.execute_query(view_query, tuple(params))

    def extract_trigger_details(self, trigger_name=None):
        trigger_query = """
        SELECT trigger_name, event_object_table, event_manipulation, action_statement, action_timing
        FROM information_schema.triggers
        WHERE event_object_schema = %s
        """
        params = [self.connection_handler.db_name]

        if trigger_name:
            trigger_query += " AND trigger_name = %s"
            params.append(trigger_name)

        return self.connection_handler.execute_query(trigger_query, tuple(params))