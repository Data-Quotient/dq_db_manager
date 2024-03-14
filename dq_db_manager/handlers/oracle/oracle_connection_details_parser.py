class ConnectionDetailsParser:
    def __init__(self, connection_details):
        self.connection_details = connection_details

    def parse(self):
        # Basic validation or transformation of connection details.
        # This can be extended for specific databases if needed.
        if not self.connection_details.get('user') or not self.connection_details.get('password') or not self.connection_details.get('dsn'):
            raise ValueError("Invalid connection details")
        return self.connection_details
