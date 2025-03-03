from .config import db_config

database_cfg = db_config["database"]
datasource_cfg = db_config["datasource"]

class DatabaseConnection:
    
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, host=None, port=None, username=None, password=None, data_path=None, factor_path=None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.data_path = data_path
        self.factor_path = factor_path
    
    def connect(self):
        return f"Connecting to database at {self.host}:{self.port} with user {self.username} and path {self.data_path}."


class ConnectionBuilder:
    def __init__(self, host=None, port=None, username=None, password=None, **kwargs):
        self._config = {
            "host": host,
            "port": port,
            "username": username,
            "password": password
        }
    
    def set_data_file_path(self, file_path):
        self._config["data_path"] = file_path
        return self
    
    def set_factor_file_path(self, file_path):
        self._config["factor_path"] = file_path
        return self
    
    def build(self):
        return DatabaseConnection(**self._config)


def connection_factory(db_type):
    builder = ConnectionBuilder(**database_cfg[db_type])
    match db_type:
        case "mongodb":
            pass
        case "duckdb":
            builder.set_data_file_path(database_cfg[db_type]["data_path"])\
                   .set_factor_file_path(database_cfg[db_type]["factor_path"])
        case "chdb":
            builder.set_data_file_path(database_cfg[db_type]["data_path"])\
                   .set_factor_file_path(database_cfg[db_type]["factor_path"])
        case _:
            raise NotImplementedError(f"Database {db_type} not implemented yet.")
    return builder.build()