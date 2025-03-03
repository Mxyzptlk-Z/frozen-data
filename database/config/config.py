import os
from typing import Any, Dict
from dataclasses import dataclass, field
from ...basis import yamler

@dataclass
class DatabaseConfig:
    db_config: Dict[str, Any] = field(init=False)

    def __post_init__(self):
        self.proj_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        db_config = yamler(f"{self.proj_path}/database/config/db_config.yaml").get_all_fields()
        self.db_config = self.replace_proj_path(db_config)
    
    def replace_proj_path(self, config: Any) -> Any:
        # NOTE: python introduced match-case statement in version 3.10
        match config:
            case dict():
                return {k: self.replace_proj_path(v) for k, v in config.items()}
            case list():
                return [self.replace_proj_path(i) for i in config]
            case str():
                return config.replace("$proj_path$", self.proj_path)
            case others:
                return config
    
    # for python version < 3.10
    # def replace_proj_path(self, config: Any) -> Any:
    #     if isinstance(config, dict):
    #         return {k: self.replace_proj_path(v) for k, v in config.items()}
    #     elif isinstance(config, list):
    #         return [self.replace_proj_path(i) for i in config]
    #     elif isinstance(config, str):
    #         return config.replace('$proj_path$', self.proj_path)
    #     else:
    #         return config
    
    def format_config_str(self, config_str):
        config_str = config_str.replace("$proj_path$", self.proj_path)
        return config_str

db_config = DatabaseConfig().db_config
