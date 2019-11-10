import dataclasses
from pathlib import Path
import re
import yaml


@dataclasses.dataclass
class CMIP6Config:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            print(k, v)
            setattr(self, k, v)

    def __str__(self):
        return f'<{self.__class__.__name__} {hash(self)}>'

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(str(self.cmip6restapi_url)+str(self.base_data_dir))

    @classmethod
    def create_from_yaml(cls, config_file):
        config = cls(**yaml.load(open(config_file, 'r'), Loader=yaml.Loader))
        config.base_data_dir = Path(config.base_data_dir)
        config.base_data_dir.mkdir(parents=True, exist_ok=True)
        return config
