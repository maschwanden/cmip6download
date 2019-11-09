import dataclasses
from pathlib import Path
import re
import yaml


@dataclasses.dataclass
class CMIP6Config:
    cmip6restapi_url: str
    base_data_dir: Path
    dir_filename_regex: list

    sqlite_file: str = None
    queries: int = None
    query_yaml_file: Path = None
    max_download_attempts: int = 2
    n_worker: int = 5

    def __str__(self):
        return f'<{self.__class__.__name__} {hash(self)}>'

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(str(self.cmip6restapi_url)+str(self.base_data_dir))

    @classmethod
    def create_from_yaml(cls, config_file):
        config = cls(**yaml.load(open(config_file, 'r'), Loader=yaml.Loader))
        config.queries = None
        if config.query_yaml_file is not None:
            config.queries = CMIP6SearchQuery.create_from_yaml(
                config.query_yaml_file)
        config.dir_filename_regex = [
            [(k, re.compile(v)) for k, v in regex_dict.items()]
            for regex_dict in config.dir_filename_regex
            ]
        config.base_data_dir = Path(config.base_data_dir)
        config.base_data_dir.mkdir(parents=True, exist_ok=True)
        return config
