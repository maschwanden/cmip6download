from pathlib import Path

from cmip6download import core, helper


def get_config(config_file):
    return core.CMIP6Config.create_from_yaml(config_file)


def get_queries(query_yaml_file):
    return core.CMIP6SearchQuery.create_from_yaml(query_yaml_file)


CONFIG_DIR = Path.home() / '.config/cmip6download/'
CONFIG_FILE = CONFIG_DIR / 'config.yaml'
CONFIG = get_config(CONFIG_FILE)
