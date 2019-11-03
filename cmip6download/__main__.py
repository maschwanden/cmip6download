import argparse
from pathlib import Path
from pprint import pprint
import sys

from cmip6download import CONFIG_DIR, CONFIG_FILE, CONFIG
from cmip6download import get_queries
from cmip6download import core, helper


parser = argparse.ArgumentParser(description='Download CMIP6.')
parser.add_argument(
    'query_file',
    help='YAML File containing information on data to download.')
parser.add_argument(
    '--config_file', dest='config_file', default=None,
    help='YAML configuration file.')

args = parser.parse_args()
print(args.query_file, args.config_file)

if args.config_file is not None:
    CONFIG_FILE = Path(args.config_file)
    CONFIG = get_config(CONFIG_FILE)
QUERY_FILE = Path(args.query_file)
QUERIES = get_queries(QUERY_FILE)


def get_config(config_file):
    return core.CMIP6Config.create_from_yaml(config_file)


def get_queries(query_yaml_file):
    return core.CMIP6SearchQuery.create_from_yaml(query_yaml_file)


def main():
    reverify_data = False
    if helper.ask_user('Reverify all already downloaded files?'):
        reverify_data = True
    pprint(QUERIES)
    if not helper.ask_user('Search with the above queries for CMIP6 data?'):
        print('Abort.')
        sys.exit()
    searcher = core.CMIP6Searcher(CONFIG)
    data_items = []
    for q in QUERIES:
        print('-----------------')
        print(q.name)
        print('-----------------')
        tmp_data_items = searcher.get_data_items(q)
        data_items.extend(tmp_data_items)

    print(f'A total of {len(data_items)} files can be downloaded.')
    if helper.ask_user('Proceed?'):
        for i, data_item in enumerate(data_items):
            if data_item.verify_download(verify_checksum=reverify_data):
                continue
            print(f'[{i} / {len(data_items)}] Download {data_item.file_url}')
            data_item.download(max_attempts=CONFIG.max_download_attempts)


if __name__ == '__main__':
    main()
