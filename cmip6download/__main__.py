import argparse
import itertools
from multiprocessing import Pool
from pathlib import Path
from pprint import pprint
import sys

from cmip6download import CONFIG_DIR, CONFIG_FILE, CONFIG
from cmip6download import get_queries
from cmip6download import core, helper
from cmip6download.gspread_upload import update_status


parser = argparse.ArgumentParser(description='Download CMIP6.')
parser.add_argument(
    'query_file',
    help='YAML File containing information on data to download.')
parser.add_argument(
    '--config_file', dest='config_file', default=None,
    help='YAML configuration file.')
parser.add_argument(
    '--verify', action='store_true', dest='verify', default=None)
parser.add_argument(
    '--noverify', action='store_false', dest='verify', default=None)
parser.add_argument(
    '--gosearch', action='store_true', default=False)
parser.add_argument(
    '--download', action='store_true', default=False)

args = parser.parse_args()
print(args.query_file, args.config_file)

VERIFY = args.verify
GOSEARCH = args.gosearch
DOWNLOAD = args.download

if args.config_file is not None:
    CONFIG_FILE = Path(args.config_file)
    CONFIG = get_config(CONFIG_FILE)
QUERY_FILE = Path(args.query_file)
QUERIES = get_queries(QUERY_FILE)


def get_config(config_file):
    return core.CMIP6Config.create_from_yaml(config_file)


def get_queries(query_yaml_file):
    return core.CMIP6SearchQuery.create_from_yaml(query_yaml_file)


def download_and_verify(i, data_item, reverify_data):
    if data_item.verify_download(verify_checksum=reverify_data):
        return
    print(f'[{i}] Download {data_item.file_url}')
    data_item.download(max_attempts=CONFIG.max_download_attempts)


def main():
    reverify_data = False
    if VERIFY is None:
        if helper.ask_user('Reverify all already downloaded files?'):
            reverify_data = True
    else:
        reverify_data = VERIFY

    pprint(QUERIES)
    if not GOSEARCH:
        if not helper.ask_user(
                'Search with the above queries for CMIP6 data?'):
            print('Abort.')
            sys.exit()
    searcher = core.CMIP6Searcher(CONFIG)
    data_items = []
    for q in reversed(sorted(QUERIES)):
        tmp_data_items = searcher.get_data_items(q)
        print(f'Search for {q.name}: > {len(tmp_data_items)} < files found')
        data_items.extend(tmp_data_items)

    print(f'A total of {len(data_items)} files can be downloaded.')
    if DOWNLOAD:
        proceed_download = True
    else:
        proceed_download = helper.ask_user('Proceed?')
    if proceed_download:
        with Pool(CONFIG.n_worker) as p:
            data = list(zip(
                list(range(len(data_items))), data_items,
                [reverify_data] * len(data_items)))
            p.starmap(download_and_verify, data, chunksize=1)
            print('Finished downloading...')

        # update_status(data_items)

        # for i, data_item in enumerate(data_items):
        #     if data_item.verify_download(verify_checksum=reverify_data):
        #         continue
        #     print(f'[{i} / {len(data_items)}] Download {data_item.file_url}')
        #     data_item.download(max_attempts=CONFIG.max_download_attempts)


if __name__ == '__main__':
    main()
