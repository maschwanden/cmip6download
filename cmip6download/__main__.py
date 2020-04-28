import argparse
import datetime
import itertools
import multiprocessing
from multiprocessing import Pool
from pathlib import Path
from pprint import pprint
import sys

from cmip6download import CONFIG_DIR, CONFIG_FILE, CONFIG
from cmip6download import get_queries, get_config
from cmip6download import core, helper


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
    '--debug', action='store_true', default=False)


args = parser.parse_args()
print(args.query_file, args.config_file)

VERIFY = args.verify
GOSEARCH = args.gosearch
DEBUG = args.debug

if args.config_file is not None:
    CONFIG_FILE = Path(args.config_file)
    CONFIG = get_config(CONFIG_FILE)
QUERY_FILE = Path(args.query_file)
QUERIES = get_queries(QUERY_FILE)

try:
    from cmip6download import gspread_database
    GSPREAD_DB = gspread_database.GoogleDataItemDatabase(
        CONFIG.google_credentials_file, CONFIG.google_sheet_path)
    print(f'Connected to Gspread Database!')
except Exception as e:
    print(f'Could not connect to Gspread Database! (Exception "{e}")')
    GSPREAD_DB = None


def download_and_verify(i, data_item, reverify_data, return_dict):
    if data_item.verify_download(verify_checksum=reverify_data):
        print(f'[{i}] Already exists...')
    else:
        print(f'[{i}] Download {data_item.file_url}')
        data_item.download(max_attempts=CONFIG.max_download_attempts)
        data_item.download_date = datetime.date.today().strftime('%Y-%m-%d')
    return_dict[i] = data_item


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
    for q in reversed(sorted(QUERIES)):
        query_data_items = searcher.get_data_items(q)
        N_query_data_items = len(query_data_items)
        q.url = searcher.get_request_url(
            CONFIG.cmip6restapi_url, q.as_query_dict())
        for data_item in query_data_items:
            data_item.query_file = QUERY_FILE
        print(f'Search for {q.name}: > {len(query_data_items)} < files found')

        with Pool(CONFIG.n_worker) as p:
            manager = multiprocessing.Manager()
            return_dict = manager.dict()
            data = list(zip(
                list(range(N_query_data_items)), query_data_items,
                [reverify_data]*N_query_data_items,
                [return_dict]*N_query_data_items))
            p.starmap(download_and_verify, data, chunksize=1)
            res = return_dict.values()

        if GSPREAD_DB is not None:
            GSPREAD_DB.update_dataitems(res)
            print('Saved current status into Gspread Database!')
        else:
            print('Could not save current status into Gspread Database.')


if __name__ == '__main__':
    main()
