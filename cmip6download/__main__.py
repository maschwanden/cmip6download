import argparse
import copy
import itertools
import multiprocessing
from multiprocessing import Pool
from pathlib import Path
from pprint import pprint
import sys

from cmip6download import helper
from cmip6download.config import CMIP6Config
from cmip6download.data_item import CMIP6DataItem
from cmip6download.query import CMIP6APIQuery
from cmip6download.searcher import CMIP6APISearcher


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
VERIFY = args.verify
GOSEARCH = args.gosearch
DEBUG = args.debug

CONFIG_FILE = Path.home() / '.config/cmip6download/config.yaml'
if args.config_file is not None:
    CONFIG_FILE = Path(args.config_file)
CONFIG = CMIP6Config.create_from_yaml(CONFIG_FILE)
QUERY_FILE = Path(args.query_file)
QUERIES = CMIP6APIQuery.create_from_yaml(QUERY_FILE)

try:
    from cmip6download import gspread_database
    GSPREAD_DB = gspread_database.GoogleDataItemDatabase(
        CONFIG.google_credentials_file, CONFIG.google_sheet_path)
    GSPREAD_OVERVIEW_DB = gspread_database.GoogleOverviewDatabase(
        CONFIG.google_credentials_file, CONFIG.google_sheet_path)
    GSPREAD_FAILED_DB = gspread_database.GoogleFailedDataItemDatabase(
        CONFIG.google_credentials_file, CONFIG.google_sheet_path+'_failed')
    print(f'Connected to Gspread Database!')
except Exception as e:
    print(f'Could not connect to Gspread Database! (Exception "{e}")')
    GSPREAD_DB = None
    GSPREAD_OVERVIEW_DB = None
    GSPREAD_FAILED_DB = None


def download_and_verify(i, data_item, reverify_data, return_dict):
    if data_item.verify_download(verify_checksum=reverify_data):
        print(f'[{i}] Already exists... {data_item.filename}')
    else:
        print(f'[{i}] Download {data_item.file_url}')
        download_status = data_item.download(
            max_attempts=CONFIG.max_download_attempts)
        if download_status is not None:
            import datetime
            data_item.download_date = datetime.date.today().strftime(
                '%Y-%m-%d')
            print(f'[{i}] Success! Downloaded {data_item.filename}!')
        else:
            data_item.download_date = None
    return_dict[i] = data_item


if __name__ == '__main__':
    reverify_data = False
    if VERIFY is None:
        if helper.ask_user('Reverify all already downloaded files?'):
            reverify_data = True
    else:
        reverify_data = VERIFY

    for query in QUERIES:
        print(query)
    if not GOSEARCH:
        if not helper.ask_user(
                'Search with the above queries for CMIP6 data?'):
            print('Abort.')
            sys.exit()

    searcher = CMIP6APISearcher(
        CONFIG.cmip6restapi_url, CONFIG.base_data_dir)

    all_failed_data_items = []

    # If in the config the min_number_of_members is set the following
    # happens:
    # 1) For every query an additional (unique) query with member_id
    #    set to None is added to all_queries in order to search for
    #    any member (no filtering of member_id; even if member_ids are
    #    set in the yaml file).
    # 2) To limit the number of members which are found by these newly
    #    added queries (if not limited, they would just found ALL members
    #    which are available; however the idea behind min_number_of_members
    #    is that despite the configured member_ids at least X members
    #    are found), the key-value pair 'max_number_of_members:
    #    min_number_of_members' is added to data_item_filter_kwargs (which
    #    is then passed to the CMIP6APISearcher.get_data_items method).
    #
    all_queries = QUERIES
    data_item_filter_kwargs = {}
    min_number_of_members = getattr(CONFIG, 'min_number_of_members', 0)
    max_number_of_members = getattr(CONFIG, 'max_number_of_members', 0)
    if min_number_of_members > 0:
        add_queries = []
        for query in all_queries:
            query = copy.deepcopy(query)
            query.member_id = None
            add_queries.append(query)
        add_queries = list(set(add_queries))
        print(
            f'Extend queries by {len(add_queries)} queries due '
            'to min_number_of_members.')
        for query in add_queries:
            print(query)
        all_queries.extend(add_queries)
        data_item_filter_kwargs['max_number_of_members'] = \
            min_number_of_members
    else:
        print('min_number_of_members was not set.')

    # If max_number_of_members is set,
    # data_item_filter_kwargs["max_number_of_members"] is set to
    # max_number_of_members.
    if max_number_of_members > 0:
        if min_number_of_members > max_number_of_members:
            max_number_of_members = min_number_of_members
        data_item_filter_kwargs['max_number_of_members'] = \
            max_number_of_members

    print(f'data_item_filter_kwargs: {data_item_filter_kwargs}')

    for query in reversed(sorted(all_queries)):
        query_data_items = searcher.get_data_items(
            query, filter_kwargs=data_item_filter_kwargs)
        N_query_data_items = len(query_data_items)
        cmip6_api_search_call = searcher.get_request_url(query)
        for data_item in query_data_items:
            data_item.query_file = QUERY_FILE
            data_item.cmip6_api_search_call = cmip6_api_search_call
        print(f'Search for {query.name}: > '
              f'{len(query_data_items)} < files found')

        with Pool(CONFIG.n_worker) as p:
            manager = multiprocessing.Manager()
            return_dict = manager.dict()
            data = list(zip(
                list(range(N_query_data_items)), query_data_items,
                [reverify_data]*N_query_data_items,
                [return_dict]*N_query_data_items))
            p.starmap(download_and_verify, data, chunksize=1)
            data_items = return_dict.values()

        failed_data_items = []
        for data_item in data_items:
            if not data_item.verify_download():
                failed_data_items.append(data_item)

        if len(failed_data_items) > 0:
            print('----------------------------------------------')
            print('The following files could not be downloaded:')
            for data_item in failed_data_items:
                    print(
                        '[FAILED] ', data_item.filename,
                        data_item._used_download_urls)
            print('----------------------------------------------')

        all_failed_data_items.extend(failed_data_items)

        if GSPREAD_DB is not None and len(data_items):
            GSPREAD_DB.update_dataitems(data_items)
            print('Saved current status into Gspread Database!')
        if GSPREAD_OVERVIEW_DB is not None and len(data_items):
            GSPREAD_OVERVIEW_DB.update_download_overview(data_items)
            print('Updated Gspread OVERVIEW Database!')

    print(f'A total of {len(all_failed_data_items)} downloads failed.')
    print('The following files could not be downloaded:')
    for data_item in all_failed_data_items:
        print(f'> {data_item.filename}')

    if GSPREAD_FAILED_DB is not None and len(all_failed_data_items):
        GSPREAD_FAILED_DB.update_dataitems(all_failed_data_items)
        print('Updated Gspread FAILED Database!')
