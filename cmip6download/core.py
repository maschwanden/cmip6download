"""This script downloads CMIP6 data from the web.

In the configuration section a list search parameters can be specified
which is then used to search the CMIP6 website for data files and then
the user is asked if all these files should be downloaded. The files
are automatically sorted by their model, experiment, etc. and stored
in a accordingly named directory.

Required python packages:
- requests
- beautifulsoup4

Created March 2019
Last Updated October 2019
@author: Mathias Aschwanden (mathias.aschwanden@gmail.com)

"""
from dataclasses import field, dataclass
import datetime
import hashlib
import itertools
import logging
from pathlib import Path
from pprint import pformat, pprint
import re
import shutil
import sys
import urllib
import yaml

import requests
from bs4 import BeautifulSoup

from cmip6download import helper
from cmip6download.config import CMIP6Config


HTTP_HEAD_TIMEOUT_TIME = 120
HTTP_BASE_TIMEOUT_TIME = 240
HTTP_DOWNLOAD_TIMEOUT_TIME = 1200
REQUESTS_CHUNK_SIZE = 128 * 1024

logger = helper.get_logger(__file__)


@dataclass
class CMIP6SearchQuery:
    variable: list
    frequency: list = None
    experiment_id: list = None
    source_id: list = None
    grid_label: list = None
    activity_id: list = None
    member_id: list = None

    project: str = 'CMIP6'
    type: str = 'File'
    replica: bool = False
    latest: bool = None # True
    distrib: bool = None # True
    limit: int = 10000

    priority: int = 100

    def __str__(self):
        return pformat(self.as_query_dict())

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(str(self))

    def __lt__(self, other):
        return self.priority < other.priority

    @property
    def name(self):
        return (f'{self.frequency} {self.variable} '
                f'({self.experiment_id}; {self.grid_label})')

    @classmethod
    def create_from_yaml(cls, yaml_file):
        data = yaml.load(open(yaml_file, 'r'), Loader=yaml.Loader)
        queries = []

        for query in data:
            product_params = {}
            kwargs = {}
            for key, value in cls.__annotations__.items():
                default_value = cls.__dataclass_fields__[key].default
                res = query.get(key, default_value)
                if value == list:
                    if res is None:
                        res = [None]
                    product_params[key] = res
                else:
                    kwargs[key] = res
            queries.extend([cls(**{**kwargs, **x})
                for x in list(helper.dict_product(product_params))])
        return list(set(queries))

    @staticmethod
    def sort_by_priority(queries):
        return sorted(queries)

    @staticmethod
    def queries_as_dict_list(queries):
        return [q.as_dict() for q in queries]

    def as_query_dict(self):
        return {para: self.__dict__[para] for para in [
            'variable', 'frequency', 'experiment_id',
            'grid_label', 'project', 'type', 'replica',
            'latest', 'distrib', 'limit', 'activity_id',
            'member_id', 'source_id',]}


@dataclass
class CMIP6DataItem:
    filename: str
    file_url: str
    remote_checksum: str
    remote_checksum_type: str
    institution_id: str
    local_data_dir: Path

    query_file: str = None
    remote_file_available: bool = True
    download_date: str = None
    download_successfull: bool = None

    @property
    def local_file(self):
        return self.local_dir / self.filename

    @property
    def checksum_matches(self):
        return self.local_checksum == self.remote_checksum

    @property
    def local_checksum(self):
        if self.remote_checksum_type == 'SHA256':
            return helper.sha256_checksum_file(self.local_file)
        raise ValueError('Unkown checksum type!')

    @property
    def institution_filename_str(self):
        return f'[{self.institution_id}] {self.filename}'

    @property
    def local_dir(self):
        return helper.get_local_dir(self.filename, self.local_data_dir)

    def verify_download(self, verify_checksum=False):
        verified = True
        if self.local_file.exists():
            if verify_checksum:
                if not self.checksum_matches:
                    verified = False
                    logger.debug(f'Local and remote checksum do no match.')
        else:
            logger.debug(f'File does not exist locally.')
            verified = False
        return verified

    def _download_file(self):
        try:
            head = requests.head(
                self.file_url, allow_redirects=True, verify=False,
                timeout=HTTP_HEAD_TIMEOUT_TIME
                # auth=requests.auth.HTTPBasicAuth('aschiii', 'Xsw2&&nji9')
                )
            logger.debug(
                f'Header retrieved with code {head.status_code}.')
            if head.status_code != 200:
                logger.warning(
                    'Failed to retrieve the header (status code '
                    f'{head.status_code}). Cannot download file.')
                self.remote_file_available = False
                return
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout) as e:
            logger.warning(f'Failed to retrieve the header (error code {e}).')
            self.remote_file_available = False
            return

        with requests.get(self.file_url, allow_redirects=True, verify=False,
                timeout=HTTP_DOWNLOAD_TIMEOUT_TIME, stream=True) as r:
            if r.status_code == 404:
                raise requests.HTTPError(404)
            try:
                with open(self.local_file, 'wb') as f:
                    for chunk in r.iter_content(REQUESTS_CHUNK_SIZE):
                        if not chunk:
                            break
                        f.write(chunk)
            except requests.exceptions.ChunkedEncodingError as e:
                print(f'Could not finish download of {self.file_url} ({e})')

    def download(
            self, max_attempts=1, attempt=1, reverify_checksum=False,
            redownload=False):
        if redownload:
            pass
        elif self.verify_download(verify_checksum=reverify_checksum):
            return self.local_file
        if self.local_file.exists():
            self.local_file.unlink()
        self.local_dir.mkdir(exist_ok=True, parents=True)
        try:
            self._download_file()
            if self.verify_download(verify_checksum=True):
                logger.info(f'Download of {self.filename} successfull.')
                self.download_date = datetime.datetime.now()
                return self.local_file
            else:
                logger.info(f'Try to re-download... (attempt {attempt})')
                if attempt < max_attempts:
                    if self.local_file.exists():
                        logger.warning(
                            f'Local file exists but is going to be deleted')
                        self.local_file.unlink()
                    return self.download(
                        max_attempts=max_attempts, attempt=attempt+1)
                    logger.warning(
                        'Download successfull but verification failed. '
                        f'Try to redownload ({attempt}th attempt).')
                logger.warning(f'Failed. Exceeded max number of attempts.')
                return
        except (requests.HTTPError, requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout) as e:
            logger.error(e)
            return


class CMIP6Searcher:
    def __init__(self, config, **kwargs):
        if not isinstance(config, CMIP6Config):
            raise TypeError(f'`config` must be of type CMIP6Config')
        self.config = config

    @staticmethod
    def get_request_url(base_url, query):
        query = {key: value for key, value in query.items()
                 if value is not None}
        urlparts = list(urllib.parse.urlparse(base_url))
        urlparts[4] = urllib.parse.urlencode(query)
        return urllib.parse.urlunparse(urlparts)

    @classmethod
    def _get_result_tag(cls, base_url, query):
        """
        Download and return dataset info corresponding to given
        search parameters.

        Attrs:
            base_url (str): Base URL for CMIP6 data search.
            query (CMIP6SearchQuery): CMIP6SearchQuery instance.
                Paramters set on this instance are sent to ESGF REST
                API to filter out the relevant datasets.

        Returns:
            result (BeautifulSoup.Tag): "result" tag from
                xml response.

        """
        try:
            url = cls.get_request_url(base_url, query.as_query_dict())
            r = requests.get(
                url, timeout=HTTP_BASE_TIMEOUT_TIME,
                # auth=requests.auth.HTTPBasicAuth('aschiii', 'Xsw2&&nji9'),
                )
        except requests.exceptions.ReadTimeout as e:
            logger.warning(f'Could not get list of downloadable files ({e}).')
        return BeautifulSoup(r.text, 'lxml').result

    @staticmethod
    def _get_http_file_url(doctag):
        arr_url = doctag.find('arr', attrs={'name': 'url'})
        file_url = None
        for url_str in arr_url.find_all('str'):
            tmp_file_url, tmp_file_type, tmp_file_download_type = [
                    s.strip() for s in url_str.string.split('|')]
            if tmp_file_download_type.lower() == 'httpserver':
                file_url = tmp_file_url
                break;
        if file_url is None:
            raise ValueError(f'No link for the file {doctag.title}')
        return file_url

    @classmethod
    def _get_data_items_from_doctags(cls, doctags, base_data_dir):
        return [cls._get_data_item_from_doctag(doctag, base_data_dir)
                for doctag in doctags]

    @classmethod
    def _get_data_item_from_doctag(cls, doctag, base_data_dir):
        filename = str(doctag.find('str', attrs={'name': 'title'}).string)
        title_tag = doctag.find('str', attrs={'name': 'title'})
        institution_id = str(title_tag.parent.find(
                'arr', attrs={'name': 'institution_id'}).str.string)
        try:
            remote_checksum = str(doctag.find(
                'arr', attrs={'name': 'checksum'}).str.string)
        except Exception as e:
            raise e
        try:
            remote_checksum_type = str(doctag.find(
                'arr', attrs={'name': 'checksum_type'}).str.string)
        except Exception as e:
            raise e
        try:
            file_url = cls._get_http_file_url(doctag)
        except:
            file_url = None
        return CMIP6DataItem(
            filename=filename,
            file_url=file_url,
            remote_checksum=remote_checksum,
            remote_checksum_type=remote_checksum_type,
            institution_id=institution_id,
            local_data_dir=base_data_dir,
            )

    def get_data_items(self, query):
        """
        Searche for CMIP6 data and return information about this data.

        Attrs:
            query (CMIP6SearchQuery): CMIP6SearchQuery instance.
                Paramters set on this instance are sent to ESGF REST
                API to filter out the relevant datasets.

        Returns:
            data_items (list[CMIP6DataItem]): Information to every
                file which is found in connection with the given
                search query.

        """
        if not isinstance(query, CMIP6SearchQuery):
            logger.warning(
                f'`query` must be of type dict but is of type {type(query)}.')
        doctags = self._get_result_tag(
                self.config.cmip6restapi_url, query).find_all('doc')
        data_items = self.__class__._get_data_items_from_doctags(
            doctags, self.config.base_data_dir)
        logger.info(f'{len(data_items)} files found.')
        return data_items
