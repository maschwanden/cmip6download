import urllib

import requests
from bs4 import BeautifulSoup

from cmip6download import helper
from cmip6download.data_item import CMIP6DataItem


logger = helper.get_logger(__file__)

HTTP_BASE_TIMEOUT_TIME = 240


class BaseAPISearcher:
    """Base class for finding relevant data from an API.

    Args:
        base_api_url (str): Base URL for the search API.

    """
    def __init__(self, base_api_url):
        self.base_api_url = base_api_url

    def get_request_url(self, query):
        """Return search URL based on a search query.

        Args:
            query (BaseAPIQuery): Paramters set of this
                instance are used in the API calls.

        """
        query_dict = {
            key: value for key, value in query.as_query_dict().items()
            if value is not None}
        urlparts = list(urllib.parse.urlparse(self.base_api_url))
        urlparts[4] = urllib.parse.urlencode(query_dict)
        url = urllib.parse.urlunparse(urlparts)
        return url

    def get_data_items(self, query):
        """Return based on query a list of data item instances."""
        raise NotImplementedError


class CMIP6APISearcher(BaseAPISearcher):
    """Class for finding relevant data from the CMIP6 API.

    This class makes HTTP requests corresponding to a given
    CMIP6Query instance and parses the returned XML file
    to extract downloadable files and associated metadata.

    Args:
        base_api_url (str): Base URL for the CMIP6 search API.
        base_data_dir (str or pathlib.Path): Base path where
            downloaded data should be stored.

    """
    def __init__(self, base_api_url, base_data_dir):
        super().__init__(base_api_url)
        self.base_data_dir = base_data_dir

    def get_data_items(self, query, filter_kwargs=None):
        """
        Search for CMIP6 data and return information about this data.

        Attrs:
            query (CMIP6Query): Paramters of this
                object are sent to ESGF REST API to filter
                out relevant datasets.

        Returns:
            data_items (list[CMIP6DataItem]): Encapsulated information
                for every file that can be downloaded.

        """
        if filter_kwargs is None:
            filter_kwargs = {}
        data_items = self._get_temporary_data_items(query)
        n_data_items0 = len(data_items)
        logger.info(f'{n_data_items0} files found.')
        data_items = self._filter_data_items(data_items, query, **filter_kwargs)
        if len(data_items) != n_data_items0:
            logger.info(f'-> {len(data_items)} after filtering/extending found.')
        else:
            logger.info(f'-> Nothing changed due to filtering/extending!!!')
        return data_items

    def _get_temporary_data_items(self, query):
        """Return data items directly from an API call using query.

        These data items can then later be filtered/extended.
        """
        try:
            url = self.get_request_url(query)
            http_request = requests.get(
                url, timeout=HTTP_BASE_TIMEOUT_TIME,
                )
        except requests.exceptions.ReadTimeout as e:
            logger.warning(f'Could not get list of downloadable files ({e}).')
        result_tag = BeautifulSoup(http_request.text, 'lxml').result
        doc_tags = result_tag.find_all('doc')
        return [self._get_data_item_from_doctag(doc_tag)
            for doc_tag in doc_tags]

    def _get_data_item_from_doctag(self, doc_tag):
        filename = str(doc_tag.find('str', attrs={'name': 'title'}).string)
        remote_checksum = str(doc_tag.find(
            'arr', attrs={'name': 'checksum'}).str.string)
        remote_checksum_type = str(doc_tag.find(
            'arr', attrs={'name': 'checksum_type'}).str.string)
        file_url = None
        for url_str in doc_tag.find(
                'arr', attrs={'name': 'url'}).find_all('str'):
            tmp_file_url, _, tmp_file_download_type = [
                    s.strip() for s in url_str.string.split('|')]
            if tmp_file_download_type.lower() == 'httpserver':
                file_url = tmp_file_url
                break;
        if file_url is None:
            raise ValueError(
                'Could not find any download URL for file {filename}')
        return CMIP6DataItem(
            filename=filename,
            file_url=file_url,
            remote_checksum=remote_checksum,
            remote_checksum_type=remote_checksum_type,
            local_base_dir=self.base_data_dir,
            )

    def _filter_data_items(self, data_items, query, **kwargs):
        """
        Kwargs:
            max_number_of_members (int): The number of unique
                members per model is limited to this number.

        """
        filtered_data_items_set = set(data_items)

        unq_models = list({
            d.metadata['source_id'] for d in data_items
            })

        max_number_of_members = kwargs.get('max_number_of_members', None)
        if max_number_of_members:
            all_model_data_items = []
            for model in unq_models:
                model_data_items = [
                    d for d in filtered_data_items_set
                    if d.metadata['source_id'] == model]
                unq_members = list({
                    d.metadata['member_id'] for d in model_data_items
                    })

                # Get the maximum number of members, if this parameter is
                # not given just take an unlimited (1e9) number of members.
                if max_number_of_members < len(unq_members):
                    # Select first X of unq_members according to
                    # member sorting algorithm
                    selected_members = helper.sort_member_id_str(
                        unq_members)[:max_number_of_members]
                    model_data_items = [
                        d for d in model_data_items
                        if d.metadata['member_id'] in selected_members]
                    unq_members2 = list(
                        {d.metadata['member_id'] for d in model_data_items})
                    logger.debug(
                        f'Filtering of number of members ({model}) '
                        f'{len(unq_members)} -> {len(unq_members)}.')

                all_model_data_items.extend(model_data_items)
            filtered_data_items_set = set(all_model_data_items)

        # More filtering could come here
        # .....

        return list(filtered_data_items_set)
