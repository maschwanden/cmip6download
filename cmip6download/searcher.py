import requests

from cmip6download import helper
from cmip6download import config


logger = helper.get_logger(__file__)

HTTP_BASE_TIMEOUT_TIME = 240


class CMIP6Searcher:
    """Class for finding relevant data from the CMIP6 API.

    This class makes HTTP requests corresponding to a given
    CMIP6Query instance and parses the returned XML file
    to extract downloadable files and associated metadata.

    Args:
        cmip6config (CMIP6Config): Configuration object
            containing serveral global parameters.

    """
    def __init__(self, cmip6config, **kwargs):
        if not isinstance(cmip6config, config.CMIP6Config):
            raise TypeError(f'`config` must be of type CMIP6Config')
        self.cmip6config = cmip6config

    def get_request_url(self, query, base_url=None):
        """Return search URL based on a search query.

        Args:
            query (CMIP6Query): CMIP6Query instance.
                Paramters set on this instance are sent to ESGF REST
                API to filter out the relevant datasets.
            base_url (str): Base CMIP6 API URL. If not specified,
                it is taken from the config file.

        """
        if base_url is None:
            base_url = self.cmip6config.cmip6restapi_url
        query = {key: value for key, value in query.items()
                 if value is not None}
        urlparts = list(urllib.parse.urlparse(base_url))
        urlparts[4] = urllib.parse.urlencode(query)
        url = urllib.parse.urlunparse(urlparts)
        return url

    def _get_xml_result_tag(self, query):
        """Get and extract XML 'result' tag from the returned CMIP6 API.

        Attrs:
            query (CMIP6Query): CMIP6Query instance.
                Paramters set on this instance are sent to ESGF REST
                API to filter out the relevant datasets.

        Returns:
            result (BeautifulSoup.Tag): 'result' tag from
                xml response.

        """
        try:
            url = self.get_request_url(base_url, query.as_query_dict())
            print(url)
            r = requests.get(
                url, timeout=HTTP_BASE_TIMEOUT_TIME,
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
        return data_item.CMIP6DataItem(
            filename=filename,
            file_url=file_url,
            remote_checksum=remote_checksum,
            remote_checksum_type=remote_checksum_type,
            local_base_dir=base_data_dir,
            )

    def _filter_data_items(self, data_items, query, config):
        print('_filter_data_items')

        # Filter number of member_id per (variable, model, experiment)
        members = np.unique(
            [d.metadata['member_id'] for d in data_items])
        models = np.unique(
            [d.metadata['source_id'] for d in data_items])
        variables = np.unique(
            [d.metadata['variable_id'] for d in data_items])
        experiments = np.unique(
            [d.metadata['experiment_id'] for d in data_items])

        var_model_exp_tuples = tuple(itertools.product(
            variables, models, experiments))
        var_model_exp_data_item_dict = {
                key: [] for key in var_model_exp_tuples}

        for d in data_items:
            m = d.metadata
            var_model_exp_data_item_dict[
                (m['variable_id'],
                 m['source_id'],
                 m['experiment_id'])].append(d)

        allowed_models = None
        if query.member_id:
            if not config.max_number_of_members:
                return data_items
            allowed_models = [query.member_id]

        print(f'Allowed models: {allowed_models}')

        max_n_members = config.max_number_of_members
        if not max_n_members:
            return data_items

        for key in var_model_exp_tuples:
            dis = var_model_exp_data_item_dict[key]
            if allowed_models is None:
                members = np.unique([d.metadata['member_id'] for d in dis])
            else:
                members = np.unique(allowed_models)
            n_members = len(members)
            if n_members > max_n_members:
                selected_members = helper.sort_member_id_str(
                    np.unique(members))[:max_n_members]
                print(selected_members)
                print(f'{key}: {len(dis)}')
                print(f'Total of {len(members)} ({members}) found but only {max_n_members} allowed ({selected_members})')
                var_model_exp_data_item_dict[key] = [
                    d for d in var_model_exp_data_item_dict[key]
                    if d.metadata['member_id'] in selected_members]
        data_items = []
        for key in var_model_exp_tuples:
            data_items.extend(var_model_exp_data_item_dict[key])
        return data_items

    def _extend_data_items(self, data_items, query, config):
        print('_extend_data_items')
        return data_items

    def get_data_items(self, query, config):
        """
        Search for CMIP6 data and return information about this data.

        Attrs:
            query (CMIP6Query): Paramters of this
                object are sent to ESGF REST API to filter
                out relevant datasets.
            config (CMIP6Config): Basic configuration object.

        Returns:
            data_items (list[CMIP6DataItem]): Encapsulated information
                for every file that can be downloaded.

        """
        if not isinstance(query, CMIP6Query):
            logger.warning(
                f'`query` must be of type dict but is of type {type(query)}.')
        doctags = self._get_xml_result_tag(
                , query).find_all('doc')
        data_items = self.__class__._get_data_items_from_doctags(
            doctags, self.cmip6config.base_data_dir)
        n_data_items0 = len(data_items)
        logger.info(f'{n_data_items0} files found.')
        # data_items = self._extend_data_items(data_items, query, config)
        # data_items = self._filter_data_items(data_items, query, config)
        if len(data_items) != n_data_items0:
            logger.info(f'-> {len(data_items)} after filtering/extending found.')
        else:
            logger.info(f'-> Nothing changed due to filtering/extending!!!')
        return data_items
