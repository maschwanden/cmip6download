from dataclasses import field, dataclass
from pprint import pformat, pprint
import yaml

from cmip6download import helper


logger = helper.get_logger(__file__)


@dataclass
class CMIP6Query:
    variable: str
    frequency: str
    experiment_id: str
    source_id: str = None
    grid_label: str = None
    activity_id: str = None
    member_id: str = None

    project: str = 'CMIP6'
    type: str = 'File'
    replica: bool = False
    latest: bool = None
    distrib: bool = None
    limit: int = 10000

    priority: int = 100

    def __str__(self):
        return pformat(self.as_query_dict())

    def __repr__(self):
        return (f'<{self.__class__.__name__} {self.variable} '
                f'{self.experiment_id} {self.frequency}>')

    def __hash__(self):
        return hash(str(self))

    def __lt__(self, other):
        return self.priority < other.priority

    @property
    def name(self):
        return (f'{self.frequency} {self.variable} '
                f'({self.experiment_id}; {self.grid_label})')

    # @classmethod
    # def create_from_yaml(cls, yaml_file):
    #     data = yaml.load(open(yaml_file, 'r'), Loader=yaml.Loader)
    #     queries = []

    #     for query in data:
    #         product_params = {}
    #         kwargs = {}
    #         for key, value in cls.__annotations__.items():
    #             default_value = cls.__dataclass_fields__[key].default
    #             res = query.get(key, default_value)
    #             if value == list:
    #                 if res is None:
    #                     res = [None]
    #                 product_params[key] = res
    #             else:
    #                 kwargs[key] = res
    #         queries.extend([cls(**{**kwargs, **x})
    #             for x in list(helper.dict_product(product_params))])
    #     return list(set(queries))

    @classmethod
    def create_from_yaml(cls, yaml_file):
        """Create query instances from a yaml file.

        The product from all given parameters in the yaml file is
        built and for each element a query instance is created.

        Args:
            yaml_file (str or pathlib.Path): Path to a YAML file
                containing query information.

        Returns:
            queries (list[CMIP6Query]): List of query instances.

        """
        queries = []
        for query in yaml.load(open(yaml_file, 'r'), Loader=yaml.Loader):
            keys = cls.__annotations__.keys()
            for k in query.keys():
                if k not in keys:
                    raise ValueError(
                        f'Configuration Error: unknwon key {k} in query '
                        f'file {yaml_file}')
            queries.extend([cls(**kwargs) for kwargs in list(helper.dict_product(query))])
        return queries

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
