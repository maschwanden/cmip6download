from dataclasses import field, dataclass
from pprint import pformat, pprint
import yaml

from cmip6download import helper


class BaseAPIQuery:
    """Base class for encapsulation of query information.

    This class should only be sublassed by dataclasses.

    """
    def __str__(self):
        return pformat(self.as_query_dict())

    # def __hash__(self):
    #     return hash(str(self))

    def __repr__(self):
        raise NotImplementedError

    def __lt__(self, other):
        raise NotImplementedError

    @property
    def name(self):
        raise NotImplementedError

    @classmethod
    def create_from_yaml(cls, yaml_file):
        """Create query instances from a yaml file."""
        queries = []
        for query in yaml.load(open(yaml_file, 'r'), Loader=yaml.Loader):
            keys = cls.__annotations__.keys()
            for k in query.keys():
                if k not in keys:
                    raise ValueError(
                        f'Configuration Error: unknown key {k} in query '
                        f'file {yaml_file}')
            queries.extend(list({
                cls(**kwargs) for kwargs in list(helper.dict_product(query))
                }))
        return queries

    @staticmethod
    def queries_as_dict_list(queries):
        return [q.as_dict() for q in queries]

    def as_query_dict(self):
        raise NotImplementedError


@dataclass(unsafe_hash=True)
class CMIP6APIQuery(BaseAPIQuery):
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

    def __repr__(self):
        return (f'<{self.__class__.__name__} {self.variable} '
                f'{self.experiment_id} {self.frequency}>')

    def __lt__(self, other):
        return self.priority < other.priority

    @property
    def name(self):
        return (f'{self.frequency} {self.variable} '
                f'({self.experiment_id}; {self.grid_label}, '
                f'{self.member_id})')

    @staticmethod
    def sort_by_priority(queries):
        return sorted(queries)

    def as_query_dict(self):
        return {para: self.__dict__[para] for para in [
            'variable', 'frequency', 'experiment_id',
            'grid_label', 'project', 'type', 'replica',
            'latest', 'distrib', 'limit', 'activity_id',
            'member_id', 'source_id',]}
