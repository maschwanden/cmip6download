import sys

from peewee import Model as PModel
from peewee import (
    SqliteDatabase, CharField, DateField, BooleanField, IntegerField,
    ForeignKeyField,
    )

from cmip6download import CONFIG

print(CONFIG)

sys.exit()

db = SqliteDatabase(sqlite_file)


class BaseModel(PModel):
    class Meta:
        database = db  # Use proxy for our DB.


class SearchQuery(BaseModel):
    variable = CharField()
    frequency = CharField()
    model_run = CharField()
    grid = CharField()

    data_type = CharField()
    replica = BooleanField()
    latest = BooleanField()
    distrib = BooleanField()
    limit = IntegerField()

    class Meta:
        database = db


class ModelRunFile(BaseModel):
    filename = CharField(unique=True)
    file_url = CharField()
    institution_id = CharField()

    query = ForeignKeyField(SearchQuery, backref='model_run_files')
    remote_file_available = BooleanField(default=True)
    local_file_available = BooleanField(default=False)

    last_verified_date = DateField()
    download_date = DateField()

    class Meta:
        database = db


    db.create_tables([SearchQuery, ModelRunFile])


