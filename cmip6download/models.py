from peewee import Model as PModel
from peewee import (
    SqliteDatabase, CharField, DateField, BooleanField, IntegerField,
    ForeignKeyField,
    )


db = SqliteDatabase('cmip6data.db')


class Model(PModel):
    name = CharField()
    institution = CharField()
    project = CharField(default='CMIP6')


class Scenario(PModel):
    name = CharField()


class ModelRun(PModel):
    model = ForeignKeyField(Model, backref='model_runs')
    scenario = ForeignKeyField(Scenario, backref='model_runs')


class SearchQuery(PModel):
    variable = CharField()
    freequency = CharField()
    model_run = ForeignKeyField(ModelRun, backref='model_runs')
    grid = CharField()

    data_type = CharField()
    replica = BooleanField()
    latest = BooleanField()
    distrib = BooleanField()
    limit = IntegerField()

    class Meta:
        database = db


class ModelRunFile(PModel):
    filename = CharField()
    file_url = CharField()
    remote_checksum = CharField()
    remote_checksum_type = CharField()
    local_dir = CharField()
    institution_id = CharField()
    remote_file_available = BooleanField(default=True)

    class Meta:
        database = db




