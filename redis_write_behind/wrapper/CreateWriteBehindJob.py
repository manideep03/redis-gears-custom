from redis_write_behind.utils import MySqlConnection, MySqlConnector

from redis_write_behind.pipelines.HashPipeline import HashWriteBehind
from redis_write_behind.pipelines.KeyValuePipeline import KeyValueWriteBehind
from redis_write_behind.pipelines.ListPipeline import ListWriteBehind

def pickAndPreparePipeline(config:dict):
    db_connector = MySqlConnector(
        MySqlConnection(config['dbConfig']['user'], config['dbConfig']['password'], config['dbConfig']['host']), 
                config['dbConfig']['tableName'], 
                config['dbConfig']['primaryKey']
            )
    targetDataType = config['targetDataType']
    if targetDataType.lower() == 'hash':
        HashWriteBehind(GB, name=config['name'], version=config['version'], keysPrefix=config['keysPrefix'], mappings=config['keysMapping'], connector=db_connector)
    if targetDataType.lower() == 'set':
        KeyValueWriteBehind(GB, name=config['name'], version=config['version'], keysPrefix=config['keysPrefix'], mappings=config['keysMapping'], connector=db_connector)
    if targetDataType.lower() == 'list':
        ListWriteBehind(GB, name=config['name'], version=config['version'], keysPrefix=config['keysPrefix'], mappings=config['keysMapping'], connector=db_connector)

def validateConfig(config: dict) -> None:
    # should check all params are there or not
    # allMandates = []
    # for i in config.keys():
    #     if i not in allMandates and i != None or i!= "": raise Exception("[%s] not found/empty/none in provided config".format(i))
    pass

def create_job(jobConfig: dict):
    if not isinstance(jobConfig, dict):
        raise Exception("Config should be in python dict format!")
    validateConfig(dict)
    pickAndPreparePipeline(config=jobConfig)