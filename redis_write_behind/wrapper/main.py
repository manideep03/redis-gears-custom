from redis_write_behind.utils.db_connectors import MySqlConnection, MySqlConnector
from redis_write_behind.pipelines import KeyValueWriteBehind, ListWriteBehind, HashWriteBehind

class WBJobBuilder():
    def __init__(self, GB, jobConfig:dict) -> str:
        self.__validateJob(jobConfig)
        dt = jobConfig['targetDataType'].lower()
        keyFlag = False
        if 'primaryCacheKey' in jobConfig.keys() and jobConfig['primaryCacheKey'] == 1:
            keyFlag = True
        
        __conn = self.__prepareConnector(jobConfig)
        
        if dt == 'hash':
            HashWriteBehind(GB, connector=__conn, keysPrefix=jobConfig['keysPrefix'], name=jobConfig['name'], version=jobConfig['version'], mappings=jobConfig['keysMapping'], primaryCacheKey=keyFlag)
            return 'Ok'
        if dt == 'list':
            ListWriteBehind(GB, connector=__conn, keysPrefix=jobConfig['keysPrefix'], name=jobConfig['name'], version=jobConfig['version'], mappings=jobConfig['keysMapping'])
            return 'Ok'
        if dt == 'keyValue':
            KeyValueWriteBehind(GB, connector=__conn, keysPrefix=jobConfig['keysPrefix'], name=jobConfig['name'], version=jobConfig['version'], mappings=jobConfig['keysMapping'])
            return 'Ok'
        else:
            raise Exception("%s target datatype not compatible".format(dt))

    def __validateJob(self, config:dict):
        __mandatoryCol = ['name', 'version', 'keysPrefix', 'targetDataType', 'keysMapping', 'dbConfig']
        __mandatoryColForDB = ['host', 'user', 'password', 'tableName', 'primaryKey']
        for i in __mandatoryCol:
            if i not in config.keys(): raise Exception('%s not found in config'.format(i))

        for i in __mandatoryColForDB:
            if i not in config['dbConfig'].keys(): raise Exception('%s not found in dbConfig'.format(i))

    def __prepareConnector(self, config:dict) -> MySqlConnector:
        return MySqlConnector(MySqlConnection(config['dbConfig']['user'],
                                              config['dbConfig']['password'], 
                                              config['dbConfig']['host']), 
                              tableName=config['dbConfig']['tableName'], 
                              pk=config['dbConfig']['primaryKey'])
    