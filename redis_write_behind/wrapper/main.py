from redis_write_behind.utils.db_connectors import MySqlConnection, MySqlConnector
from redis_write_behind.pipelines import KeyValueWriteBehind, ListWriteBehind, HashWriteBehind, TrackExpireKey

class WBJobBuilder():
    def __init__(self, GB, jobConfig:dict):
        self.__config = jobConfig
        self.__GB = GB
        self.__validateJob(jobConfig)

    def expireKeyTest(self, name, version) -> str:
        GB = self.__GB
        TrackExpireKey(GB=GB, name=name, version=version)
        return "OK"
    
    def create(self) -> str:
        jobConfig = self.__config
        GB = self.__GB
        
        dt = jobConfig['targetDataType'].lower()
        keyFlag = False
        if 'primaryCacheKey' in jobConfig.keys() and jobConfig['primaryCacheKey'] == 1:
            keyFlag = True
        
        __conn = self.__prepareConnector(jobConfig)
        
        if dt == 'hash':
            HashWriteBehind(GB, connector=__conn, keysPrefix=jobConfig['keysPrefix'], name=jobConfig['name'], version=jobConfig['version'], mappings=jobConfig['keysMapping'], primaryCacheKey=keyFlag)
            return "OK"
        if dt == 'list':
            ListWriteBehind(GB, connector=__conn, keysPrefix=jobConfig['keysPrefix'], name=jobConfig['name'], version=jobConfig['version'], mappings=jobConfig['keysMapping'])
            return "OK"
        if dt == 'keyValue':
            KeyValueWriteBehind(GB, connector=__conn, keysPrefix=jobConfig['keysPrefix'], name=jobConfig['name'], version=jobConfig['version'], mappings=jobConfig['keysMapping'])
            return "OK"
        else:
            raise Exception("{} target datatype not compatible".format(dt))

    def __validateJob(self, config:dict):
        __mandatoryCol = ['name', 'version', 'keysPrefix', 'targetDataType', 'keysMapping', 'dbConfig']
        __mandatoryColForDB = ['host', 'user', 'password', 'tableName', 'primaryKey']
        for i in __mandatoryCol:
            if i not in config.keys(): raise Exception('{} not found in config'.format(i))

        for i in __mandatoryColForDB:
            if i not in config['dbConfig'].keys(): raise Exception('{} not found in dbConfig'.format(i))

    def __prepareConnector(self, config:dict) -> MySqlConnector:
        return MySqlConnector(MySqlConnection(config['dbConfig']['user'],
                                              config['dbConfig']['password'], 
                                              config['dbConfig']['host']), 
                              tableName=config['dbConfig']['tableName'], 
                              pk=config['dbConfig']['primaryKey'])
    