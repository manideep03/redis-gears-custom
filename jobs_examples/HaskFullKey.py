# custoum config for storing whole key as primary key
from redis_write_behind import DefaultWriteBehind, MySqlConnection, MySqlConnector

connection = MySqlConnection('root', 'root', '172.17.0.3:3306/test')

'''
Create MySQL persons connector
'''
personsConnector = MySqlConnector(connection, 'persons_new', 'cache_key')

personsMappingsWithKey = {
	'first_name':'first',
	'last_name':'last',
	'age':'age'
}

DefaultWriteBehind(GB,  keysPrefix='hello1/', mappings=personsMappingsWithKey, connector=personsConnector, name='PersonsWriteBehindWithKey1',  version='0.0.4', primaryCacheKey=True)