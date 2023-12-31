# basic job

from redis_write_behind import DefaultWriteBehind, MySqlConnection, MySqlConnector

connection = MySqlConnection('root', 'root', '172.17.0.3:3306/test')

'''
Create MySQL persons connector
'''
personsConnector = MySqlConnector(connection, 'persons_new', 'cache_key')

personsMappings = {
	'first_name':'first',
	'last_name':'last',
	'age':'age'
}

DefaultWriteBehind(GB,  keysPrefix='person', mappings=personsMappings, connector=personsConnector, name='PersonsWriteBehind',  version='6.099.999')
