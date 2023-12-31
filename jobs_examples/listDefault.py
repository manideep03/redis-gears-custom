from redis_write_behind import MySqlConnection, MySqlConnector, ListWriteBehind

connection = MySqlConnection('root', 'root', '172.17.0.3:3306/test')

keyvalueConnector = MySqlConnector(connection, 'list_value', 'cache_key')

keyvalueMappingsWithKey = {
	'set_value':'value'
}

ListWriteBehind(GB,  keysPrefix='lv/', mappings=keyvalueMappingsWithKey, connector=keyvalueConnector, name='ListTest',  version='0.0.1')