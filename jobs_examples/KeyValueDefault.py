from redis_write_behind import KeyValueWriteBehind, MySqlConnection, MySqlConnector

connection = MySqlConnection('root', 'root', '172.17.0.3:3306/test')

keyvalueConnector = MySqlConnector(connection, 'key_value', 'cache_key')

keyvalueMappingsWithKey = {
	'set_value':'value'
}

KeyValueWriteBehind(GB,  keysPrefix='kv/', mappings=keyvalueMappingsWithKey, connector=keyvalueConnector, name='KeyValueTest',  version='0.0.4')