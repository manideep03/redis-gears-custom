from redis_write_behind import WBJobBuilder

c = {
    "name" : "TestNewVersion",
    "version":"0.0.7",
    "keysPrefix":"newPrefix/",
    "targetDataType": "hash",
    "primaryCacheKey": 1,
    "dbConfig" : {
        "host":"172.17.0.2:3306/test",
        "user":"root",
        "password":"root",
        "tableName" : "v2_table",
        "primaryKey": "cache_key"
    },
    "keysMapping" : {"value":"value"}
}

WBJobBuilder(GB, c).create()