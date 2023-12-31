'''
All constants are here to make better configurable
'''

ackExpireSeconds = 3600

HASH_EVENT_TYPES = ['hset', 'hmset', 'del', 'change']
LIST_EVENT_TYPES = ['lpop', 'lpush', 'lrem', 'rpop', 'rpush', 'del', 'change']
KEY_VALUE_EVENT_TYPES = ['set', 'del', 'change']

DEFAULT_ON_FAILED_RETRY_INTERVAL = 5
DEFAULT_BATCH = 100
DEFAULT_DURATION_IN_MS = 100