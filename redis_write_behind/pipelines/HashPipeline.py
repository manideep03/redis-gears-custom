from redis_write_behind.utils.basic_utils import *
from redis_write_behind.pipelines.PipelinesBase import *
from redis_write_behind.pipelines.constants import *

from redisgears import executeCommand as execute
import json
import uuid

def ValidateHash(r):
    key = r['key']
    value = r['value']

    if value == None:
        # key without value consider delete
        value = {OP_KEY : OPERATION_DEL_REPLICATE}
        r['value'] = value
    else:
        # make sure its a hash
        if not (isinstance(r['value'], dict)) :
            msg = 'Got a none hash value, key="%s" value="%s"' % (str(r['key']), str(r['value'] if 'value' in r.keys() else 'None'))
            WriteBehindLog(msg)
            raise Exception(msg)
        if OP_KEY not in value.keys():
            value[OP_KEY] = defaultOperation
        else:
            # we need to delete the operation key for the hash
            execute('hdel', key, OP_KEY)

    op = value[OP_KEY]
    if len(op) == 0:
        msg = 'Got no operation'
        WriteBehindLog(msg)
        raise Exception(msg)

    operation = op[0]

    if operation not in OPERATIONS:
        msg = 'Got unknown operations "%s"' % operation
        WriteBehindLog(msg)
        raise Exception(msg)

    # lets extrac uuid to ack on
    uuid = op[1:]
    value[UUID_KEY] = uuid if uuid != '' else None
    value[OP_KEY] = operation

    r['value'] = value

    return True

class HashWriteBehind(RGWriteBase):
    def __init__(self, GB, keysPrefix, mappings, connector, name, version=None, 
                 primaryCacheKey=False,
                 onFailedRetryInterval=DEFAULT_ON_FAILED_RETRY_INTERVAL, 
                 batch=DEFAULT_BATCH, 
                 duration=DEFAULT_DURATION_IN_MS, 
                 transform=lambda r: r, 
                 eventTypes=HASH_EVENT_TYPES):
        '''
        Register a write behind execution to redis gears

        GB - The Gears builder object

        keysPrefix - Prefix on keys to register on

        mappings - a dictionary in the following format
            {
                'name-on-redis-hash1':'name-on-connector-table1',
                'name-on-redis-hash2':'name-on-connector-table2',
                .
                .
                .
            }

        connector - a connector object that implements the following methods
            1. TableName() - returns the name of the table to write the data to
            2. PrimaryKey() - returns the name of the public key of the relevant table
            3. PrepereQueries(mappings) - will be called at start to allow the connector to
                prepare the queries. This function is not mandatory and will be called only
                if exists.
            4. WriteData(data) -
                data is a list of dictionaries of the following format

                    {
                        'streamId':'value'
                        'name-of-column':'value-of-column',
                        'name-of-column':'value-of-column',
                        .
                        .

                    }

                The streamId is a unique id of the dictionary and can be used by the
                connector to achieve exactly once property. The idea is to write the
                last streamId of a batch into another table. When new connection
                established, this streamId should be read from the database and
                data with lower stream id should be ignored
                The stream id is in a format of '<timestamp>-<increasing counter>' so there
                is a total order between all streamIds

                The WriteData function should write all the entries in the list to the database
                and return. On error it should raise exception.

                This function should ignore keys that starts with '_'

        name - The name of the created registration. This name will be used to find old version
               and remove them.

        version - The version to set to the new created registration. Old versions with the same
               name will be removed. 99.99.99 is greater then any other version (even from itself).

        primaryCacheKey - If True The key in redis will be supplied as primary key.
                          If False part of key will be supplied as primary key.

        batch - the batch size on which data will be writen to target

        duration - interval in ms in which data will be writen to target even if batch size did not reached

        onFailedRetryInterval - Interval on which to performe retry on failure.

        transform - A function that accepts as input a redis record and returns a hash

        eventTypes - The events for which to trigger
        '''

        UUID = str(uuid.uuid4())
        self.GetStreamName = CreateGetStreamNameCallback(UUID)

        RGWriteBase.__init__(self, mappings, connector, name, version)

        ## create the execution to write each changed key to stream
        descJson = {
            'name':'%s.KeysReader' % name,
            'version':version,
            'desc':'add each changed key with prefix %s:* to Stream' % keysPrefix,
        }
        GB('KeysReader', desc=json.dumps(descJson)).\
        map(transform).\
        filter(ValidateHash).\
        filter(ShouldProcessData).\
        foreach(DeleteDataIfNeeded).\
        foreach(CreateAddToStreamFunction(self, primaryCacheKey)).\
        register(mode='sync', prefix='%s*' % keysPrefix, eventTypes=eventTypes, convertToStr=False)

        ## create the execution to write each key from stream to DB
        descJson = {
            'name':'%s.StreamReader' % name,
            'version':version,
            'desc':'read from stream and write to DB table %s' % self.connector.TableName(),
        }
        GB('StreamReader', desc=json.dumps(descJson)).\
        aggregate([], lambda a, r: a + [r], lambda a, r: a + r).\
        foreach(CreateWriteDataFunction(self.connector)).\
        count().\
        register(prefix='_%s-stream-%s-*' % (self.connector.TableName(), UUID),
                 mode="async_local",
                 batch=batch,
                 duration=duration,
                 onFailedPolicy="retry",
                 onFailedRetryInterval=onFailedRetryInterval,
                 convertToStr=False)
