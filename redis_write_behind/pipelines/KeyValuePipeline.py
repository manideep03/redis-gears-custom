from redisgears import executeCommand as execute
from redis_write_behind.utils.basic_utils import *
from redis_write_behind.pipelines.BasePipeline import *
import json
import uuid

def ValidateKeyValue(r):
    key = r['key']
    value = r['value']

    if value == None:
        # key without value consider delete
        value = {OP_KEY : OPERATION_DEL_REPLICATE}
        r['value'] = value
    else:
        # make sure its a hash
        if not (isinstance(r['value'], str)) :
            msg = 'Got a none string value, key="%s" value="%s"' % (str(r['key']), str(r['value'] if 'value' in r.keys() else 'None'))
            WriteBehindLog(msg)
            raise Exception(msg)
        # confirmned value is not empty
        r['value'] = {SET_DEFAULT_KEY : r['value']}
        value = r['value']

        if OP_KEY not in value.keys():
            value[OP_KEY] = defaultOperation
        else:
            # we need to delete the operation key for the hash
            execute('del', key, OP_KEY)

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

class KeyValueWriteBehind(RGWriteBase):
    def __init__(self, GB, keysPrefix, mappings, connector, name, version=None, primaryCacheKey=True,
                 onFailedRetryInterval=DEFAULT_FAILED_RETRY_INTERVAL, batch=DEFAULT_BATCH, duration=DEFAULT_DURATION_IN_MS, 
                 transform=lambda r: r, 
                 eventTypes=['set', 'del', 'change']):
        
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
        filter(ValidateKeyValue).\
        filter(ShouldProcessData).\
        foreach(DeleteDataIfNeeded).\
        foreach(CreateAddToStreamFunction(self, primaryCacheKey, keysPrefix)).\
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