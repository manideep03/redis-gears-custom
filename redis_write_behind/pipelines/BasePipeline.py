from redisgears import executeCommand as execute
from redis_write_behind.utils.basic_utils import *
import json

def SafeDeleteKey(key):
    '''
    Deleting a key by first renaming it so we will not trigger another execution
    If key does not exists we will get an execution and ignore it
    '''
    try:
        newKey = '__{%s}__' % key
        execute('RENAME', key, newKey)
        execute('DEL', newKey)
    except Exception:
        pass

def DeleteDataIfNeeded(r):
    key = r['key']
    operation = r['value'][OP_KEY]
    if operation == OPERATION_DEL_REPLICATE:
        SafeDeleteKey(key)

def ShouldProcessData(r):
    key = r['key']
    value = r['value']
    uuid = value[UUID_KEY]
    operation = value[OP_KEY]
    res = True

    if operation == OPERATION_DEL_NOREPLICATE:
        # we need to just delete the key but delete it directly will cause
        # key unwanted key space notification so we need to rename it first
        SafeDeleteKey(key)
        res = False

    if operation == OPERATION_UPDATE_NOREPLICATE:
        res = False

    if not res and uuid != '':
        # no replication to connector is needed but ack is require
        idToAck = '{%s}%s' % (key, uuid)
        execute('XADD', idToAck, '*', 'status', 'done')
        execute('EXPIRE', idToAck, ackExpireSeconds)

    return res

def RegistrationArrToDict(registration, depth):
    if depth >= 2:
        return registration
    if type(registration) is not list:
        return registration
    d = {}
    for i in range(0, len(registration), 2):
        d[registration[i]] = RegistrationArrToDict(registration[i + 1], depth + 1)
    return d

def CompareVersions(v1, v2):
    # None version is less then all version
    if v1 is None:
        return -1
    if v2 is None:
        return 1

    if v1 == '99.99.99':
        return 1
    if v2 == '99.99.99':
        return -1

    v1_major, v1_minor, v1_patch = v1.split('.')
    v2_major, v2_minor, v2_patch = v2.split('.')

    if int(v1_major) > int(v2_major):
        return 1
    elif int(v1_major) < int(v2_major):
        return -1

    if int(v1_minor) > int(v2_minor):
        return 1
    elif int(v1_minor) < int(v2_minor):
        return -1

    if int(v1_patch) > int(v2_patch):
        return 1
    elif int(v1_patch) < int(v2_patch):
        return -1

    return 0

def UnregisterOldVersions(name, version):
    WriteBehindLog('Unregistering old versions of %s' % name)
    registrations = execute('rg.dumpregistrations')
    for registration in registrations:
        registrationDict = RegistrationArrToDict(registration, 0)
        descStr = registrationDict['desc']
        try:
            desc = json.loads(descStr)
        except Exception as e:
            continue
        if 'name' in desc.keys() and name in desc['name']:
            WriteBehindLog('Version auto upgrade is not atomic, make sure to use it when there is not traffic to the database (otherwise you might lose events).', logLevel='warning')
            if 'version' not in desc.keys():
                execute('rg.unregister', registrationDict['id'])
                WriteBehindLog('Unregistered %s' % registrationDict['id'])
                continue
            v = desc['version']
            if CompareVersions(version, v) > 0:
                execute('rg.unregister', registrationDict['id'])
                WriteBehindLog('Unregistered %s' % registrationDict['id'])
            else:
                raise Exception('Found a version which is greater or equals current version, aborting.')
    WriteBehindLog('Unregistered old versions')

def CreateAddToStreamFunction(self, primaryCacheKey, keysPrefix):
    def func(r):
        data = []
        data.append([ORIGINAL_KEY, r['key']])
        if primaryCacheKey:
            data.append([self.connector.PrimaryKey(), r['key']])
        else:
            # after key prefix value
            data.append([self.connector.PrimaryKey(), r['key'].split(keysPrefix)[-1]])
        if 'value' in r.keys():
            value = r['value']
            uuid = value.pop(UUID_KEY, None)
            op = value[OP_KEY]
            data.append([OP_KEY, op])
            keys = value.keys()
            if uuid is not None:
                data.append([UUID_KEY, uuid])
            if op == OPERATION_UPDATE_REPLICATE:
                for kInHash, kInDB in self.mappings.items():
                    if kInHash.startswith('_'):
                        continue
                    if kInHash not in keys:
                        msg = 'AddToStream: Could not find %s in hash %s' % (kInHash, r['key'])
                        WriteBehindLog(msg)
                        raise Exception(msg)
                    data.append([kInDB, value[kInHash]])
        execute('xadd', self.GetStreamName(self.connector.TableName()), '*', *sum(data, []))
    return func

def CreateWriteDataFunction(connector, dataKey=None):
    def func(data):
        idsToAck = []
        for d in data:
            originalKey = d['value'].pop(ORIGINAL_KEY, None)
            uuid = d['value'].pop(UUID_KEY, None)
            if uuid is not None and uuid != '':
                idsToAck.append('{%s}%s' % (originalKey, uuid))

        # specifically, to not updating all the old WriteData calls
        # due to JSON
        if dataKey is None:
            connector.WriteData(data)
        else:
            connector.WriteData(data, dataKey)

        for idToAck in idsToAck:
            execute('XADD', idToAck, '*', 'status', 'done')
            execute('EXPIRE', idToAck, ackExpireSeconds)

    return func

class RGWriteBase():
    def __init__(self, mappings, connector, name, version=None):
        UnregisterOldVersions(name, version)

        self.connector = connector
        self.mappings = mappings

        try:
            self.connector.PrepereQueries(self.mappings)
        except Exception as e:
            # cases like mongo, that don't implement this, silence the warning
            if "object has no attribute 'PrepereQueries'" in str(e):
                return
            WriteBehindLog('Skip calling PrepereQueries of connector, err="%s"' % str(e))

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

primaryCacheKey - If True primary key will be whole cache key, If False primary key will be a part of cache key

batch - the batch size on which data will be writen to target

duration - interval in ms in which data will be writen to target even if batch size did not reached

onFailedRetryInterval - Interval on which to performe retry on failure.

transform - A function that accepts as input a redis record and returns a hash

eventTypes - The events for which to trigger
'''