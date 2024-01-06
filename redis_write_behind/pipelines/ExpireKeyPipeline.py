import uuid
from redisgears import executeCommand as execute
from redis_write_behind.utils.basic_utils import *
import json

def LogDataFun(r):
    key = r['key']
    value = r['value']
    msg = "Key is "+str(key)+" - value is "+str(value)
    WriteBehindLog(msg=msg)
    return True

class TrackExpireKey():
    def __init__(self, name, version):        
        UUID = str(uuid.uuid4())
        self.GetStreamName = CreateGetStreamNameCallback(UUID)

        ## create the execution to write each changed key to stream
        descJson = {
            'name':'%s.KeysReader' % name,
            'version':version,
            'desc':'Testing key expire event in redis',
        }
        GB('KeysReader', desc=json.dumps(descJson)).\
        map(lambda r: r).\
        foreach(LogDataFun).\
        register(mode='sync', prefix="*", eventTypes=['expired'], convertToStr=False)