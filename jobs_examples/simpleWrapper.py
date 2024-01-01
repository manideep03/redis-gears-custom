import redis

r = redis.Redis()

def runScript(filepath):
    with open(filepath, 'rt') as f:
        script = f.read()
    q = ['rg.pyexecute', script]

    reply = r.execute_command(*q)
    print("--------------------- reply here")
    print(reply)

runScript('script2.py')