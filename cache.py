import redis

_redis_port = 6379

r = redis.StrictRedis(host='localhost', port=_redis_port, db=0)

def saveVClock(fileName, vClock):
    key = fileName
    r.set(key,str(vClock))

def keyExists(key):
    return r.exists(key)

def getFileVclock(filename):
    return r.get(filename).decode('utf-8')