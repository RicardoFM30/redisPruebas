import redis

conexionRedis = redis.ConnectionPool(host='localhost', port=6379, db=0,decode_responses=True)
baseDatosRedis = redis.Redis(connection_pool=conexionRedis)

print(baseDatosRedis.get("libro_1"))

print(baseDatosRedis.get("libro_2"))

print(baseDatosRedis.get("libro_3"))

baseDatosRedis.close()

