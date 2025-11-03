import redis

conexionRedis = redis.ConnectionPool(host='localhost', port=6379, db=0,decode_responses=True)
baseDatosRedis = redis.Redis(connection_pool=conexionRedis)

print("INFO 1 INDICE CONCRETO")
print(baseDatosRedis.ft("indice:usuarios").info())

print("INDICES")
indices = baseDatosRedis.execute_command('FT._LIST')
print(indices)


baseDatosRedis.close()