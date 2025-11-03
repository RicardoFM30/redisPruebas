import redis

conexionRedis = redis.ConnectionPool(host='localhost', port=6379, db=0,decode_responses=True)
baseDatosRedis = redis.Redis(connection_pool=conexionRedis)

res1 = baseDatosRedis.json().set("usuarios:1", "$", {"nombre": "Jorge", "apellido": "Baron", "edad": 37})
res2 = baseDatosRedis.json().set("usuarios:2", "$", {"nombre": "Lucía", "apellido": "Benitez", "edad": 24})

res11 = baseDatosRedis.json().set("peña:1", "$", {"nombre": "Jorge", "apellido": "Baron", "edad": 37})
res22 = baseDatosRedis.json().set("peña:2", "$", {"nombre": "Lucía", "apellido": "Benitez", "edad": 24})

baseDatosRedis.json().set("usuarios_array", "$", [])
res3 = baseDatosRedis.json().arrappend("usuarios_array", "$", {"nombre": "Pepe", "apellido": "Sanchez", "edad": 45})
res4 = baseDatosRedis.json().arrappend("usuarios_array", "$", {"nombre": "Calisto", "apellido": "Melibea", "edad": 67})

baseDatosRedis.close()


