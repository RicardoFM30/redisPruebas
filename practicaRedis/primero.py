import redis
import json

from redis.commands.json.path import Path
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType

# Conexión a Redis
conexionRedis = redis.ConnectionPool(
    host='localhost',
    port=6379,
    db=0,
    decode_responses=True
)
baseDatosRedis = redis.Redis(connection_pool=conexionRedis)

print("\n====================")
print("#1 - Crear registros clave-valor")
print("====================")
# Registro actividad en plataforma web
baseDatosRedis.set('actividad:est001', 'Acceso a plataforma', ex=900)
baseDatosRedis.set('actividad:est001:tiempo', 35)

baseDatosRedis.set('actividad:est002', 'Envió tarea', ex=900)
baseDatosRedis.set('actividad:est002:tiempo', 48)

baseDatosRedis.set('actividad:est003', 'Acceso al módulo de Matemáticas', ex=900)
baseDatosRedis.set('actividad:est003:tiempo', 78)

baseDatosRedis.set('actividad:est004', 'Acceso al módulo de Matemáticas', ex=900)
baseDatosRedis.set('actividad:est004:tiempo', 12)

baseDatosRedis.set('actividad:est005', 'Acceso al módulo de Química', ex=900)
baseDatosRedis.set('actividad:est005:tiempo', 125)

baseDatosRedis.set('actividad:est006', 'Acceso a los ajustes del perfil', ex=900)
baseDatosRedis.set('actividad:est006:tiempo', 34)

# Estado de tutorías en la plataforma
baseDatosRedis.set('tutoria:sesion101', 'En progreso', ex=300)
baseDatosRedis.set('tutoria:sesion101:tutor', 'Jorge Agustín')

baseDatosRedis.set('tutoria:sesion102', 'Finalizada', ex=300)
baseDatosRedis.set('tutoria:sesion102:tutor', 'José Antonio')

baseDatosRedis.set('tutoria:sesion103', 'Finalizada', ex=300)
baseDatosRedis.set('tutoria:sesion103:tutor', 'José Antonio')

# Otros datos de la plataforma
baseDatosRedis.set('profesor:ultimaconexion:prof001', '2025-11-18 10:56:02')
baseDatosRedis.set('profesor:ultimaconexion:prof002', '2025-11-19 20:00:12')
baseDatosRedis.set('profesor:ultimaconexion:prof003', '2025-11-19 17:03:21')
baseDatosRedis.set('profesor:ultimaconexion:prof004', '2025-11-20 16:23:51')
baseDatosRedis.set('profesor:ultimaconexion:prof012', '2025-11-19 11:23:51')

valor = baseDatosRedis.get('actividad:est001')
print(f"Valor de 'actividad:est001': {valor}\n")


print("\n====================")
print("#2 - Obtener y mostrar el número de claves registradas")
print("====================")
claves = baseDatosRedis.keys()
print(f"Número de claves: {len(claves)}")


print("\n====================")
print("#3 - Obtener un registro en base a una clave")
print("====================")
clave = 'actividad:est001'
valor = baseDatosRedis.get(clave)
print(f"El valor en la clave '{clave}' es: '{valor}'")


print("\n====================")
print("#4 - Actualizar el valor de una clave")
print("====================")
print(f"Antiguo valor en '{clave}': '{valor}'")
nuevo_valor = 'Ingreso a módulo de Física'
baseDatosRedis.set(clave, nuevo_valor)
valor_actualizado = baseDatosRedis.get(clave)
print(f"Nuevo valor en '{clave}': '{valor_actualizado}'")


print("\n====================")
print("#5 - Eliminar una clave-valor")
print("====================")
claveBorrar = 'actividad:est006'
valor_eliminado = baseDatosRedis.get(claveBorrar)
baseDatosRedis.delete(claveBorrar)
print(f"Clave '{claveBorrar}' y valor '{valor_eliminado}' eliminados")


print("\n====================")
print("#6 - Obtener todas las claves")
print("====================")
claves = baseDatosRedis.keys()
for clave in claves:
    print(clave)


print("\n====================")
print("#7 - Obtener todos los valores")
print("====================")
for clave in claves:
    tipo = baseDatosRedis.type(clave)  # ya es string
    
    if tipo == 'string':
        valor = baseDatosRedis.get(clave)
    elif tipo == 'list':
        valor = baseDatosRedis.lrange(clave, 0, -1)
    elif tipo == 'hash':
        valor = baseDatosRedis.hgetall(clave)
    else:
        valor = f"Tipo {tipo} no manejado"
    
    print(f"Clave: {clave}, Tipo: {tipo}, Valor: {valor}")


print("\n====================")
print("#8 - Obtener registros con patrón '*'")
print("====================")
print(f"El patrón es 'actividad:*'")
claves_actividad = baseDatosRedis.keys('actividad:*')
for clave in claves_actividad:
    print(f"{clave} --> {baseDatosRedis.get(clave)}")


print("\n====================")
print("#9 - Obtener registros con patrón '[]'")
print("====================")
print(f"El patrón es 'actividad:est00[1-5]'")
claves_rango = baseDatosRedis.keys('actividad:est00[1-5]')
for clave in claves_rango:
    print(f"{clave} --> {baseDatosRedis.get(clave)}")


print("\n====================")
print("#10 - Obtener registros con patrón '?'")
print("====================")
print(f"El patrón es 'profesor:ultimaconexion:prof00?'")
claves_comodin = baseDatosRedis.keys('profesor:ultimaconexion:prof00?')
for clave in claves_comodin:
    print(f"{clave} --> {baseDatosRedis.get(clave)}")


print("\n====================")
print("#11 - Filtrar registros por valor 'Finalizada'")
print("====================")
claves_tutorias = baseDatosRedis.keys('tutoria:*')
for clave in claves_tutorias:
    valor = baseDatosRedis.get(clave)
    if valor == 'Finalizada':
        print(f"{clave} --> {valor}")


print("\n====================")
print("#12 - Actualizar registros filtrados (sumar 10 al tiempo)")
print("====================")
claves_tiempo = baseDatosRedis.keys('actividad:*:tiempo')
for clave in claves_tiempo:
    valor_actual = baseDatosRedis.get(clave)
    if valor_actual is not None:
        nuevo_valor = int(valor_actual) + 10
        baseDatosRedis.set(clave, nuevo_valor)
        print(f"{clave}: {valor_actual} --> {nuevo_valor}")


print("\n====================")
print("#13 - Eliminar registros filtrados")
print("====================")
claves_tiempo = baseDatosRedis.keys('actividad:*:tiempo')
for clave in claves_tiempo:
    valor = baseDatosRedis.get(clave)
    baseDatosRedis.delete(clave)
    print(f"Clave eliminada: {clave} --> Valor eliminado: {valor}")
"""
print("\n====================")
print("#14 - Crear estructura JSON a partir de datos existentes en Redis")
print("====================")

# Crear array vacío para cada categoría
baseDatosRedis.json().set("json_data", "$", {"actividad": [], "tutoria": [], "profesor": []})

# Actividades de estudiantes
claves_actividad = baseDatosRedis.keys('actividad:est00[1-6]')
for clave in claves_actividad:
    actividad = baseDatosRedis.get(clave)
    tiempo = baseDatosRedis.get(clave + ":tiempo")
    estudiante_dict = {
        "id": clave.split(":")[1],
        "actividad": actividad,
        "tiempo": int(tiempo) if tiempo else None
    }
    baseDatosRedis.json().arrappend("json_data", "$.actividad", estudiante_dict)

# Tutorías
claves_tutoria = baseDatosRedis.keys('tutoria:sesion*')
for clave in claves_tutoria:
    if clave.endswith(":tutor"):
        continue
    estado = baseDatosRedis.get(clave)
    tutor = baseDatosRedis.get(clave + ":tutor")
    tutoria_dict = {
        "sesion": clave.split(":")[1],
        "estado": estado,
        "tutor": tutor
    }
    baseDatosRedis.json().arrappend("json_data", "$.tutoria", tutoria_dict)

# Profesores
claves_profesor = baseDatosRedis.keys('profesor:ultimaconexion:*')
for clave in claves_profesor:
    profesor_dict = {
        "id": clave.split(":")[2],
        "ultima_conexion": baseDatosRedis.get(clave)
    }
    baseDatosRedis.json().arrappend("json_data", "$.profesor", profesor_dict)

# Mostrar JSON final
json_final = baseDatosRedis.json().get("json_data")
print("\nJSON final generado a partir de Redis:")
print(json.dumps(json_final, indent=4))


print("\n====================")
print("#15 - Filtros por atributos de la estructura JSON")
print("====================")

# Obtener el JSON completo
json_data = baseDatosRedis.json().get("json_data")

# Filtrar ejemplos
actividades_mayor_50 = [a for a in json_data["actividad"] if a["tiempo"] and a["tiempo"] > 50]
print("\nActividades con tiempo > 50 minutos:")
print(json.dumps(actividades_mayor_50, indent=4))

tutorias_finalizadas = [t for t in json_data["tutoria"] if t["estado"] == "Finalizada"]
print("\nTutorías con estado 'Finalizada':")
print(json.dumps(tutorias_finalizadas, indent=4))

profesores_recientes = [p for p in json_data["profesor"] if p["ultima_conexion"] > "2025-11-19"]
print("\nProfesores con última conexión posterior al 2025-11-19:")
print(json.dumps(profesores_recientes, indent=4))
"""

print("\n====================")
print("#16 - Crear listas completas en Redis")
print("====================")

# ====================
# Lista de estudiantes completa
# ====================
baseDatosRedis.delete("estudiantes:lista")  # limpiar si existía

claves_actividad = baseDatosRedis.keys("actividad:est00*")
for clave in claves_actividad:
    estudiante_dict = {
        "id": clave.split(":")[1],
        "actividad": baseDatosRedis.get(clave),
        "tiempo": int(baseDatosRedis.get(clave + ":tiempo")) if baseDatosRedis.get(clave + ":tiempo") else None
    }
    baseDatosRedis.rpush("estudiantes:lista", json.dumps(estudiante_dict))

# Mostrar lista completa
estudiantes = [json.loads(x) for x in baseDatosRedis.lrange("estudiantes:lista", 0, -1)]
print("\nLista completa de estudiantes:")
print(json.dumps(estudiantes, indent=4, ensure_ascii=False))

# ====================
# Lista de tutorías completa
# ====================
baseDatosRedis.delete("tutorias:lista")

claves_tutorias = baseDatosRedis.keys("tutoria:sesion*")
for clave in claves_tutorias:
    if clave.endswith(":tutor"):
        continue
    tutoria_dict = {
        "sesion": clave.split(":")[1],
        "estado": baseDatosRedis.get(clave),
        "tutor": baseDatosRedis.get(clave + ":tutor")
    }
    baseDatosRedis.rpush("tutorias:lista", json.dumps(tutoria_dict))

tutorias = [json.loads(x) for x in baseDatosRedis.lrange("tutorias:lista", 0, -1)]
print("\nLista completa de tutorías:")
print(json.dumps(tutorias, indent=4, ensure_ascii=False))

# ====================
# Lista de profesores completa
# ====================
baseDatosRedis.delete("profesores:lista")

claves_profesor = baseDatosRedis.keys("profesor:ultimaconexion:*")
for clave in claves_profesor:
    profesor_dict = {
        "id": clave.split(":")[2],
        "ultima_conexion": baseDatosRedis.get(clave)
    }
    baseDatosRedis.rpush("profesores:lista", json.dumps(profesor_dict))

profesores = [json.loads(x) for x in baseDatosRedis.lrange("profesores:lista", 0, -1)]
print("\nLista completa de profesores:")
print(json.dumps(profesores, indent=4, ensure_ascii=False))

print("\n====================")
print("#17 - Obtener elementos de listas con filtro")
print("====================")

# ====================
# Filtrar estudiantes cuya actividad sea "Acceso al módulo de Matemáticas"
# ====================
estudiantes_matematicas = [
    json.loads(x) for x in baseDatosRedis.lrange("estudiantes:lista", 0, -1)
    if json.loads(x)["actividad"] == "Acceso al módulo de Matemáticas"
]

print("\nEstudiantes con actividad 'Acceso al módulo de Matemáticas':")
print(json.dumps(estudiantes_matematicas, indent=4, ensure_ascii=False))

# ====================
# Filtrar tutorías que estén 'Finalizada'
# ====================
tutorias_finalizadas = [
    json.loads(x) for x in baseDatosRedis.lrange("tutorias:lista", 0, -1)
    if json.loads(x)["estado"] == "Finalizada"
]
print("\nTutorías con estado 'Finalizada':")
print(json.dumps(tutorias_finalizadas, indent=4, ensure_ascii=False))

# ====================
# Filtrar profesores con última conexión posterior al 2025-11-19
# ====================
profesores_recientes = [
    json.loads(x) for x in baseDatosRedis.lrange("profesores:lista", 0, -1)
    if json.loads(x)["ultima_conexion"] > "2025-11-19"
]
print("\nProfesores con última conexión posterior al 2025-11-19:")
print(json.dumps(profesores_recientes, indent=4, ensure_ascii=False))

print("\n====================")
print("#18 - Crear índice para los estudiantes usando JSON")
print("====================")

# Definir esquema para estudiantes
squema_estudiantes = (
    TextField("$.id", as_name="id"),
    TextField("$.actividad", as_name="actividad"),
    NumericField("$.tiempo", as_name="tiempo")
)

# Crear índice
try:
    baseDatosRedis.ft("indice:estudiantes").create_index(
        squema_estudiantes,
        definition=IndexDefinition(
            prefix=["estudiante:"],
            index_type=IndexType.JSON
        )
    )
except Exception as e:
    print("El índice ya existe o hubo un error:", e)

# Guardar los estudiantes como JSON
claves_actividad = baseDatosRedis.keys("actividad:est00*")
for clave in claves_actividad:
    estudiante_dict = {
        "id": clave.split(":")[1],
        "actividad": baseDatosRedis.get(clave),
        "tiempo": int(baseDatosRedis.get(clave + ":tiempo")) if baseDatosRedis.get(clave + ":tiempo") else 0
    }
    baseDatosRedis.json().set(f"estudiante:{estudiante_dict['id']}", Path.root_path(), estudiante_dict)

print("Estudiantes guardados como JSON con índice.")

print("\n====================")
print("#19 - Búsqueda con índice por campo 'actividad'")
print("====================")

# Buscar estudiantes cuya actividad sea "Acceso al módulo de Matemáticas"
query = '@actividad:"Acceso al módulo de Matemáticas"'
resultados = baseDatosRedis.ft("indice:estudiantes").search(query)

print("Estudiantes con actividad 'Acceso al módulo de Matemáticas':")
for doc in resultados.docs:
    print(f"{doc.id} --> id={doc.id}, actividad={doc.actividad}, tiempo={doc.tiempo}")

print("\n====================")
print("#20 - Group By por actividad y sumar tiempos")
print("====================")

from redis.commands.search.aggregation import AggregateRequest, reducers

# Agrupar por actividad y sumar tiempos
req = AggregateRequest("*").group_by("@actividad", reducers.Sum("tiempo").alias("total_tiempo"))
res = baseDatosRedis.ft("indice:estudiantes").aggregate(req)

print("Suma de tiempo por actividad:")
for row in res.rows:
    print(row)
# Cerrar conexión
baseDatosRedis.close()
