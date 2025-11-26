import redis
import json

from redis.commands.search.query import Query
import redis.commands.search.aggregation as aggregations

from redis.commands.json.path import Path
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.aggregation import AggregateRequest
import redis.commands.search.reducers as reducers



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
baseDatosRedis.set('actividad:est004:tiempo', 32)

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
print("#13 - Eliminar registros filtrados (solo los de prueba con prefijo est09*)")
print("====================")

# 1. Crear registros de prueba
baseDatosRedis.set('actividad:est099:tiempo', 135)
baseDatosRedis.set('actividad:est098:tiempo', 135)
baseDatosRedis.set('actividad:est097:tiempo', 135)

print("Registros creados de prueba para eliminación:")
for clave in [
    "actividad:est099:tiempo",
    "actividad:est098:tiempo",
    "actividad:est097:tiempo"
]:
    print(f"  {clave} -> {baseDatosRedis.get(clave)}")

# 2. Filtrar SOLO los registros cuyo prefijo sea actividad:est09*
claves_prueba = baseDatosRedis.keys("actividad:est09*")

print("\nClaves encontradas para eliminar:")
for clave in claves_prueba:
    print(f"  {clave}")

# 3. Eliminar los registros filtrados
print("\nEliminando registros filtrados...")
for clave in claves_prueba:
    valor = baseDatosRedis.get(clave)
    baseDatosRedis.delete(clave)
    print(f"Clave eliminada: {clave} --> Valor: {valor}")

print("\nProceso finalizado. Se eliminaron únicamente los registros actividad:est09*.")

print("\n====================")
print("#14 - Crear estructura JSON a partir de datos existentes en Redis")
print("====================")

# Crear array vacío para cada categoría
baseDatosRedis.json().set("json_data", "$", {"actividad": [], "tutoria": [], "profesor": []})

# ================================
# ACTIVIDADES DE ESTUDIANTES
# ================================
# Filtramos SOLO claves de actividad SIN incluir :tiempo
claves_actividad = sorted([
    c for c in baseDatosRedis.keys('actividad:est0??')
    if not c.endswith(':tiempo')
])

for clave in claves_actividad:
    actividad = baseDatosRedis.get(clave)
    tiempo = baseDatosRedis.get(f"{clave}:tiempo")  # tiempo asociado

    estudiante_dict = {
        "id": clave.split(":")[1],       # est001
        "actividad": actividad,          # Acceso a plataforma
        "tiempo": int(tiempo) if tiempo else None
    }

    baseDatosRedis.json().arrappend("json_data", "$.actividad", estudiante_dict)


# ================================
# TUTORÍAS
# ================================
claves_tutoria = sorted(baseDatosRedis.keys('tutoria:sesion*'))

for clave in claves_tutoria:
    if clave.endswith(":tutor"):
        continue  # evitar duplicado

    estado = baseDatosRedis.get(clave)
    tutor = baseDatosRedis.get(f"{clave}:tutor")

    tutoria_dict = {
        "sesion": clave.split(":")[1],   # sesion101
        "estado": estado,
        "tutor": tutor
    }

    baseDatosRedis.json().arrappend("json_data", "$.tutoria", tutoria_dict)


# ================================
# PROFESORES
# ================================
claves_profesor = sorted(baseDatosRedis.keys('profesor:ultimaconexion:*'))

for clave in claves_profesor:
    profesor_dict = {
        "id": clave.split(":")[2],       # prof001
        "ultima_conexion": baseDatosRedis.get(clave)
    }

    baseDatosRedis.json().arrappend("json_data", "$.profesor", profesor_dict)


# ================================
# MOSTRAR JSON FINAL
# ================================
json_final = baseDatosRedis.json().get("json_data")
print("\nJSON final generado a partir de Redis:")
print(json.dumps(json_final, indent=4, ensure_ascii=False))


print("\n====================")
print("#15 - Filtros por atributos de la estructura JSON")
print("====================")

# Obtener el JSON completo
json_data = baseDatosRedis.json().get("json_data")

# Filtrar ejemplos
actividades_mayor_50 = [a for a in json_data["actividad"] if a["tiempo"] and a["tiempo"] > 50]
print("\nActividades con tiempo > 50 minutos:")
print(json.dumps(actividades_mayor_50, indent=4, ensure_ascii=False))

tutorias_finalizadas = [t for t in json_data["tutoria"] if t["estado"] == "Finalizada"]
print("\nTutorías con estado 'Finalizada':")
print(json.dumps(tutorias_finalizadas, indent=4, ensure_ascii=False))

profesores_recientes = [p for p in json_data["profesor"] if p["ultima_conexion"] > "2025-11-19"]
print("\nProfesores con última conexión posterior al 2025-11-19:")
print(json.dumps(profesores_recientes, indent=4, ensure_ascii=False))


print("\n====================")
print("#16 - Crear listas completas en Redis")
print("====================")

# ====================
# Lista de estudiantes
# ====================
baseDatosRedis.delete("estudiantes:lista")  # Limpiar lista previa

# Filtrar solo claves de actividad de estudiantes, excluyendo tiempos
claves_actividad = sorted([c for c in baseDatosRedis.keys("actividad:est0*") if not c.endswith(":tiempo")])

for clave in claves_actividad:
    tiempo_val = baseDatosRedis.get(clave + ":tiempo")
    estudiante_dict = {
        "id": clave.split(":")[1],  # ej. est001
        "actividad": baseDatosRedis.get(clave),
        "tiempo": int(tiempo_val) if tiempo_val else None
    }
    baseDatosRedis.rpush("estudiantes:lista", json.dumps(estudiante_dict, ensure_ascii=False))

# Mostrar lista completa de estudiantes
estudiantes = [json.loads(x) for x in baseDatosRedis.lrange("estudiantes:lista", 0, -1)]
print("\nLista completa de estudiantes:")
print(json.dumps(estudiantes, indent=4, ensure_ascii=False))


# ====================
# Lista de tutorías
# ====================
baseDatosRedis.delete("tutorias:lista")  # Limpiar lista previa

# Filtrar solo sesiones principales, excluyendo claves ":tutor"
claves_tutorias = sorted([c for c in baseDatosRedis.keys("tutoria:sesion*") if not c.endswith(":tutor")])

for clave in claves_tutorias:
    tutor_val = baseDatosRedis.get(clave + ":tutor") or "Desconocido"
    tutoria_dict = {
        "sesion": clave.split(":")[1],  # ej. sesion101
        "estado": baseDatosRedis.get(clave),
        "tutor": tutor_val
    }
    baseDatosRedis.rpush("tutorias:lista", json.dumps(tutoria_dict, ensure_ascii=False))

# Mostrar lista completa de tutorías
tutorias = [json.loads(x) for x in baseDatosRedis.lrange("tutorias:lista", 0, -1)]
print("\nLista completa de tutorías:")
print(json.dumps(tutorias, indent=4, ensure_ascii=False))


# ====================
# Lista de profesores
# ====================
baseDatosRedis.delete("profesores:lista")  # Limpiar lista previa

claves_profesor = sorted(baseDatosRedis.keys("profesor:ultimaconexion:*"))

for clave in claves_profesor:
    profesor_dict = {
        "id": clave.split(":")[2],  # ej. prof001
        "ultima_conexion": baseDatosRedis.get(clave)
    }
    baseDatosRedis.rpush("profesores:lista", json.dumps(profesor_dict, ensure_ascii=False))

# Mostrar lista completa de profesores
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
print("#18 - Crear índice para los estudiantes usando JSON (eliminar y recrear)")
print("====================")

# Definir esquema para estudiantes
squema_estudiantes = (
    TextField("$.id", as_name="id"),
    TextField("$.actividad", as_name="actividad"),
    NumericField("$.tiempo", as_name="tiempo")
)

indice_nombre = "indice:estudiantes"

# Eliminar índice si existe
try:
    baseDatosRedis.ft(indice_nombre).dropindex(delete_documents=False)
    print(f"Índice '{indice_nombre}' eliminado.")
except Exception as e:
    print(f"No existía índice previo o hubo un error: {e}")

# Crear el índice nuevamente
baseDatosRedis.ft(indice_nombre).create_index(
    squema_estudiantes,
    definition=IndexDefinition(
        prefix=["estudiante:"],
        index_type=IndexType.JSON
    )
)
print(f"Índice '{indice_nombre}' creado correctamente.")

# Guardar estudiantes como JSON usando pipeline
claves_actividad = sorted([c for c in baseDatosRedis.keys("actividad:est0*") if not c.endswith(":tiempo")])

pipeline = baseDatosRedis.pipeline()
for clave in claves_actividad:
    tiempo_val = baseDatosRedis.get(clave + ":tiempo")
    estudiante_dict = {
        "id": clave.split(":")[1],
        "actividad": baseDatosRedis.get(clave),
        "tiempo": int(tiempo_val) if tiempo_val else 0
    }
    pipeline.json().set(f"estudiante:{estudiante_dict['id']}", Path.root_path(), estudiante_dict)

pipeline.execute()
print(f"{len(claves_actividad)} estudiantes guardados como JSON con índice '{indice_nombre}'.")


print("\n====================")
print("#19 - Búsqueda con índice por campo 'actividad'")
print("====================")

# Buscar estudiantes cuya actividad sea "Acceso al módulo de Matemáticas"
query = '@actividad:"Acceso al módulo de Matemáticas"'
resultados = baseDatosRedis.ft("indice:estudiantes").search(query)

print("Estudiantes con actividad 'Acceso al módulo de Matemáticas':")
for doc in resultados.docs:
    data = json.loads(doc.json)  # <-- parseamos el JSON
    print(f"{doc.id} --> id={data['id']}, actividad={data['actividad']}, tiempo={data['tiempo']}")

print("\n====================")
print("#20 - Group By por actividad: sumar tiempos y contar estudiantes")
print("====================")

# Crear request de agregación
req = aggregations.AggregateRequest("*").group_by(
    '@actividad',
    reducers.sum('@tiempo').alias('total_tiempo'),  # suma de tiempos
    reducers.count().alias('num_estudiantes')       # cuenta de estudiantes por grupo
)

# Ejecutar agregación
res = baseDatosRedis.ft("indice:estudiantes").aggregate(req)

# Mostrar resultados convertidos a diccionario
print("Resumen por actividad:")
for row in res.rows:
    # Cada row viene como lista: ['clave1', valor1, 'clave2', valor2, ...]
    row_dict = {row[i]: row[i+1] for i in range(0, len(row), 2)}
    print(f"Actividad: {row_dict['actividad']}")
    print(f"  Total tiempo: {row_dict['total_tiempo']}")
    print(f"  Número de estudiantes: {row_dict['num_estudiantes']}\n")


# Cerrar conexión
baseDatosRedis.close()
