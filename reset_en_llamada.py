import psycopg2
from psycopg2 import sql

try:
    # Conecta a la base de datos
    conn = psycopg2.connect(
        host='10.1.10.83',
        database='produccion',
        user='postgres',
        password='Desar0ll0'
    )

    cur = conn.cursor()

    # Actualiza la tabla registro_llamadas, estableciendo en_llamada a False
    query = sql.SQL("UPDATE registro_llamadas SET en_llamada = %s")
    cur.execute(query, (False,))

    # Confirma los cambios
    conn.commit()

except Exception as e:
    print(f"Ocurrió un error: {e}")
    if conn:
        conn.rollback()
finally:
    if cur:
        cur.close()
    if conn:
        conn.close()
