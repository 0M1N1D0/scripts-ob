"""
Basándose en el id_usuario, toma la columna del archivo de nombre 
mail y actualiza la DB insertando el mail correspondiente a cada
usuario. Si el usuario existe, se actualiza el mail, si no existe
se inserta.

"""

import pandas as pd
import psycopg2


# lee el archivo csv con pandas
df = pd.read_csv('actualizacion_correo.csv')

# se crea conexion a la base de datos con postgresql
conn = psycopg2.connect(
    host='localhost',
    database='produccion',
    user='postgres',
    password='2010'
)

# se crea un cursor para ejecutar las sentencias
cur = conn.cursor()

# el df solo se queda con las columnas id_usuario y mail
df = df[['id_usuario', 'correo']]

# actualiza la tabla usuarios con el mail correspondiente

for i in range(len(df)):
    try:
        cur.execute(
            f"UPDATE usuarios SET correo = '{df.iloc[i, 1]}' WHERE id_usuario = {df.iloc[i, 0]}"
        )
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()


# se cierra la conexión
cur.close()






