from psycopg2.extras import execute_values
import os
import pandas as pd
import psycopg2


# region Clase Database
class Database:

    def __init__(self):
        self.nombre_db = 'produccion'
        self.usuario = 'postgres'
        self.password = 'Desar0ll0'
        self.host = '10.1.10.83'
        self.puerto = '5432'
        self.conexion = None

    def conectar(self):
        try:
            self.conexion = psycopg2.connect(
                dbname=self.nombre_db,
                user=self.usuario,
                password=self.password,
                host=self.host,
                port=self.puerto
            )
            
        except Exception as e:
            print("Error al conectar a la base de datos: ", e)


    def desconectar(self):
        if self.conexion:
            self.conexion.close()
            print("Desconexión exitosa")

    # TODO: eliminar este método
    def ejecutar_query(self, query, parametros=None):
        cursor = self.conexion.cursor()
        try:
            cursor.execute(query, parametros)
            self.conexion.commit()
            if query.strip().upper().startswith("SELECT"):
                return cursor.fetchall()  # Devuelve los resultados solo para consultas SELECT
            elif query.strip().upper().startswith("INSERT"):
                id = cursor.fetchone()[0]
                return {"numero_de_registro_creado":id}
            elif query.strip().upper().startswith("UPDATE"):
                return {"mensaje":"actualización realizada exitosamente"}
        except Exception as e:
            self.conexion.rollback()
            raise e
        finally:
            cursor.close()

# endregion



# region Clase ProcesaArchivoCampania

class ProcesaArchivoCampania:

    def __init__(self, df_chunk, id_campania):

        self.df_chunk = df_chunk
        self.id_campania = id_campania
        self.db = Database()
        self.db.conectar()
        self.conn = self.db.conexion
        self.procesar()


    def procesar(self):

        try:
            df_datos_personales, df_datos_detalle = ProcesaArchivoCampania.\
                separa_datos(self.df_chunk)
        except Exception as e:
            raise Exception(f"Error al separar los datos: {e}")

        df_datos_personales['nombre'] = df_datos_personales['nombre'].\
            str.replace(',', '')
        # df_datos_personales.fillna('', inplace=True)

        try:
            df_datos_detalle = ProcesaArchivoCampania.\
                melt_df_detalle(df_datos_detalle)
        except Exception as e:
            raise Exception(f"Error al hacer melt en detalles: {e}")
        
        try:
            df_datos_personales2 = self.procesa_datos_personales(df_datos_personales)
        except Exception as e:
            raise Exception(f"Error al procesar los datos personales: {e}")

        try:
            self.cargar_datos_personales(df_datos_personales2)
        except Exception as e:
            raise Exception(f"Error al cargar los datos personales: {e}")

        try:
            df_id_campania_eos = self.carga_campania_eos(df_datos_personales, 
                                                         self.id_campania)
        except Exception as e:
            raise Exception(f"Error al cargar campania eos. {e}")

        # une df_datos_detalle con df_id_campania_eos por id_empresario
        df_datos_detalle = df_datos_detalle.merge(df_id_campania_eos, 
                                                  on='id_empresario', 
                                                  how='inner')

        try:
            self.cargar_datos_detalle(df_datos_detalle)
        except Exception as e:
            raise Exception(f"Error al cargar los detalles de los contactos: {e}")

        try:
            self.carga_campania_paises(df_datos_personales, self.id_campania)
        except Exception as e:
            raise Exception(f"Error al cargar los paises de la campaña: {e}")



    @staticmethod
    def separa_datos(df):
        df_datos_personales = df.iloc[:, :10]
        df_datos_detalle = df.iloc[:, 10:].copy()
        df_datos_detalle.insert(0, df_datos_personales.columns[0], df_datos_personales.iloc[:, 0])
        return df_datos_personales, df_datos_detalle


    @staticmethod
    def melt_df_detalle(df):
        print(df)
        # realiza un melt del dataframe df en las columnas nombre_detalle y valor_detalle
        # conserva la columna id_empresario
        df = df.melt(id_vars='id_empresario', var_name='nombre_detalle', value_name='valor_detalle')
        return df
        

    def cargar_datos_personales(self, df):

        # Convierte las columnas id_empresario, nombre, mail, telefono, celular y sexo a string
        df['id_empresario'] = df['id_empresario'].fillna('').astype(str)
        df['nombre'] = df['nombre'].fillna('').astype(str)
        df['mail'] = df['mail'].fillna('').astype(str)
        df['telefono'] = df['telefono'].fillna('').astype(str)
        df['celular'] = df['celular'].fillna('').astype(str)
        df['sexo'] = df['sexo'].fillna('Default').astype(str)

        df['fecha_alta'] = df['fecha_alta'].where(pd.notnull(df['fecha_alta']), None)  
        df['descuento'] = df['descuento'].apply(lambda x: int(x) if pd.notna(x) else None)
        #df['id_centro_alta'] = df['id_centro_alta'].apply(lambda x: int(x) if pd.notna(x) else None)
        df['id_centro_alta'] = df['id_centro_alta'].where(pd.notnull(df['id_centro_alta']),None)

        # Verifica si hay valores en sexo que no están en el enum
        sex_enum_values = ['Masculino', 'Femenino', 'Default']  # Ajusta según tu enumeración
        df['sexo'] = df['sexo'].apply(lambda x: x if x in sex_enum_values else 'Default')
        
        
        df = df.drop_duplicates(subset='id_empresario')
        # Comprobación específica para valores NAType
        # for column in df.columns:
        #     if df[column].isna().any():
        #         print(f"\nLa columna '{column}' contiene valores nulos")
        records = df.to_dict(orient='records')
        # execute_valuesespera una lista de tuplas, por lo que se convierte 
        # el diccionario
        records = [tuple(record.get(column, None) for column in df.columns) for record in records]
        query = """
        INSERT INTO empresarios (id_empresario, nombre, mail, telefono, celular, id_pais, descuento, fecha_alta, sexo, id_centro_alta)
        VALUES %s
        ON CONFLICT (id_empresario) DO UPDATE SET
            nombre = EXCLUDED.nombre,
            mail = EXCLUDED.mail,
            telefono = EXCLUDED.telefono,
            celular = EXCLUDED.celular,
            id_pais = EXCLUDED.id_pais,
            descuento = EXCLUDED.descuento,
            fecha_alta = EXCLUDED.fecha_alta,
            sexo = EXCLUDED.sexo,
            id_centro_alta = EXCLUDED.id_centro_alta;
        """
        with self.conn.cursor() as cur:
            execute_values(cur, query, records)
            self.conn.commit()


    def cargar_datos_detalle(self, df):

        # solo deja las columnas nombre_detalle, valor_detalle y id_campania_eo
        df = df[['nombre_detalle', 'valor_detalle', 'id_campania_eo']]

        # TODO: revisar esto
        # df = df.drop_duplicates(subset=['nombre_detalle', 'valor_detalle', 'id_campania_eo'])
        records = df.to_dict(orient='records')
        # execute_valuesespera una lista de tuplas, por lo que se convierte 
        # el diccionario
        records = [tuple(record.values()) for record in records]
        query = """
        INSERT INTO detalle_contactos (nombre_detalle, valor_detalle, id_campania_eo)
        VALUES %s
        """
        with self.conn.cursor() as cur:
            execute_values(cur, query, records)
            self.conn.commit()


    def obtener_id_pais(self, df_pais):
        """
        Sustituye el nombre del pais del df_pais por su respectivo id 
        de la tabla paises.
        """

        query = """
        SELECT id_pais, nombre FROM paises;
        """
        df_paises = pd.read_sql(query, self.conn)
        df_pais = df_pais.merge(df_paises, left_on='pais', 
                                right_on='nombre',
                                how='left')
        # solo se queda con las columnas id_empresario y id_pais
        df_pais = df_pais[['id_empresario', 'id_pais']]

        return df_pais
    

    def obtener_id_centro_alta(self, df_centro_alta, df_id_pais):
        
       # une df_centro_alta con df_id_pais por id_empresario
        df_centro_alta = df_centro_alta.merge(df_id_pais, on='id_empresario', how='left')

        query = """SELECT * FROM centros;"""
        df_centros = pd.read_sql(query, self.conn)

        # une df_centro_alta con df_centros por id_pais
        df_centro_alta = df_centro_alta.merge(df_centros, left_on=['id_pais', 'centro_alta'], 
                                              right_on=['id_pais', 'nombre_centro'],
                                              how='left')
        
        # solo se queda con las columnas id_empresario y id_centro
        df_centro_alta = df_centro_alta[['id_empresario', 'id_centro']]
                
        return df_centro_alta
        

    

    def procesa_datos_personales(self, df_datos_personales):
        """
        Substituye los nombres de los paises y centros de alta por sus
        respectivos id's en las tablas paises y centros_alta.
        """

        # logger.debug(f"df_datos_personales: {df_datos_personales}")


        # dataframe pais 
        df_pais = df_datos_personales[['id_empresario', 'pais']]
        # dataframe centro_alta
        df_centro_alta = df_datos_personales[['id_empresario', 'centro_alta']]

        df_id_pais = self.obtener_id_pais(df_pais)
        df_id_centro_alta = self.obtener_id_centro_alta(df_centro_alta, df_id_pais)

        # agrega id_pais y id_centro_alta a df_datos_personales por id_empresario
        df_datos_personales = df_datos_personales.merge(df_id_pais, on='id_empresario', how='left')
        df_datos_personales = df_datos_personales.merge(df_id_centro_alta, on='id_empresario', how='left') 

        # elimina las columnas pais y centro_alta 
        df_datos_personales.drop(columns=['pais', 'centro_alta'], inplace=True)
        # renombra la columna id_centro por id_centro_alta
        df_datos_personales.rename(columns={'id_centro': 'id_centro_alta'}, inplace=True)

        # Asegurarse de que las columnas de tipo entero no contengan cadenas vacías
        df_datos_personales['id_pais'] = df_datos_personales['id_pais'].replace('', None).astype('Int64')
        df_datos_personales['descuento'] = df_datos_personales['descuento'].replace('', None).astype('Int64')
        df_datos_personales['id_centro_alta'] = df_datos_personales['id_centro_alta'].replace('', None).astype('Int64')

        # Asegurarse de que la columna sexo no contenga cadenas vacías y convertir 'NaN' a None
        df_datos_personales['sexo'] = df_datos_personales['sexo'].replace('', None).replace({pd.NA: None, 'NaN': None, float('nan'): None})

        # Asegurarse de que la columna fecha_alta no contenga cadenas vacías y convertirla a tipo fecha
        df_datos_personales['fecha_alta'] = pd.to_datetime(df_datos_personales['fecha_alta'], errors='coerce')

        # acomoda las columnas en el orden: id_empresario, nombre, mail, 
        # telefono, celular, id_pais, descuento, fecha_alta, sexo, id_centro_alta
        df_datos_personales = df_datos_personales[['id_empresario', 'nombre', 
                                                   'mail', 'telefono', 'celular',
                                                    'id_pais', 'descuento', 
                                                    'fecha_alta', 'sexo', 
                                                    'id_centro_alta']]
        
        return df_datos_personales
    

    def carga_campania_eos(self, df, id_campania):
        """
        Carga los datos de la campaña en la tabla campanias_eos.
        """

        # obtien solo la columna id_empresario del df
        df_id_empresario = df[['id_empresario']]
        # agrega la columna id_campania al df
        df_id_empresario['id_campania'] = id_campania

        # convierte el df a una lista de tuplas
        records = df_id_empresario.to_dict(orient='records')
        records = [tuple(record.values()) for record in records]

        # realiza la carga
        query = """
        INSERT INTO campania_eos (id_empresario, id_campania)
        VALUES (%s, %s)
        RETURNING id_campania_eo, id_empresario;
        """
        generated_ids = []
        with self.conn.cursor() as cur:
            for record in records:
                cur.execute(query, record)
                # guarda los registros generados devueltos
                generated_id = cur.fetchone()
                generated_ids.append(generated_id)
                self.conn.commit()

        # Crear un DataFrame con los id_campania_eo generados
        df_generated_ids = pd.DataFrame(generated_ids, columns=['id_campania_eo', 'id_empresario'])

        return df_generated_ids

    
    def carga_campania_paises(self, df_datos_personales, id_campania):
        """
        Carga los datos a la tabla campania_paises de la base de datos.
        Basándose en el pais.
        """
        df_pais = df_datos_personales[['id_empresario', 'pais']]
        query = """
        SELECT id_pais, nombre FROM paises;
        """
        df_paises = pd.read_sql(query, self.conn)
        df_pais = df_pais.merge(df_paises, left_on='pais', 
                                right_on='nombre',
                                how='left')
        # solo se queda con una columna: id_pais
        df_pais = df_pais[['id_pais']]
        
        # elimina los duplicados
        df_pais = df_pais.drop_duplicates(subset='id_pais')

        # agrega la columna id_campania al df
        df_pais['id_campania'] = id_campania

        # elimina las filas con valores nulos y/o nan
        df_pais = df_pais.dropna(subset=['id_pais', 'id_campania'])

        # convierte el df a una lista de tuplas
        records = df_pais.to_dict(orient='records')
        records = [tuple(record.values()) for record in records]

        # realiza la carga de uno en uno con un for
        query = """
            INSERT INTO campanias_paises (id_pais, id_campania)
            VALUES (%s, %s)
            ON CONFLICT (id_pais, id_campania) DO NOTHING
        """
        with self.conn.cursor() as cur:
            for record in records:
                cur.execute(query, record)
                self.conn.commit()

# endregion 

        


UPLOAD_DIR = "archivos/carga_masiva_ob"
name_file = "carga_Inscritos_Julio_2024.csv"
id_campania = 68

file_path = os.path.join(UPLOAD_DIR, name_file)


# Leer el archivo CSV con la codificación correcta
df_chunk = pd.read_csv(file_path, encoding='utf-8-sig')

# Instanciar y procesar
try:
    ProcesaArchivoCampania(df_chunk, id_campania)
except Exception as e:
    print(f"Error en la carga masiva: {e}")
finally:
    # Cerrar la conexión
    db = Database()
    db.desconectar()
    print("Proceso finalizado")