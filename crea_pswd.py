import pandas as pd
import random
import string
import bcrypt


def generar_contrasena():
    longitud = 10  # longitud de la contraseña
    caracteres_especiales = '%@$&'
    caracteres_validos = string.ascii_uppercase + string.ascii_lowercase + string.digits
    contrasena = []

    # Asegura que al menos un carácter de cada tipo esté presente
    contrasena.append(random.choice(string.ascii_uppercase))
    contrasena.append(random.choice(string.ascii_lowercase))
    contrasena.append(random.choice(string.digits))
    contrasena.append(random.choice(caracteres_especiales))

    # Llena el resto asegurando que el primer carácter no sea especial
    while len(contrasena) < longitud - 1:
        contrasena.append(random.choice(
            string.ascii_uppercase +
            string.ascii_lowercase +
            string.digits
        ))

    # Añade un carácter válido al principio
    contrasena.insert(0, random.choice(caracteres_validos))

    # Mezclamos la contraseña (excepto el primer carácter) para que no siempre sigan el mismo patrón
    random.shuffle(contrasena[1:])

    return ''.join(contrasena)


def encriptar_contrasena(contrasena):
    # Encriptamos la contraseña usando bcrypt
    contrasena_bytes = contrasena.encode('utf-8')
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(contrasena_bytes, salt)
    return hashed.decode('utf-8')


def crear_dataframe(filas):
    contrasenas = [generar_contrasena() for _ in range(filas)]
    contrasenas_encriptadas = [encriptar_contrasena(contrasena) for contrasena in contrasenas]
    df_final = pd.DataFrame({
        'contrasena': contrasenas,
        'contrasena_encriptada': contrasenas_encriptadas
    })
    return df_final


n = 21  # Número de filas
df = crear_dataframe(n)
# Guardamos el dataframe en un archivo CSV
df.to_csv('contrasenas.csv', index=False)

print(df)
