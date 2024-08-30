import pandas as pd 
import bcrypt

# lee el archivo csv usuarios_pass.csv
df = pd.read_csv('usuarios_pass.csv')

# toma la última columna del df y a cada uno de los registros les 
# aplica la función bcrypt.hashpw y los guarda en una columna 
# nueva llamada 'hash'
df['hash'] = df[df.columns[-1]].\
    apply(lambda x: bcrypt.hashpw(x.encode('utf-8'), bcrypt.gensalt()))

# exporta el df a un archivo csv llamado usuarios_pass_hash.csv
df.to_csv('usuarios_pass_hash_MEX.csv', index=False)


