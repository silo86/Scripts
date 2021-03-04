#!/usr/bin/env python
# coding: utf-8

# In[1]:


from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import datetime


#datos
hostname_vero = '172.23.12.23'
hostname = '172.20.0.80'
hostname_penal = '172.20.0.100'
username = 'postgres'
password = 'estadistica'
tabla = '"ADJU"'
update = "UPDATE  public.""{0}"" SET vers = '{1}'"
select = "SELECT vers from public.""{0}"" "

con = psycopg2.connect(host=hostname_vero, user=username, password=password)

lista_db = 'SELECT datname FROM pg_database WHERE datistemplate = false'
bases = pd.read_sql(lista_db,con)
################### calcula la version anterior de base de datos
def bdanterior(bd):     
    caracter = bd.find("_")
    t = bd[: caracter + 1 ] 
    listado = []
    for i, fila in bases.iterrows():
            if str(t) in fila.datname:
                listado.append(fila.datname)
    listado.sort()

    j=0
    for i in listado:
        j = j + 1
        if str(bd) in i:
             bd = i
             break
    bd2 = listado[j-2]

    return(bd2)
###################
print('Ingresa la base de datos a actualizar')
bd = input()

for i, fila in bases.iterrows():
        if str(bd) in fila.datname:
            print("Iniciando base de datos {0}\n".format(fila.datname))
            con_b = psycopg2.connect(host=hostname_vero, user=username, password=password, dbname = fila.datname)
            cursor = con_b.cursor()
            cursor.execute(select.format(tabla))
            a = cursor.fetchall()
            version = a[0][0]
            print('La version actual es '+ version)
            con_b.commit()
            con_b.close()

print('Ingresa la base de datos de la cual tomar la version o presiona Enter para cambiar por una version anterior')
bd2 = input()
if bd2 == '':
    bd2 = bdanterior(bd)
else:
    for i, fila in bases.iterrows():
        if str(bd) in fila.datname:
            print(fila.datname + ' esta en la lista de bases de datos')

for i, fila in bases.iterrows():
        if str(bd2) in fila.datname:
            print("Iniciando base de datos {0}\n".format(fila.datname))
            con_c = psycopg2.connect(host=hostname_vero, user=username, password=password, dbname = fila.datname)
            cursor = con_c.cursor()
            cursor.execute(select.format(tabla))
            result1 = cursor.fetchall()
            print('La version de '+ fila.datname + ' es ' + result1[0][0])
            con_c.commit()
            con_c.close()

vers = result1[0][0]


for i, fila in bases.iterrows():
        if str(bd) in fila.datname:
            print("Actualizando base de datos {0}\n".format(fila.datname))
            con_b = psycopg2.connect(host=hostname_vero, user=username, password=password, dbname = fila.datname)
            cursor = con_b.cursor()
            cursor.execute(update.format(tabla,vers))
            con_b.commit()
            con_b.close()


# In[ ]:




