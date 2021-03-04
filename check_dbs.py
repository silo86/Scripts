#!/usr/bin/env python
# coding: utf-8

from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import datetime
import os
import glob
os.path.abspath(os.getcwd())
import sys
import errno
from pathlib import Path
import sandesh
###################################### Check the status of all databases in ddedbs ######################################


#datos ddedbs
hostname = os.getenv("ddedbs_ip")
username = os.getenv("ddedbs_name")
password = os.getenv("ddedbs_pass")

#trae lista de bases de datos
con = psycopg2.connect(host=hostname, user=username, password=password, dbname='saeciv')
lista_db = '''SELECT datname FROM pg_database WHERE datistemplate = false '''
bases = pd.read_sql(lista_db,con)

#filtra las bases y guarda en la lista dbs
dbs = [base for base in bases.datname if (base.startswith('sae') or base.startswith('oga')) and not base.startswith('saemed') and not base.startswith('saeoga') and base not in ('saejes','saepjt','saeori','saepencam')]

#crea dos dataframes vacios: uno para oga y otro para el resto
dfs = pd.DataFrame()
dfOGA = pd.DataFrame()

#consultas
query = ''' select current_database() as base,max(febo) as febo,max(fepf) as fepf, max(fefi) as fefi from public."HIST" '''
queryoga = '''SELECT current_database() as base, max(created_at) as created_at FROM public.audiencias '''

#corre las consultas y guarda los 2 dataframes: dfOGA y dfs
for  fila in dbs:
    print(f'Iniciando lectura de base de datos {fila}', end='\n')
    if fila.startswith('oga'):
        con_b = psycopg2.connect(host=hostname, user=username, password=password, dbname = fila)
        df = pd.read_sql(queryoga,con_b)
        dfOGA = dfOGA.append(df)
        con_b.close
    else:
        con_b = psycopg2.connect(host=hostname, user=username, password=password, dbname = fila)
        df = pd.read_sql(query,con_b)
        dfs = dfs.append(df)
        con_b.close()

#formatea las fechas
hoy = datetime.datetime.now() 
if hoy.weekday() == 0: #si es lunes toma los 3 dias anteriores como validos
    d = datetime.datetime.now() - datetime.timedelta(days=1)
    s = datetime.datetime.now() - datetime.timedelta(days=2)
    v = datetime.datetime.now() - datetime.timedelta(days=3)
    dias = [hoy,v,s,d]
elif hoy.weekday == 6: # si es domingo
    s = datetime.datetime.now() - datetime.timedelta(days=1)
    v = datetime.datetime.now() - datetime.timedelta(days=2)
    dias = [hoy,v,s]
else: #para cualquier otro dia de la semana toma la fecha de ayer
    ayer = datetime.datetime.now() - datetime.timedelta(days=1)
    dias = [hoy,ayer]

    
#formato de fecha para bases oga
diasOGA = [dia.strftime("%Y") + '-' + dia.strftime("%m") + '-' + dia.strftime("%d") for dia in dias]
#formato de fecha para el resto de las bases
dias = [dia.strftime("%Y") + dia.strftime("%m") + dia.strftime("%d") for dia in dias]

#print(dfOGA)
#print(dfs)
msg= ''

for i in range(dfOGA.shape[0]):
    if((str(dfOGA.created_at.iloc[1].date()) in diasOGA) == False):
        msg += f"La base {dfOGA.base.iloc[i]} no está actualizada, último movimiento:  {dfOGA.created_at.iloc[i]} "
for i in range(dfs.shape[0]):
    if((dfs.febo.iloc[i] in dias or  dfs.fepf.iloc[i] in dias or  dfs.fefi.iloc[i] in dias) == False):
        msg += f"La base {dfs.base.iloc[i]} no está actualizada, último movimiento: febo: {dfs.febo.iloc[i]} ,fepf: {dfs.fepf.iloc[i]}, fefi: {dfs.fefi.iloc[i]} "
if msg == '':
    msg = 'Todas las bases estan actualizadas'
sandesh.send(msg, webhook = "https://hooks.slack.com/services/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")
#print(msg)