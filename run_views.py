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
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows

dfs = pd.DataFrame()
nombre = 'vistas.xlsx'
hostname = os.getenv("pc_mv_ip")
username = os.getenv("pc_mv_username")
password = os.getenv("pc_mv_pass")

con = psycopg2.connect(host=hostname, user=username, password=password, dbname='bdestadisticas')

list_views = ''' select viewname as name from pg_catalog.pg_views
where schemaname NOT IN ('pg_catalog', 'information_schema')'''

views = pd.read_sql(list_views,con)
views = [view for view in views.name]


for row,view in enumerate(views):
    if row == 0: #for the first cycle it creates an excel
        query = f'''SELECT * from public.{view}'''
        print(f'Iniciando lectura de la vista {view}', end='\n')
        df = pd.read_sql(query,con)
        df.to_excel(nombre, sheet_name = f'{view}', index=False, header= True)
    else: #for the rest of the cycles it creates new sheets
        query = f'''SELECT * from public.{view}'''
        print(f'Iniciando lectura de la vista {view}', end='\n')
        df = pd.read_sql(query,con)
        view = view[0:31] # sheet can only have 31 character lenght
        with pd.ExcelWriter(nombre, engine='openpyxl', mode='a') as writer: 
             df.to_excel(writer,sheet_name=f'{view}', index = False) 
print(f"Finalizado: {datetime.datetime.now()}\n")
con.close()