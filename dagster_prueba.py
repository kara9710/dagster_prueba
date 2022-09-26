from dagster import job,sensor,RunRequest,op,schedule,asset
import pandas as pd
from pandas import DataFrame
import mysql.connector
from sqlalchemy import create_engine
import os 

def create_db():
    pwd = os.environ['DAGSTER_MYSQL_PASSWORD']
    db=mysql.connector.connect(
        host='localhost',
        user='root',
        passwd=pwd)
    cs=db.cursor()
    try:
        cs.execute("CREATE DATABASE testing_dagster")
    except:
        print('Database already exists')

#Obtenemos el path
def get_path():
    return 'C:/Users/L03123909/Downloads/Matriz_de_adyacencia_1.xlsx'   


#Nos traemos el dataframe del archivo
def get_dataframe():
    path=get_path()
    try:
        df= pd.read_excel(f'{path}',sheet_name='Matriz de adyacencia')
    except:
        df= pd.read_excel(f'{path}')
    return df

#Tomamos el dataframe y lo manejamos
def manejo_de_datos(df):
    try:
        #Eliminar columnas 
        df.drop(['Unnamed: 0','Unnamed: 1'], axis=1,inplace=True)
        #Renombrar las columnas
        df.columns=df.iloc[0]
        #Eliminar el index 0
        df.drop(0, axis=0, inplace=True)
        #generar columna ids
        ids = [*range(1,len(df)+1, 1)]
        df['id_letra_actor']=ids
    except:
        print('No se encuentran las columnas')
    #Generaremos una tabla con relaciones entre los actores
    columna=[]
    relaciones_columnas=[]
    for i in df.columns:
        #Filtramos en donde si aparezca relación
        relacion=list(df['id_letra_actor'][df[f'{i}']==1])
        #Generamos 2 listas una del nombre de la columna y otre de todas las relaciones que tiene
        relaciones_columnas.append(relacion)
        columna.append(i)
    #Eliminar el ultimo valor de las listas
    del columna[-1]
    del relaciones_columnas[-1]
    #Generamos el dataframe
    data=list(zip(columna,relaciones_columnas))
    df_alumnos= pd.DataFrame(data, columns=['columna_letra','relacion_actores'])
    return df_alumnos

#Generamos la tabla final de actores con el nombre
def limpieza_tabla_actores(df_datos,path):
    #path=get_path()
    try:
        df2=pd.read_excel(path,sheet_name='Lista de actores')
        #Limpiamos nuestra tabla y le añadimos nombres de columnas
        df2.drop([0,1,2],axis=0,inplace=True)
        df2.columns=['columna_letra','id_actor_letra','nombre_actor']
        df_datos=pd.merge(df2,df_datos, how="left", on=["columna_letra"])
    except:
        print('No hay una tabla con ese nombre')

    return df_datos

#Subimos la tabla del excel a mysql
def to_sql_table(df):
    password = os.environ['DAGSTER_MYSQL_PASSWORD']
    host='localhost'
    user='root'
    dbname='testing_dagster'
    # Create SQLAlchemy engine to connect to MySQL Database
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
				.format(host=host, db=dbname, user=user, pw=password))
    
    # Convert dataframe to sql table                                   
    df.to_sql('test_karen', engine, index=False, if_exists= 'replace')

def to_sql_rel_table(df):
    password = os.environ['DAGSTER_MYSQL_PASSWORD']
    host='localhost'
    user='root'
    dbname='testing_dagster'
    # Create SQLAlchemy engine to connect to MySQL Database
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
				.format(host=host, db=dbname, user=user, pw=password))
    
    # Convert dataframe to sql table                                   
    df.to_sql('relations_table', engine, index=False, if_exists= 'replace')

#@op(config_schema={'path': str})
#def getting_path(context):
#    path_ = context.op_config['path']
#    print(path_)
#    return path_


##Declaración de OPS y JOB
#create database
@op
def create_database():
    create_db()

@op
def update_sql_table():
    to_sql_table(get_dataframe())
 
#Generar assets
@asset
def df_transform()-> DataFrame:
   return manejo_de_datos(get_dataframe())

@asset
def relations(df_transform:DataFrame):
   return limpieza_tabla_actores(df_transform,get_path())

@job
def run_etl_job():
    """
    Generación del job para que cree y actualice la base de datos
    """
    create_database()
    update_sql_table()


@sensor(job=run_etl_job)
def my_sensor():
    should_run = True
    if should_run:
        yield RunRequest(run_key=None, run_config={})

@schedule(cron_schedule="0 8 * * *", job=run_etl_job, execution_timezone="America/Mexico_City")
def etl_job_schedule():
    run_config= {}
    
    return run_config