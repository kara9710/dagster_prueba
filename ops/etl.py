from dagster import op
from db_mysql import create_db,get_path,get_dataframe,to_sql_table


#import needed libraries
import pandas as pd
from sqlalchemy import create_engine

#create database
@op
def create_database():
    create_db()

@op
def create_dataframe():
    get_path()
    get_dataframe()

@op
def update_sql_table():
    to_sql_table(get_dataframe())