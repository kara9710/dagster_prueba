from dagster import repository
from .dagster_prueba import run_etl_job, my_sensor,etl_job_schedule,df_transform,relations

@repository
def dagster_prueba():
    jobs = [run_etl_job]
    schedules = [etl_job_schedule]
    sensors=[my_sensor]
    assets=[df_transform,relations]
    return jobs + schedules + sensors + assets
