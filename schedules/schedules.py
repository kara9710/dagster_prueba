from dagster import schedule
from dagster_prueba.dagster_prueba import run_etl_job


@schedule(cron_schedule="*/5 * * * *", job=run_etl_job)
def etl_job_schedule(_context):
    run_config = {}
    return run_config

