from dagster import load_assets_from_package_module, repository

from dagster_etl import assets


@repository
def dagster_etl():
    return [load_assets_from_package_module(assets)]
