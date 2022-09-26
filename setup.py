from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="dagster_etl",
        packages=find_packages(exclude=["dagster_etl_tests"]),
        install_requires=[
            "dagster",
        ],
        extras_require={"dev": ["dagit", "pytest"]},
    )
