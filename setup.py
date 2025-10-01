"""
Configuration installation package CBM_ETL
Permet import direct : from src.utils import ...
"""

from setuptools import setup, find_packages

setup(
    name="cbm_etl",
    version="1.0.0",
    description="Pipeline ETL Progress OpenEdge vers SQL Server",
    author="CBM Team",
    packages=find_packages(),
    python_requires=">=3.10",
    install_requires=[
        "prefect>=2.14.0",
        "pandas>=2.0.0",
        "pyodbc>=4.0.39",
        "sqlalchemy>=2.0.0",
        "python-dotenv>=1.0.0",
        "pyarrow>=14.0.0",
        "requests>=2.31.0",
    ],
    entry_points={
        'console_scripts': [
            'cbm-etl=src.flows.load_flow_simple:main',
            'cbm-orchestrator=src.flows.orchestrator:main',
            'cbm-validate=src.flows.validate_tables:main',
            'cbm-health=scripts.python.daily_health_check:main',
        ],
    },
)