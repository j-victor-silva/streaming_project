from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from pathlib import Path
from datetime import datetime
import pendulum


DOC_TRACKING = """
DAG that runs a Bash command to download tracking files from Drive for streaming to work
"""
DOC_PROFILES = """
DAG that runs a Bash command to download profile files from Drive so that streaming works
"""

with DAG(
    dag_id="download_tracking_files_from_drive",
    schedule="* * * * *",
    start_date=pendulum.datetime(2024, 9, 18, 22, 0, tz="America/Sao_Paulo"),
    catchup=False,
    doc_md=DOC_TRACKING,
) as dag1:
    TRACKING_PATH = Path().resolve() / "tracking" / "files"
    FILE_ID_TRACKING = "<FILE_ID_FROM_GOOGLE_DRIVE>"
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    download_tracking = BashOperator(
        task_id="download_tracking_files",
        bash_command=f'gdown {FILE_ID_TRACKING} -O {TRACKING_PATH}/transactions_{str(datetime.now()).replace(" ", "-").replace(":", "-")[0:19]}.csv',
    )

    (start >> download_tracking >> end)
    
with DAG(
    dag_id="download_profiles_files_from_drive",
    schedule="* * * * *",
    start_date=pendulum.datetime(2024, 9, 18, 22, 0, tz="America/Sao_Paulo"),
    catchup=False,
    doc_md=DOC_PROFILES,
) as dag2:
    PROFILES_PATH = Path().resolve() / "profiles" / "files"
    FILE_ID_PROFILES = "<FILE_ID_FROM_GOOGLE_DRIVE>"
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    download_profiles = BashOperator(
        task_id="download_profiles_files",
        bash_command=f'gdown {FILE_ID_PROFILES} -O {PROFILES_PATH}/profiles_{str(datetime.now()).replace(" ", "-").replace(":", "-")[0:19]}.csv',
    )

    (start >> download_profiles >> end)
