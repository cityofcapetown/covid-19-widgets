from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret

from datetime import datetime, timedelta
import uuid

##############################################################################
# Generic Airflow dag arguments
DAG_STARTDATE = datetime(2020, 1, 31, 00)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': DAG_STARTDATE,
    'email': ['riaz.arbi@capetown.gov.za'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# env variables for inside the k8s pod
k8s_run_env = {
    'CODE_REPO': 'covid-19-widgets',
    'DB_UTILS_DIR': '/tmp/db-utils',
    'SECRETS_FILE': '/secrets/secrets.json'
}

# airflow-workers' secrets
secret_file = Secret('volume', '/secrets', 'airflow-workers-secret')

# arguments for the k8s operator
k8s_run_args = {
    "image": "riazarbi/datasci-r-heavy:20200803",
    "namespace": 'airflow-workers',
    "is_delete_operator_pod": True,
    "get_logs": True,
    "in_cluster": True,
    "secrets": [secret_file],
    "env_vars": k8s_run_env,
    "image_pull_policy": "Always",
    "startup_timeout_seconds": 60*30,
    "task_concurrency": 1,
}

##############################################################################
# Generic prod repo arguments
startup_cmd = 'git clone https://ds1.capetown.gov.za/ds_gitlab/OPM/covid-19-widgets.git "$CODE_REPO" && ' \
              'bash "$CODE_REPO"/bin/env-setup.sh && ' \
              'git clone https://ds1.capetown.gov.za/ds_gitlab/OPM/db-utils.git "$DB_UTILS_DIR"'

##############################################################################
# dag
dag_interval = timedelta(minutes=30)
dag = DAG(dag_id='covid-19-widgets-R',
          catchup=False,
          default_args=default_args,
          schedule_interval=dag_interval,
          concurrency=1)

# Pull changed tables from rgdb to minio
widgets_to_minio_operator = KubernetesPodOperator(
    cmds=["bash", "-cx"],
    arguments=["""{} &&
    cd "$CODE_REPO" &&
    Rscript widgets_to_minio.R""".format(startup_cmd)],
    name="render-widgets-to-minio-{}".format(uuid.uuid4()),
    task_id='widgets_to_minio_operator',
    dag=dag,
    execution_timeout=timedelta(minutes=1200),
    **k8s_run_args
)

############################################################################
# Task ordering
widgets_to_minio_operator

