from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret

from datetime import datetime, date, timedelta

DAG_STARTDATE = datetime(2021, 3, 11)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': DAG_STARTDATE,
    'email': ['colinscott.anthony@capetown.gov.za'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

startup_cmd = (
    "mkdir $COVID_19_WIDGETS_DIR && "
    "curl $COVID_19_DEPLOY_URL/$COVID_19_DEPLOY_FILE -o $COVID_19_WIDGETS_DIR/$COVID_19_DEPLOY_FILE && "
    "cd $COVID_19_WIDGETS_DIR && unzip $COVID_19_DEPLOY_FILE && "
    "pip3 install $DB_UTILS_LOCATION/$DB_UTILS_PKG"
)

# At 6am, every weekday
# NB because of Airflow's dag trigger semantics, the run at 6am will apply to the previous day
dag_interval = timedelta(hours=1)
dag_name = "covid-19-vaccine-emailer"
dag = DAG(dag_name,
          start_date=DAG_STARTDATE,
          catchup=False,
          default_args=default_args,
          schedule_interval=dag_interval,
          concurrency=2)

# env variables for inside the k8s pod
k8s_run_env = {
    'SECRETS_PATH': '/secrets/secrets.json',
    'COVID_19_DEPLOY_FILE': 'covid-19-widgets.zip',
    'COVID_19_DEPLOY_URL': 'https://ds2.capetown.gov.za/covid-19-widgets-deploy',
    'COVID_19_WIDGETS_DIR': '/covid-19-widgets',
    'DB_UTILS_LOCATION': 'https://ds2.capetown.gov.za/db-utils',
    'DB_UTILS_PKG': 'db_utils-0.4.0-py2.py3-none-any.whl',
}

# airflow-workers' secrets
secret_file = Secret('volume', '/secrets', 'vaccine-data-secret')

# arguments for the k8s operator
k8s_run_args = {
    "image": "cityofcapetown/datascience:python@sha256:491802742dabd1eb6c550d220b6d3f3e6ac4359b8ded3307416831583cbcdee9",
    "namespace": 'airflow-workers',
    "is_delete_operator_pod": True,
    "get_logs": True,
    "in_cluster": True,
    "secrets": [secret_file],
    "env_vars": k8s_run_env,
    "image_pull_policy": "IfNotPresent",
     "startup_timeout_seconds": 60*30,
}


def covid_19_widget_task(task_name, task_args, task_kwargs={}):
    """Factory for k8sPodOperator"""
    name = f"{dag_name}-{task_name}"
    run_args = {**k8s_run_args.copy(), **task_kwargs}
    run_cmd = f"bash -c '{startup_cmd} && \"$COVID_19_WIDGETS_DIR\"/bin/{task_name}.sh {task_args}"
    
    operator = KubernetesPodOperator(
        cmds=["bash", "-cx"],
        arguments=[run_cmd],
        name=name,
        task_id=name,
        dag=dag,
        execution_timeout=timedelta(hours=1),
        **run_args
    )

    return operator


# Defining tasks
VACCINE_PLOT_TASK = "vaccine-rollout-plots"
vaccine_plot_operator = covid_19_widget_task(VACCINE_PLOT_TASK, '')

VACCINE_EMAIL_TASK = 'vaccine-emailer'
report_date = str(date.today)
report_date_arg = f" --report_date {report_date}"
vaccine_email_operator = covid_19_widget_task(VACCINE_EMAIL_TASK, report_date_arg)

# dependencies
vaccine_plot_operator >> vaccine_email_operator
