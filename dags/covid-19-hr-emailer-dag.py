from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret

from datetime import datetime, timedelta

DAG_STARTDATE = datetime(2020, 4, 23)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': DAG_STARTDATE,
    'email': ['gordon.inggs@capetown.gov.za'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=15),
}

startup_cmd = (
    "mkdir $COVID_19_WIDGETS_DIR && "
    "curl $COVID_19_DEPLOY_URL/$COVID_19_DEPLOY_FILE -o $COVID_19_WIDGETS_DIR/$COVID_19_DEPLOY_FILE && "
    "cd $COVID_19_WIDGETS_DIR && unzip $COVID_19_DEPLOY_FILE && "
    "pip3 install $DB_UTILS_LOCATION/$DB_UTILS_PKG"
)

# At 6am, every weekday
# NB because of Airflow's dag trigger semantics, the run at 6am will apply to the previous day
dag_interval = "0 6 * * 1-5"
dag = DAG('covid-19-hr-emailer',
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
    'DB_UTILS_PKG': 'db_utils-0.3.7-py2.py3-none-any.whl'
}

# airflow-workers' secrets
secret_file = Secret('volume', '/secrets', 'airflow-workers-secret')

# arguments for the k8s operator
k8s_run_args = {
    "image": "cityofcapetown/datascience:python@sha256:c5a8ec97e35e603aca281343111193a26a929d821b84c6678fb381f9e7bd08d7",
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
    directorate_sanitised = task_args[0].lower().replace(" ", "-").replace("&", "and")
    name = "covid-19-hr-emailer-{}-{}".format(task_name, directorate_sanitised)
    run_args = {**k8s_run_args.copy(), **task_kwargs}
    run_cmd = "bash -c '{} && \"$COVID_19_WIDGETS_DIR\"/bin/{}.sh {{{{ ds }}}} \"{}\"'".format(
        startup_cmd, task_name, '" "'.join(task_args)
    )

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


DIRECTORATE_SET = {
    # "WATER AND WASTE",
    # "COMMUNITY SERVICES and HEALTH",
    # "SAFETY AND SECURITY",
    # "ENERGY AND CLIMATE CHANGE",
    "FINANCE",
    "CORPORATE SERVICES",
    "TRANSPORT",
    "ECONOMIC OPPORTUNITIES &ASSET MANAGEMENT",
    "SPATIAL PLANNING AND ENVIRONMENT",
    "HUMAN SETTLEMENTS",
    "URBAN MANAGEMENT",
    "CITY MANAGER",
}

# Defining tasks
EMAIL_TASK = 'hr-bp-emailer'
email_operators = [
    covid_19_widget_task(EMAIL_TASK, task_args=[directorate])
    for directorate in DIRECTORATE_SET
]
