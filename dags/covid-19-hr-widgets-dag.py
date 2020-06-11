from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret

from datetime import datetime, timedelta

DAG_STARTDATE = datetime(2020, 3, 31)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': DAG_STARTDATE,
    'email': ['gordon.inggs@capetown.gov.za'],
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

dag_interval = timedelta(hours=1)
dag = DAG('covid-19-hr-widgets',
          start_date=DAG_STARTDATE,
          catchup=False,
          default_args=default_args,
          schedule_interval=dag_interval,
          concurrency=4)

# env variables for inside the k8s pod
k8s_run_env = {
    'SECRETS_PATH': '/secrets/secrets.json',
    'COVID_19_DEPLOY_FILE': 'covid-19-widgets.zip',
    'COVID_19_DEPLOY_URL': 'https://ds2.capetown.gov.za/covid-19-widgets-deploy',
    'COVID_19_WIDGETS_DIR': '/covid-19-widgets',
    'DB_UTILS_LOCATION': 'https://ds2.capetown.gov.za/db-utils',
    'DB_UTILS_PKG': 'db_utils-0.3.2-py2.py3-none-any.whl'
}

# airflow-workers' secrets
secret_file = Secret('volume', '/secrets', 'airflow-workers-secret')

# arguments for the k8s operator
k8s_run_args = {
    "image": "cityofcapetown/datascience:python",
    "namespace": 'airflow-workers',
    "is_delete_operator_pod": True,
    "get_logs": True,
    "in_cluster": True,
    "secrets": [secret_file],
    "env_vars": k8s_run_env,
    "image_pull_policy": "Always",
     "startup_timeout_seconds": 60*30,
}


def covid_19_widget_task(task_name, task_kwargs={}, task_cmdline_args=()):
    """Factory for k8sPodOperator"""
    if len(task_cmdline_args) == 0:
        name_template = "covid-19-hr-widgets-{}"
        name = name_template.format(task_name)
    else:
        name_template = "covid-19-hr-widgets-{}-{}"
        name = name_template.format(task_name, task_cmdline_args[0].replace("_", "-"))

    run_args = {**k8s_run_args.copy(), **task_kwargs}
    run_cmd = "bash -c '{} && \"$COVID_19_WIDGETS_DIR\"/bin/{}.sh \"{}\"'".format(
        startup_cmd, task_name, '" "'.join(task_cmdline_args)
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


# Defining tasks
DIRECTORATE_LIST = {
    ("city", "*"),
    ("city_manager", 'CITY MANAGER'),
    ("water_and_waste", 'WATER AND WASTE'),
    # ("energy_and_climate_change", 'ENERGY AND CLIMATE CHANGE'),
    ("finance", 'FINANCE'),
    # ("safety_and_security", "SAFETY AND SECURITY"),
    # ("community_services_and_health", 'COMMUNITY SERVICES and HEALTH'),
    ("transport", "TRANSPORT"),
    ("corporate_services", "CORPORATE SERVICES"),
    ("urban_management", "URBAN MANAGEMENT"),
    ("human_settlements", "HUMAN SETTLEMENTS"),
    ("economic_opportunities_and_asset_management", "ECONOMIC OPPORTUNITIES &ASSET MANAGEMENT"),
    ("spatial_planning_and_environment", "SPATIAL PLANNING AND ENVIRONMENT")
}

LATEST_VALUES = 'hr-latest-values'
latest_values_operators = [
    covid_19_widget_task(
        LATEST_VALUES,
        task_cmdline_args=directorate_args
    ) for directorate_args in DIRECTORATE_LIST
]

ABSENTEEISM_LINE_PLOT = 'hr-absenteeism-plot'
absenteeism_operators = [
    covid_19_widget_task(
        ABSENTEEISM_LINE_PLOT,
        task_cmdline_args=directorate_args
    ) for directorate_args in DIRECTORATE_LIST
]

BUSUNIT_STATUS_PLOT = 'hr-busunit-status-plot'
busunit_operators = [
    covid_19_widget_task(
        BUSUNIT_STATUS_PLOT,
        task_cmdline_args=directorate_args
    ) for directorate_args in DIRECTORATE_LIST
]
