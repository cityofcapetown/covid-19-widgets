from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret

from datetime import datetime, timedelta

DAG_STARTDATE = datetime(2020, 5, 14)
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
DAG_NAME = 'covid-19-sr-widgets'
dag = DAG(DAG_NAME,
          start_date=DAG_STARTDATE,
          catchup=False,
          default_args=default_args,
          schedule_interval=dag_interval,
          concurrency=5)

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
    "startup_timeout_seconds": 60 * 30,
}


def covid_19_widget_task(task_name, task_kwargs={}, task_cmdline_args=[]):
    """Factory for k8sPodOperator"""
    name = "{}-{}".format(DAG_NAME, task_name)
    if len(task_cmdline_args) > 0:
        name = "{}-{}".format(name, task_cmdline_args[0].replace("_", "-"))

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


DIRECTORATE_LIST = [
    ("city", "*"),
    ("water_and_waste", 'Water and Waste Services'),
    ("energy_and_climate_change", 'Energy and Climate Change'),
    ("finance", 'Finance'),
    ("transport", 'Transport'),
    ("corporate_services", 'Corporate Services'),
    ("human_settlements", 'Human Settlements'),
    ("community_services_and_health", 'Community Services and Health'),
    ("economic_opportunities_and_asset_management", 'Economic Opportunities and Asset Management'),
    ("spatial_planning_and_environment", 'Spatial Planning and Environment')
]

# Defining tasks
TS_PLOT_TASK = 'sr-timeseries-plot'
timeseries_operators = [
    covid_19_widget_task(
        TS_PLOT_TASK,
        task_cmdline_args=[directorate_filename_prefix, directorate_title, ]
    ) for directorate_filename_prefix, directorate_title in DIRECTORATE_LIST
]

MAP_LAYER_TASK = 'sr-map-layers-generate'
map_layer_operators = [
    covid_19_widget_task(
        MAP_LAYER_TASK,
        task_cmdline_args=[directorate_filename_prefix, directorate_title, ]
    ) for directorate_filename_prefix, directorate_title in DIRECTORATE_LIST
]

MAP_LAYER_PUSH_TASK = 'sd-map-layers-push'
map_layer_push_operator = covid_19_widget_task(
    MAP_LAYER_PUSH_TASK,
)

MAP_WIDGET_TASK = 'sr-maps-generate'
map_widget_operators = [
    covid_19_widget_task(
        MAP_WIDGET_TASK,
        task_cmdline_args=[directorate_filename_prefix, directorate_title, ]
    ) for directorate_filename_prefix, directorate_title in DIRECTORATE_LIST
]

FOCUS_MAP_WIDGET_TASK = 'sr-focus-maps-generate'
focus_map_widget_operators = [
    covid_19_widget_task(
        FOCUS_MAP_WIDGET_TASK,
        task_cmdline_args=[directorate_filename_prefix, directorate_title, ]
    ) for directorate_filename_prefix, directorate_title in DIRECTORATE_LIST
]

SD_MAP_WIDGET_TASK = "sd-maps-generate"
sd_map_widget_operator = covid_19_widget_task(SD_MAP_WIDGET_TASK, task_cmdline_args=list(DIRECTORATE_LIST[0]))

SD_VALUES_TASK = "sd-latest-values"
sd_latest_values_operator = covid_19_widget_task(SD_VALUES_TASK,)

SD_VOLUME_WIDGET_TASK = "sd-volume-plots"
sd_volume_widget_operator = covid_19_widget_task(SD_VOLUME_WIDGET_TASK,)

SD_METRIC_WIDGET_TASK = "sd-metric-plots"
sd_metric_widget_operator = covid_19_widget_task(SD_METRIC_WIDGET_TASK,)

SD_DURATION_WIDGET_TASK = "sd-duration-plots"
sd_duration_widget_operator = covid_19_widget_task(SD_DURATION_WIDGET_TASK,)

# Dependencies
for layer_gen, widget_gen in zip(map_layer_operators, map_widget_operators):
    layer_gen >> widget_gen

map_layer_operators[0] >> sd_map_widget_operator
map_layer_push_operator >> sd_map_widget_operator

sd_latest_values_operator >> [sd_volume_widget_operator, sd_metric_widget_operator, sd_duration_widget_operator]
