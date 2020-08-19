from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret

from datetime import datetime, timedelta

DAG_STARTDATE = datetime(2020, 4, 1)
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
dag = DAG('covid-19-epi-widgets',
          start_date=DAG_STARTDATE,
          catchup=False,
          default_args=default_args,
          schedule_interval=dag_interval,
          concurrency=8)

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
    name = "covid-19-epi-widgets-{}-{}-{}".format(task_name,
                                                  task_cmdline_args[0].replace("_", "-").replace(" ", "-"),
                                                  task_cmdline_args[2].replace("_", "-").replace(" ", "-"))
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


DISTRICT_TUPLES = (
    # ((district 1 file prefix, district 1 name),
    #   ((subdistrict 1 file prefix, sub district 1 name),
    #    ...,
    #    (subdistrict n file prefix, sub district n name)),
    ("city", "city of cape town", (
        ('eastern', 'eastern'),
        ('klipfontein', 'klipfontein'),
        ('southern', 'southern'),
        ('mitchells_plain', 'mitchells plain'),
        ('khayelitsha', 'khayelitsha'),
        ('northern', 'northern'),
        ('western', 'western'),
        ('tygerberg', 'tygerberg'),
        ('all', '*')
    )),
    ("prov", "*", (('all', '*'),))
)

# Defining tasks
CITY_MAP_LAYERS_GENERATE = 'city-map-layers-generate'
city_map_layers_operator = covid_19_widget_task(
    CITY_MAP_LAYERS_GENERATE,
    task_cmdline_args=["city", "city of cape town", "all", "*"]
)

MOBILE_DATA_MAP_LAYERS_GENERATE = 'mobile-data-map-layers-generate'
mobile_data_map_layers_operator = covid_19_widget_task(
    MOBILE_DATA_MAP_LAYERS_GENERATE,
    task_cmdline_args=["city", "city of cape town", "all", "*"]
)

EPI_MAP_CASE_LAYERS_GENERATE = 'epi-map-case-layers-generate'
city_map_layers_operators = [
    (covid_19_widget_task(
        EPI_MAP_CASE_LAYERS_GENERATE,
        task_cmdline_args=[district_filename_prefix, district_name, subdistrict_filename_prefix, subdistrict_name]
    ), city_map_layers_operator, mobile_data_map_layers_operator)
    for district_filename_prefix, district_name, subdistrict_tuples in DISTRICT_TUPLES
    for subdistrict_filename_prefix, subdistrict_name in subdistrict_tuples
]

CITY_MAP_PLOT = 'city-map-plot'
city_map_plot_operators = [
    covid_19_widget_task(
        CITY_MAP_PLOT,
        task_cmdline_args=[district_filename_prefix, district_name, subdistrict_filename_prefix, subdistrict_name]
    )
    for district_filename_prefix, district_name, subdistrict_tuples in DISTRICT_TUPLES
    for subdistrict_filename_prefix, subdistrict_name in subdistrict_tuples
]

CITY_HOTSPOT_MAP_PLOT = 'city-hotspot-map-plot'
city_hotspot_map_plot_operator = covid_19_widget_task(
    CITY_HOTSPOT_MAP_PLOT,
    task_cmdline_args=["city", "city of cape town", "all", "*"]
)

SUBDISTRICT_STATS_TABLE = 'subdistrict-stats-table'
subdistrict_table_operators = [
    covid_19_widget_task(
        SUBDISTRICT_STATS_TABLE,
        task_cmdline_args=[district_filename_prefix, district_name, "all", '*']
    )
    # only done on a per district basis, for the City, for now
    for district_filename_prefix, district_name, _ in DISTRICT_TUPLES
    if district_filename_prefix != "prov"
]

SPV_PLOT_TASK = 'spv-data-plot'
spv_plot_operator = covid_19_widget_task(SPV_PLOT_TASK,
                                         task_cmdline_args=["city", "city of cape town", "all", "*"])

# Dependencies
for layer_generate_operators, map_plot_operator in zip(city_map_layers_operators, city_map_plot_operators):
    map_plot_operator.set_upstream([operator for operator in layer_generate_operators
                                    if operator is not mobile_data_map_layers_operator])

city_hotspot_map_plot_operator.set_upstream([CITY_MAP_LAYERS_GENERATE, city_map_layers_operators[-2]])
