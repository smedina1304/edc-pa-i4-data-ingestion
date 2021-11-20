import airflow
from airflow import DAG
from airflow.decorators import task
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.kubernetes.secret import Secret
from airflow.models import Variable

# Airflow DAGs - Ingestão de dados do Processo Ind.
# https://github.com/apache/airflow/issues/12760
# https://github.com/VBhojawala/airflow/blob/k8s-docs/docs/apache-airflow-providers-cncf-kubernetes/operators.rst#mounting-secrets-as-volume

# pip install install 'apache-airflow[kubernetes]'

# Trigger DAG: {{ dag_run.conf }}.
# {"PARAM_EXECUTION_DATE": "2021-11-08", "PARAM_LINE_ID": "101"}

# google_app_credentials = Variable.get(key='GOOGLE_APPLICATION_CREDENTIALS')
# google_oauth_settings = Variable.get(key='GOOGLE_OAUTH_SETTINGS_FILE')

# ## Variaveis de Ambiente para o Pod, com base nos Parametros
vars = {
    'PRG_NAME': './job_source_data_collection.py',
    'PARAM_EXECUTION_DATE': '{{ dag_run.conf["PARAM_EXECUTION_DATE"] }}',
    'PARAM_LINE_ID': '{{ dag_run.conf["PARAM_LINE_ID"] }}',
    # 'GOOGLE_APPLICATION_CREDENTIALS': f'{google_app_credentials}',
    # 'GOOGLE_OAUTH_SETTINGS_FILE': f'{google_oauth_settings}',
}

print('Vars:', vars)

# ## Secrets oauth-settings
settings_volume = k8s.V1Volume(
    name='oauth-settings-key',
    secret=k8s.V1SecretVolumeSource(secret_name="oauth-settings-key")
)

settings_volume_mount = k8s.V1VolumeMount(
    name='oauth-settings-key', mount_path='/var/secrets/settings', sub_path=None, read_only=True
)

# ## Secrets oauth-credentials
credentials_volume = k8s.V1Volume(
    name='oauth-credentials-key',
    secret=k8s.V1SecretVolumeSource(secret_name="oauth-credentials-key")
)

credentials_volume_mount = k8s.V1VolumeMount(
    name='oauth-credentials-key', mount_path='/var/secrets/credentials', sub_path=None, read_only=True
)

# ## Secrets gcp-credentials
gcp_volume = k8s.V1Volume(
    name='gcp-credentials-key',
    secret=k8s.V1SecretVolumeSource(default_mode=384, secret_name="gcp-credentials-key")
)

gcp_volume_mount = k8s.V1VolumeMount(
    name='gcp-credentials-key', mount_path='/var/secrets/gcp', sub_path=None, read_only=True
)


# DAG - Definition
with DAG(
    'task-pa-i4-data-process',
    default_args={
        'owner': 'Sergio C. Medina',
        'depends_on_past': False,
        'start_date': airflow.utils.dates.days_ago(1),
        'email': ['smedina1304@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'max_active_runs': 1,
    },
    description='Processamento de dados de Produção - 1.3h',
    schedule_interval="@once",
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
    tags=['i40', 'kubernetes', 'batch', 'oee'],
) as dag:

    data_collect = KubernetesPodOperator(
        namespace='airflow',
        image="docker.io/smedina1304/run-pods-python:1.0",
        env_vars=vars,
        cmds=["/run_in_docker.sh"],
        volume_mounts=[gcp_volume_mount, settings_volume_mount, credentials_volume_mount],
        volumes=[gcp_volume, settings_volume, credentials_volume],        
        name="data_collect",
        task_id="data_collect",
        image_pull_policy="Always",
        is_delete_operator_pod=True,
        in_cluster=True,
        get_logs=True,
    )

data_collect




