# edc-pa-i4-data-ingestion

## Projeto Aplicado - Instituto de Gestão e Tecnologia da Informação.
<br>

## Ingestão de Dados de Processos Industriais.

Orientador(a): João Paulo Barbosa Nascimento

_____
<br>

### Preparação e Deploy do Airflow no Cluster k8s:
<br>

Para a orquestração dos pipelines de processamento de dados estaremos utilizando o Apache Airflow, e para fazer o deploy no cluster k8s iremos utilizar uma imagem `Helm` que pode ser localizada no link abaixo:
<br>
Helm Chart for Apache Airflow:
https://airflow.apache.org/docs/helm-chart
<br>

Seguem a etapas de instalação:


1. Criação do namespace `ariflow` em seu cluster k8s.
    <br>
    Para verificar os namespaces existentes utilize o comando abaixo e verifique na lista retornada:

    ```shell
    kubectl get namespaces
    ```
    
    <br>

    Para criar o namespace *`airflow`*:

    ```shell
    kubectl create namespace airflow
    ```

    <br>

    Apenas como verificação você pode utilizar o comando para listar todos os namespaces e verificar se o *`airflow`* foi criado devidamente, e/ou também pode utilizar o comando abaixo para verificar todos os recursos disponíveis em um namespace, no caso *`airflow`* quando acabar de ser criado não deve apresentar nenhum item.

    ```shell
    kubectl get all -n airflow
    ```
    <br>

2. Atualização a imagem chart do *`airflow`* no repositório *`helm repo`* local antes do deploy no k8s.
    <br>
    Para incluir a imagem chart do `airflow` localmente, execute este comando:

    ```shell
    helm repo add apache-airflow https://airflow.apache.org
    ```

    <br>
    Se já houver a imagem do chart do `airflow` baixada anteriormente, execute a atualização do repositório para garantir que está com a versão mais atualizada.
    
    ```shell
    helm repo update
    ```

    <br>

3. Preparação do arquivo `values` que contém as instuções para deploy no `k8s` via `helm`.
    <br>
    Para gerar o arquivo `values` para o deploy, executar o comando abaixo:
    
    ```shell
    helm show values apache-airflow/airflow > airflow/deploy-airflow-values.yaml
    ```

    Onde:
    - Sintaxe - `helm show values [CHART] [flags]`
    - `[CHART]` - identificação do chart que foi feito o download no passo acima.
    - `[flags]` - parametros adicionais não utilizados neste exemplo.

    <br>

    Output: (operador `>`)
    - `airflow` - pasta do projeto para armazenar as instruções ou configurações de deploy para k8s via helm.
    - `deploy-airflow-values.yaml` - nome do arquivo que irá armazenar as alterações das configurações originais.

    <br>

    Alterações realizadas no arquivo `deploy-airflow-values.yaml`:

    - Version:

        ```yaml

            # Airflow home directory
            # Used for mount paths
            airflowHome: /opt/airflow

            # Default airflow repository -- overrides all the specific images below
            defaultAirflowRepository: apache/airflow

            # Default airflow tag to deploy
            defaultAirflowTag: "2.1.2"

            # Airflow version (Used to make some decisions based on Airflow Version being deployed)
            airflowVersion: "2.1.2"
        ```

        :point_right: *Atenção: é muito importante que seja verificado a versão do airflow, pois algumas variáveis de ambiente são diretamente ligadas as versão do airflow. Para este projeto estaremos utilizando a `versão 2.1.2`.*
        <br>

    - Executor:

        ```yaml
            # Airflow executor
            # Options: LocalExecutor, CeleryExecutor, KubernetesExecutor, CeleryKubernetesExecutor
            # executor: "CeleryExecutor"
            executor: "KubernetesExecutor"
        ```

    - Environment variables:

        ```yaml
            # Environment variables for all airflow containers
            env:
            - name: AIRFLOW__CORE__REMOTE_LOGGING
                value: 'True'
            - name: AIRFLOW__CORE__REMOTE_BASE_LOG_FOLDER
                value: 'gs://dl-techinical-apps/airflow-logs/'
            - name: AIRFLOW__CORE__REMOTE_LOG_CONN_ID
                value: 'my_gcp'
        ```

        :point_right: *Atenção: Observar que os logs estão sendo direcionados para uma pasta `airflow-logs` em um Bucket no GCS chamado `dl-techinical-apps`, é importante que esta estrutura tenha sido criada previamente para que o processo de logs do airflow funcione corretamente.*
        <br>


    - Create initial user:

        ```yaml
            # Create initial user.
            defaultUser:
                enabled: true
                role: Admin
                username: smedina
                email: smedina1304@gmail.com
                firstName: Sergio
                lastName: Medina
                password: admin
        ```

    - Service type.

        ```yaml
            service:
                #type: ClusterIP
                type: loadBalancer
        ```

    - Redis disable.

        ```yaml
            # Configuration for the redis provisioned by the chart
            redis:
                enabled: false
        ```

    - DAGs\Gitsync enable

        ```yaml
            gitSync:
                enabled: true

                # git repo clone url
                # ssh examples ssh://git@github.com/apache/airflow.git
                # git@github.com:apache/airflow.git
                # https example: https://github.com/apache/airflow.git
                repo: https://github.com/smedina1304/edc-pa-i4-data-ingestion
                branch: main
                rev: HEAD
                depth: 1
                # the number of consecutive failures allowed before aborting
                maxFailures: 0
                # subpath within the repo where dags are located
                # should be "" if dags are at repo root
                subPath: "airflow/dags"
        ```

    <br>


4. Airflow DAGs, criando uma pasta para armazenar as dags e um exemplo para teste de funcionalidade do airflow.
    <br>
    Na pasta raiz do projeto foi criada uma pasta `airflow` e dentro outra com o Nome `dags`.
    <br>
    Foi adicionado um código de exemplo com o nome `example_taskflow_api_etl.py`, para testes e evidências de funcionamento do `airflow`.
    <br>
    <br>


5. Criando uma `Secret` com as credenciais para acesso aos recursos GCP ao Cloud Storage onde estão os Buckets do Data Lake, e ao Google Drive onde estão os arquivos de dados.

    GCP - Credenciais de acesso via Arquivo JSON:
    <br>

    Carregando o conteúdo o arquivo com as chaves de acesso na `Secret` no namespace `airflow`:
    
    ```shell
    kubectl create secret generic gcp-credentials-key --from-file=key.json=/path-file-service-account-gcp.json -n ariflow
    ```
    <br>

    Para verificar se a `Secret` foi criada corretamente, utilize o comando abaixo:
    <br>

    ```shell
    kubectl describe secret gcp-credentials-key -n airflow
    ```

    *Output:*<br>
    *Observe que o conteúdo não será exposto*.
    ```console
    Name:         gcp-credentials-key
    Namespace:    airflow
    Labels:       <none>
    Annotations:  <none>

    Type:  Opaque

    Data
    ====
    key.json:  2323 bytes        
    ```
    <br>

    Google Drive - Configuirações Credenciais de acesso via OAUTH2:
    <br>

    Carregando o conteúdo dos arquivos de configuração e credenciais na `Secret` no namespace `airflow`:
    
    ```shell
    kubectl create secret generic oauth-settings-key --from-file=key.json=secrets/settings.yaml -n ariflow
    ```
    <br>

    ```shell
    kubectl create secret generic oauth-credentials-key --from-file=key.json=secrets/credentials.json -n ariflow
    ```
    <br>

    Para verificar se a `Secret` foi criada corretamente, utilize o comando abaixo:
    <br>

    ```shell
    kubectl describe secret oauth-settings-key -n airflow
    ```

    *Output:*<br>
    *Observe que o conteúdo não será exposto*.
    ```console
    Name:         oauth-settings-key
    Namespace:    airflow
    Labels:       <none>
    Annotations:  <none>

    Type:  Opaque

    Data
    ====
    key.json:  2323 bytes        
    ```
    <br>

    ```shell
    kubectl describe secret oauth-credentials-key -n airflow
    ```

    *Output:*<br>
    *Observe que o conteúdo não será exposto*.
    ```console
    Name:         oauth-credentials-key
    Namespace:    airflow
    Labels:       <none>
    Annotations:  <none>

    Type:  Opaque

    Data
    ====
    key.json:  2323 bytes        
    ```
    <br>
    <br>


6. Delpoy do Airflow no Cluster k8s.

    Após as preparações anteriores finalizadas executar o seguinte comando:

    ```shell
    helm install airflow apache-airflow/airflow -f airflow/deploy-airflow-values.yaml -n airflow --debug
    ```
    <br>

    :point_right: *Atenção: Sendo necessário desistalar o airflow por qualquer motivos, utilize o comando abaixo ou busque uma referência do mesmo para atendimento da necessidade:*

    ```shell
    helm uninstall airflow -n airflow --debug
    ```

    Após uma soliciatação de desinstalação sempre verifique se os recursos foram liberados.
    <br>
    <br>

7. Recuperar a `Fernet Key value`.

    Caso não tenha sido definido a `Fernet Key value` no arquivo `yaml`, como no caso deste procedimento, a mesma é gerada automaticamente e sendo necessário a sua utilização, é possível buscar o valor desta chave utilizando o comando abaixo:

    ```shell
    kubectl get secret --namespace airflow airflow-fernet-key -o jsonpath="{.data.fernet-key}" | base64 --decode
    ``` 

    <br>
    <br>
