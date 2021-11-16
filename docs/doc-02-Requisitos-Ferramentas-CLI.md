# edc-pa-i4-data-ingestion

## Projeto Aplicado - Instituto de Gestão e Tecnologia da Informação.
<br>

## Ingestão de Dados de Processos Industriais.

Orientador(a): João Paulo Barbosa Nascimento

_____
<br>

### Requisitos de ferramentas CLI (Command line):
<br>

A utilização de ferramentas via CLI (*"command line"*) é importante pois podemos definir os scripts de criação, preparação de deploy em nosso ambiente, deixando o mesmo facilmente de ser replicavel em outra estrutura se necessário, e até mesmo em um caso de um acelerador para um "disaster recovery".
<br>

- Recursos indicados:
    <br>

    - Ambiente GCP, seguir as instruções da página conforme o sistema operaiconal desejado:
        - Google Cloud SDK Command Line (CLI) - https://cloud.google.com/sdk/docs/quickstart
        <br>
        :point_right: *Importante: realize todos os passos de preparação do ambiente Google Cloud SDK conforme a página Quickstart.*
        <br>     

   - EKS para interação com o cluster Kubernetes - eksctl - https://docs.aws.amazon.com/eks/latest/userguide/eksctl.html
      - Também deve ser verificado de juntamente com o eksctl foi instalando kubectl, caso contrário verificar o procedimento em: https://docs.aws.amazon.com/eks/latest/userguide/install-kubectl.html
      <br>

   - HELM - repositório de pacotes para deploy no k8s - https://helm.sh/docs/intro/install/
    <br>

   - kubectx - para alterar o contexto o cluster k8s, em caso de haver mais de uma referência na máquina utilizada para operação - https://github.com/ahmetb/kubectx
    <br>


- Verificar as instalações GCP CLI:

    ```shell
    gcloud --version
    ```

    *Output:*
    ```console
    Google Cloud SDK 358.0.0
    bq 2.0.71
    core 2021.09.17
    gsutil 4.68
    ```
    <br>

- Verificar as instalações EKSCTL:

    ```shell
    eksctl version
    ```

    *Output:*
    ```console
    0.63.0
    ```
    <br>

- Verificar as instalações EKSCTL:

    ```shell
    kubectl version
    ```

    *Output:*
    ```console    
    Client Version: version.Info{Major:"1", Minor:"22", GitVersion:"v1.22.1", GitCommit:"632ed300f2c34f6d6d15ca4cef3d3c7073412212", GitTreeState:"clean", BuildDate:"2021-08-19T15:38:26Z", GoVersion:"go1.16.6", Compiler:"gc", Platform:"darwin/amd64"}
    ```
    <br>

- Verificar as instalações HELM:

    ```shell
    helm version
    ```

    *Output:*
    ```console    
    version.BuildInfo{Version:"v3.6.3", GitCommit:"d506314abfb5d21419df8c7e7e68012379db2354", GitTreeState:"dirty", GoVersion:"go1.16.6"}
   ```

<br>

:point_right: *Importante: verifique todos os passos de configuração das ferramentas CLI referentes a interação com a AWS e GCP, pois cada uma tem seu padrão de autenticação.*
<br>
<br>

