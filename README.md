# edc-pa-i4-data-ingestion

## Projeto Aplicado - Instituto de Gestão e Tecnologia da Informação.
<br>

## Ingestão de Dados de Processos Industriais.

Orientador(a): João Paulo Barbosa Nascimento

_____
<br>

### Objetivo:
<br>

Este projeto tem por finalidade documentar todas as estapas técnicas realizadas para desenvolvimento e implementação do projeto, com base no documento de Relatório de Projeto Aplicado de conclusão do curso de Pós-Graduação em MBA de Engenharia de Dados.

<br>

### Estrutura de Pastas:

As pastas deste projeto estão organizadas da seguinte forma:

- `root`
    - `docs` - Documentação gerada no formato `.md`, `.docx` ou `.pdf` para detalhamento e suporte conceitual deste projeto.
        - `images` - Imagens referenciadas das documentações e procedimentos deste projeto.
        - `ipynb`- Arquivos do `Jupyter Notebook` utilizados para verificar ou analizar pontos necessários para o desenvolvimentos deste projeto.
    - `sh-infra` - Pasta que contém os *'shell scripts'* utilizados para criação e manuteção dos recursos de infraestrutura em cloud.

<br>

### Consulte os tópicos abaixo relacionados para detalhamento de cada etapa realizada:

1. Preparação do Ambiente de Desenvolvimento.
    >.\docs\doc-01-Preparação-Ambiente-Desenv.md
<br>

2. Requisitos de ferramentas CLI.
    >.\docs\doc-02-Requisitos-Ferramentas-CLI.md
<br>

3. Criação do Cluster Kubernetes (k8s) em Cloud.
    >.\docs\doc-03-Criação-Cluster-k8s.md
<br>

### Repositórios Github usados como referência de pesquisa:

- "edc-airflow-spark-on-k8s"
    - https://github.com/smedina1304/edc-airflow-spark-on-k8s
        <br>
        *Implementação de Airflow em ambiente Kubernetes*

- "PyDrive2"
    - https://github.com/iterative/PyDrive2
        <br>
        *PyDrive2 é uma biblioteca wrapper de google-api-python-client que simplifica muitas tarefas comuns da API V2 do Google Drive.*

    - https://docs.iterative.ai/PyDrive2/oauth/#sample-settings-yaml
        <br>
        *Autenticação automática e personalizada com `settings.yaml`, como autenticação silenciosa em uma máquina remota.*

<br>