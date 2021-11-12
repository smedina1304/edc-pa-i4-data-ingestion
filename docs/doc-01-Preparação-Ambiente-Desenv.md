# edc-pa-i4-data-ingestion

## Projeto Aplicado - Instituto de Gestão e Tecnologia da Informação.
<br>

## Ingestão de Dados de Processos Industriais.

Orientador(a): João Paulo Barbosa Nascimento

_____
<br>

### Preparação do Ambiente de Desenvolvimento em Python:
<br>

Ambiente de desenvolvimento (IDE):
- Linguagem Python 3.8 (ou superior)
- VS Code (IDE)
- Plugins (requeridos): 
   - Python extension for VS Code.
   - Pylance
<br>

Ambiente Virtual Python para configuração de pacotes requetidos para o desenvolvimento.
<br>

- Criando o ambiente virtual chamado **`"venv"`**:

    ```shell
    python -m venv venv
    ```
    <br>

    :point_right:  *Atenção: No windows para funcionamento do **`"venv"`** pode ser necessário executar o seguinte comando via Powershell:*
    <br>

    ```shell
    Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
    ```
    <br>


- Ativando o ambiente virtual **`"venv"`**:

    No Windows via Powershell utilizar "`activate.bat`".

    ```shell
    .\venv\Scripts\Activate.ps1
    ```
    <br>

    No Windows via CMD utilizar "`activate.bat`".

    ```shell
    .\venv\Scripts\activate.bat
    ```
    <br>

    No `Linux` ou `MAC` utiliar "`activate`".

    ```shell
    source .venv/bin/activate
    ```
    <br>

    :point_right:  *Atenção: Para verificar que está funcionando e o ambiente foi ativado, deve aparecer o nome do ambiente destacado com prefixo do seu prompt de comandos, conforme abaixo:*
    <br>

    ```shell
    (venv)
    ```



<br>
