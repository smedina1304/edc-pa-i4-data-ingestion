# edc-pa-i4-data-ingestion

## Projeto Aplicado - Instituto de Gestão e Tecnologia da Informação.
<br>

## Ingestão de Dados de Processos Industriais.

Orientador(a): João Paulo Barbosa Nascimento

_____
<br>

### Preparação de um docker para executar os scripts do pipeline em um KubernetesPodOperator:
<br>

**`ATENÇÃO - É PRE-REQUISITO QUE ESTEJA INSTALADO OS RECURSOS DE DOCKER BUILD NA MÁQUINA ONDE ESTES PROCEDIMENTOS FOREM SER REALIZADOS. VERIFIQUE ANTES DE INICIAR!`**

<br>

Inicialmente é necessário criar um arquivo de configuração do seu container docker com base em uma imagem, normalmente verifica-se os pré-requisitos necessário para a aplicação com base no sistema operacional. Neste projeto foi selecionada a imagem `python:3.8-slim-buster`, que já disponibiliza a instalação do python 3.8 em um sistema baseado em linux, ficando assim a necessidade de configurar a instalação dos pacotes necessários e o programas para serem executados.
<br>

Iniciamos com a criação de um arquivo `Dockerfile` conforme abaixo:
<br>

```dockerfile
    # Definição da Imagem, para detalhes ver: https://aka.ms/vscode-docker-python
    FROM python:3.8-slim-buster

    # Desabilita a geração dos arquivos .pyc no contêiner pelo Python
    ENV PYTHONDONTWRITEBYTECODE=1

    # Desativa o armazenamento em buffer para facilitar o registro do contêiner
    ENV PYTHONUNBUFFERED=1

    # Variaveis de Ambiente para parametrização de chamada do programa Python
    ENV PRG_NAME $PRG_NAME

    # Instalação dos pacotes requeridos
    COPY requirements.txt .
    RUN pip3 install --no-cache -r requirements.txt

    # Copia e define as permissão para o script que irá executar a chamada do script Python
    COPY run_in_docker.sh .
    RUN chmod 755 run_in_docker.sh

    # Criação das pastas
    RUN mkdir /app

    # Define o diretorio de trabalho e a copia dos scripts
    WORKDIR /app
    COPY pods/ /app

    # Cria um usuário não root com um UID explícito e adiciona permissão para acessar a 
    # pasta do aplicativo
    # Mais detelhes consultar: https://aka.ms/vscode-docker-python-configure-containers
    RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
    USER appuser

    # Execute Script
    CMD ["/run_in_docker.sh"]
```

<br>
<br>

Após a criação do Dockerfile e toda a estrutura de programas que serão executados, é necessário realizar o `Build` da imagem para tornar utilizavel com um container. Para realizar esta atividade executar o comando `docker build` conforme sintax abaixo:
<br>

```shell
docker build -f Dockerfile -t smedina1304/runPodsPython:v1.0 .
```

<br>
Onde:

- `docker build` -> É a instrução o docker realizar a construção da imagem com base no arquivo de configuração.

- `-f Dockerfile` -> O parametro `-f` identifica qual é o arquido com as instruções de construção do container, normalmente utilizado quando o arquivo tem o nome diferente de `Dockerfile`, porem para este exempo o parametro foi deixado explicito para exemplificação do uso, mas para este caso não seria necessário.

- `-t smedina1304/runPodsPython:v1.0` -> O parametro `-t` indica qual é a TAG de identificação da imagem do container, onde as informações que a compõem são: `[ID REPOSITORIO]/[ID CONTAINER]:[VERSAO]`, sendo `/` e `:` separadores. O [ID REPOSITORIO] é a identificação de onde o container será armazenado, por exemplo no `Docker Hub` ou `AWS`, porem se for apenas uma imagem local não será necessário. O [ID CONTAINER] é o nome de identificação do container e deve sempre ser informado. A [VERSAO] também chamado de `TAG` é onde se identifica a versão, também utilizado para fins de armazenamento. Atenção é necessário que todas as letras estejam em minusculo para todos os elementos informados no parametro `-t`.

- `.` --> Esta é o parametro que indica onde é o diretório base do processo de construção do container e neste caso indica o diretório corrente.

<br>

Ao executar o comando o processo deve executar os downloads das partições necessárias para montar a imagem e iniciar a construção. Também todos os comandos definidos no `Dockerfile` são executados de demonstrados na tela para acompanhamento. Para verificar se a imagem foi montada execute o comando conforme abaixo:

<br>

```shell
docker images
```

O retorno de ser semelhante ao abaixo:

```shell
REPOSITORY                  TAG       IMAGE ID       CREATED          SIZE
smedina1304/runPodsPython   v1.0      1fcc69fb574b   16 minutes ago   270MB   
```

<br>

Executando o **`PUSH`** da imagem no `Docker Hub`:
<br>

Com os testes realizados na máquina de desenvolvimento, e estando ok, você pode realizar o *push* da imagem em um repositório para tornar disponível para replicar em outros locais e também deixar disponível para outras pessoas em caso de um resitório público.
<br>

Para este caso vamos utilizar o *[hub.docker.com](https://hub.docker.com/)*.

- Primeiro passo é executar o login pelo docker:

    ```shell
    docker login -u user_name
    password prompt: <token> or <password>
    ```

- Com o login realizado e o acesso confirmado execute o commando de push definindo corretamente a TAG para a imagem, onde a TAG deve ter o seguinte formato:

   - Sintax `[ID REPOSITORIO]/[ID CONTAINER]:[VERSAO]`:
      - Sendo `/` e `:` separadores. 
      
      - O [ID REPOSITORIO] é a identificação do repositório identificado pela conta do usuário onde o container será armazenado. 
      
      - O [ID CONTAINER] é o nome de identificação do container e deve sempre ser informado, será seu nome de identificação. 
      - A [VERSAO] onde se identifica a versão ou alguma caracteristica específica da imagem.
      
   `Atenção:` é necessário que todas as letras estejam em minusculo para todos os elementos informados.


Comando para executar o *push* (exemplo):

```shell
docker push smedina1304/runPodsPython:v1.0
```

Após o comando verifique  no `Docker Hub` se a imagem foi devidamente atualizada.
<br>
<br>
