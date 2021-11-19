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

# Copia e define as permissão para o script que irá executar a chamada da DAG via Python
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
