# Declaração dos Pacotes, Libs ou Classes utilizadas no processo.
import os
import pytz
from datetime import datetime
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


# Declaração de variaveis do contexto
param_execution_date = None         # Parametro de definição de data do arquivo
param_line_id = None                # Parametro de definição do ID da linha de processo
gcp_credentials = None              # Parametro de credenciais GCP via Secrets k8s
oauth_settings_file = None          # Parametro de credenciais OAUTH para Google Drive via Secrets k8s
dt_current = None                   # Data e Hora corrente


# Função para retornar a lista de objetos do Google Drive
def getObjectList(gauth, drive, folderId):
    query = f"'{folderId}' in parents and trashed=false"
    #print('>> getObjectList', '> query :',query)
    objs = drive.ListFile({'q': query}).GetList()
    return objs


# Função para retornar ID de objeto do Google Drive
def getObjectId(file_list, titleName):
    objId = None
    for item in file_list:
        #print('>> getObjectId', '> title :',item['title'].upper(), ' - ', titleName)
        if item['title'].upper() == titleName.upper():
            objId = item['id']
            break
    return objId


# Main
if __name__ == "__main__":

    # Variável para manipulação de mensagem de erro
    msg_error = None
    dt_current = datetime.now(pytz.timezone('Brazil/East'))

    ## Verificação dos parametros recebidos
    try:
        # Parametro de definição de data do arquivo
        param_execution_date = os.environ['PARAM_EXECUTION_DATE']

        # Parametro de definição do ID da linha de produção
        param_line_id = os.environ['PARAM_LINE_ID']

        # Parametros de credenciais de autenticação
        # gcp_credentials = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        # oauth_settings_file = os.environ['GOOGLE_OAUTH_SETTINGS_FILE']
        gcp_credentials = '/var/secrets/gcp/key.json'
        oauth_settings_file = '/app/secrets/settings.yaml'
        oauth_credentials_file = '/app/secrets/credentials.json'

    except KeyError as e:
        msg_error = f'Error ## {repr(e)} - Parameter not defined.'

    except Exception as e:
        msg_error = f'Error ## {repr(e)}'

    # Verificação de Entrada de Parametros
    if msg_error is not None:
        print("MSG -", msg_error)
    else:
        # Entrada de parametros
        print('### Entrada de Parametros.')
        print('OK ->','PARAM_EXECUTION_DATE:',param_execution_date)
        print('OK ->','PARAM_LINE_ID',param_line_id)
        print('OK ->','GOOGLE_APPLICATION_CREDENTIALS',gcp_credentials)
        print('OK ->','GOOGLE_OAUTH_SETTINGS_FILE',oauth_settings_file)
        print('\n')

        print('### Validação Acesso aos Secrets Files.')
        # Se existe o arquivo no caminho definido
        if os.path.exists(gcp_credentials):
            print('OK ->','Arquivo disponível:',gcp_credentials)
        else:
            msg_error = f'ERRO ## Arquivo NÃO localizado: {gcp_credentials}'
            print(msg_error)

        # Se existe o arquivo no caminho definido
        if os.path.exists(oauth_settings_file):
            print('OK ->','Arquivo disponível:',oauth_settings_file)
        else:
            msg_error = f'ERRO ## Arquivo NÃO localizado: {oauth_settings_file}'
            print(msg_error)

        # Se existe o arquivo no caminho definido
        if os.path.exists(oauth_credentials_file):
            print('OK ->','Arquivo disponível:',oauth_credentials_file)
        else:
            msg_error = f'ERRO ## Arquivo NÃO localizado: {oauth_credentials_file}'
            print(msg_error)            

    # Se houve algum erro até este ponto é lançada uma exceção
    if msg_error is not None:
        raise Exception( msg_error )

    print('\n')

    ## Validação da Data de Execução
   
    # Se nenhum erro reportado
    if msg_error is None:
        print('### Validação da Data de Execução.')

        if param_execution_date.upper() in ['TODAY', 'HOJE', 'NOW']:
            param_execution_date = dt_current.date().strftime("%Y-%m-%d")

        # validando formatação da data
        try:
            datetime.strptime(param_execution_date, '%Y-%m-%d')

            print('VALID ->','PARAM_EXECUTION_DATE:',param_execution_date)
        except Exception as e:
            msg_error = f'ERRO ## PARAM_EXECUTION_DATE ({param_execution_date}) - {repr(e)}'

    # Se houve algum erro até este ponto é lançada uma exceção
    if msg_error is not None:
        raise Exception( msg_error )

    print('\n')

    ## Validação da Data Source

    # Se nenhum erro reportado
    if msg_error is None:    
        print('### Validação ID da Linha de Produção.')

        try:
            id = int(param_line_id)
            if (id>100 and id<121):
                print('VALID ->','PARAM_LINE_ID:',param_line_id)
            else:
                msg_error = f'ERRO ## PARAM_LINE_ID ({param_line_id}) - out of range.'

        except Exception as e:
            msg_error = f'ERRO ## PARAM_LINE_ID ({param_line_id}) - Invalid.'

    # Se houve algum erro até este ponto é lançada uma exceção
    if msg_error is not None:
        raise Exception( msg_error )

    print('\n')

    ## Autenticação na fonte de dados

    # Se nenhum erro reportado
    if msg_error is None:    
        print('### Autenticação na fonte de dados - Google Drive.')

        try:
            # Configuração da Autenticação 
            gauth = GoogleAuth(settings_file=oauth_settings_file)
            
            # Cria uma instancia do "GoogleDrive" autenticada para acesso ao repositório
            drive = GoogleDrive(gauth) 

            # Varredura da pasta raiz do Google Drive
            dir_root = getObjectList(gauth,drive, 'root')

            # Localiza as pastas referentes aos data sources
            for dir in dir_root:
                print(dir['title'].upper())               
                if dir['title'].upper() == 'DATAOP':
                    file_name = f'{param_execution_date}.csv'
                    file_id = getObjectId(getObjectList(gauth,drive, dir['id']),titleName=file_name)
                elif dir['title'].upper() == 'DATAPROD':
                    file_name = f'{param_line_id}-{param_execution_date}.csv'
                    file_id = getObjectId(getObjectList(gauth,drive, dir['id']),titleName=file_name)                
                elif dir['title'].upper() == 'DATACONFIRM':
                    file_name = f'{param_execution_date}.csv'
                    file_id = getObjectId(getObjectList(gauth,drive, dir['id']),titleName=file_name)
                else:
                    file_name = None
                    file_id = None

                #if (file_name is not None and file_id is not None):
                print("Folder:", dir['title'].upper(), "- File:", file_name, "- ID:", file_id)


        except Exception as e:
            msg_error = f'ERRO ## GOOGLE DRIVE OAUTH (settings_file={oauth_settings_file}) - {repr(e)}'

    # Se houve algum erro até este ponto é lançada uma exceção
    if msg_error is not None:
        raise Exception( msg_error )

    print('\n')

