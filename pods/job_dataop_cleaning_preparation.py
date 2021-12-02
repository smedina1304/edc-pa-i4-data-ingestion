# Declaração dos Pacotes, Libs ou Classes utilizadas no processo.
import os
import io
import pytz
import pandas as pd
import gcsfs
import pyarrow
import pyarrow.parquet as pq

from google.cloud import storage
from datetime import datetime, timedelta

# Funções de integração com o Cloud Storage
from utilGCS import utilGCS
gcs = utilGCS(bucketName='edc-pa-i4-data')

# Funções diversas de manipulação de dados
from utilFuncs import utilFuncs
func = utilFuncs()

# Parametros
param_execution_date = None         # Parametro de definição de data do arquivo

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
        #param_line_id = os.environ['PARAM_LINE_ID']

        # Parametros de credenciais de autenticação
        gcp_credentials = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        # gcp_credentials = '/var/secrets/gcp/key.json'

        
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
        print('OK ->','GOOGLE_APPLICATION_CREDENTIALS',gcp_credentials)
        print('\n')

        print('### Validação Acesso aos Secrets Files.')
        # Se existe o arquivo no caminho definido
        if os.path.exists(gcp_credentials):
            print('OK ->','Arquivo disponível:',gcp_credentials)
        else:
            msg_error = f'ERRO ## Arquivo NÃO localizado: {gcp_credentials}'
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

    ## Inicio do processo de preparação e limpesa

    # Se nenhum erro reportado
    if msg_error is None:
        source = "dataop"
        folder = f"raw-data-zone/{source}"

        df_dataop = gcs.read_csv_to_df(folder=folder, dtexec=param_execution_date, sep=";")

        if df_dataop is not None:
            # Limpa os dados nulos
            df_dataop.dropna(inplace=True)

            # Define os tipos de cada coluna
            df_dataop['OP'] = df_dataop['OP'].astype(str)
            df_dataop['CODMAT'] = df_dataop['CODMAT'].astype(str)
            df_dataop['LOTEFAB'] = df_dataop['LOTEFAB'].astype(str)
            df_dataop['DTINI'] = pd.to_datetime(df_dataop['DTINI'])
            df_dataop['DTFIM'] = pd.to_datetime(df_dataop['DTFIM'])
            df_dataop['QTDPLAN'] = df_dataop['QTDPLAN'].astype(int)

            # Definindo a Data de Produção DTPROD
            df_dataop.insert(0, 'DTPROD', None)
            df_dataop['DTPROD'] = df_dataop.apply(lambda row:func.compate_dtprod(row['DTINI'], row['DTFIM']),axis=1)
            df_dataop['DTPROD'] = df_dataop['DTPROD'].astype(str)

            # save parquet - gcp
            # Verifica se existe arquivos no path para deletar
            gcs.delete_blob(pathName=f"processing-zone/{source}/DTPROD={param_execution_date}")

            # Gravando na Processing-Zone
            gcs.write_pandas_to_parquet(path=f"edc-pa-i4-data/processing-zone/{source}", df=df_dataop, partitionCols=['DTPROD'])



    # Se houve algum erro até este ponto é lançada uma exceção
    if msg_error is not None:
        raise Exception( msg_error )

    print('\n')