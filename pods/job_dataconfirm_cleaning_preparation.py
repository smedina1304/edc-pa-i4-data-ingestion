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
        #gcp_credentials = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        gcp_credentials = '/var/secrets/gcp/key.json'

        
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
        source = "dataconfirm"
        folder = f"raw-data-zone/{source}"

        # Carregando os dados do arquivo da data e linha informados
        df_dataconfirm = gcs.read_csv_to_df(folder=folder, dtexec=param_execution_date, sep=";")

        if df_dataconfirm is not None:

            # verificando se existe dados do dia posterior para completar o turno 3
            nextDay = datetime.strptime(param_execution_date, "%Y-%m-%d") + timedelta(days=1)
            print("dtexec:", param_execution_date, " next:", nextDay)    
            df = gcs.read_csv_to_df(folder=folder, dtexec=nextDay.strftime("%Y-%m-%d"), sep=";")
            if df is not None:
                df_dataconfirm = df_dataconfirm.append(df, sort=False, ignore_index=True)    

            # Limpa os dados nulos
            df_dataconfirm.dropna(inplace=True)

            # Alterando o delimitador decimal de "," para "."
            df_dataconfirm['KGPACK']    = df_dataconfirm.KGPACK.str.replace(',','.')
            df_dataconfirm['KGUNMED']   = df_dataconfirm.KGUNMED.str.replace(',','.')

            # Define os tipos de cada coluna
            df_dataconfirm['DTAPONT']   = pd.to_datetime(df_dataconfirm['DTAPONT'])
            df_dataconfirm['LOTE']      = df_dataconfirm['LOTE'].astype(str)
            df_dataconfirm['PACKID']    = df_dataconfirm['PACKID'].astype('int16')
            df_dataconfirm['UNIDADES']  = df_dataconfirm['UNIDADES'].astype('int16')
            df_dataconfirm['KGPACK']    = df_dataconfirm['KGPACK'].astype(float)
            df_dataconfirm['KGUNMED']   = df_dataconfirm['KGUNMED'].astype(float)

            # Definindo a Data de Produção DTPROD
            df_dataconfirm.insert(0, 'DTPROD', None)
            df_dataconfirm['DTPROD'] = df_dataconfirm.apply(lambda row:func.calc_dtprod(row['DTAPONT']),axis=1)
            df_dataconfirm['DTPROD'] = df_dataconfirm['DTPROD'].astype(str)
            
            # Definindo a ID do Turno de Produção IDTURNO
            df_dataconfirm.insert(1, 'IDTURNO', 0)
            df_dataconfirm['IDTURNO'] = df_dataconfirm.apply(lambda row:func.calc_idturno(row['DTAPONT']),axis=1)
            df_dataconfirm['IDTURNO'] = df_dataconfirm['IDTURNO'].astype(int)

            # Columns Name
            df_dataconfirm.rename(columns = {'LOTE':'BATCH', 'UNIDADES':'UN'}, inplace = True)            

            # Seleciona apenas o periodo referente ao DTPROD
            df_dataconfirm = df_dataconfirm.loc[ df_dataconfirm['DTPROD'] == param_execution_date ]

            # reindex
            df_dataconfirm = df_dataconfirm.reset_index(drop=True)

            # Verifica se existe arquivos no path para deletar
            gcs.delete_blob(pathName=f"processing-zone/{source}/DTPROD={param_execution_date}")

            # Gravando na Processing-Zone
            gcs.write_pandas_to_parquet(path=f"edc-pa-i4-data/processing-zone/{source}", df=df_dataconfirm, partitionCols=['DTPROD'])    



    # Se houve algum erro até este ponto é lançada uma exceção
    if msg_error is not None:
        raise Exception( msg_error )

    print('\n')