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

# Funções diversas de manipulação de dados
from utilFuncs import utilFuncs
func = utilFuncs()

# Parametros
param_execution_date = None         # Parametro de definição de data do arquivo
param_line_id = None                # Parametro de definição do ID da linha de processo

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

        # Parametro de definição do Projeto e Bucket GCS
        param_project_id = os.environ['PARAM_PROJECT_ID']
        param_bucket_name = os.environ['PARAM_BUCKET_NAME']        

        # Parametros de credenciais de autenticação
        # gcp_credentials = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
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
        print('OK ->','PARAM_PROJECT_ID:',param_project_id)
        print('OK ->','PARAM_BUCKET_NAME:',param_bucket_name)
        print('OK ->','PARAM_EXECUTION_DATE:',param_execution_date)
        print('OK ->','PARAM_LINE_ID',param_line_id)
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

    ## Validação da Linha

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

    ## Funções de integração com o Cloud Storage
    gcs = utilGCS(projectid=param_project_id, bucketName=param_bucket_name)

    ## Inicio do processo de preparação e limpesa

    # Se nenhum erro reportado
    if msg_error is None:
        source = "dataprod"
        folder = f"raw-data-zone/{source}"

        # Carregando os dados do arquivo da data e linha informados
        df_dataprod = gcs.read_csv_to_df(folder=folder, dtexec=param_execution_date, lineprod=param_line_id, sep=";")

        if df_dataprod is not None:

            # verificando se existe dados do dia posterior para completar o turno 3
            nextDay = datetime.strptime(param_execution_date, "%Y-%m-%d") + timedelta(days=1)
            print('DEBUG ->',"param_execution_date:", param_execution_date, " next:", nextDay)    
            df = gcs.read_csv_to_df(folder=folder, dtexec=nextDay.strftime("%Y-%m-%d"), lineprod=param_line_id, sep=";")
            if df is not None:
                df_dataprod = df_dataprod.append(df, sort=False, ignore_index=True)

                # Limpa os dados nulos
                df_dataprod.dropna(inplace=True)

                # Alterando o delimitador decimal de "," para "."
                df_dataprod['TOTMIN']=df_dataprod.TOTMIN.str.replace(',','.')

                # Define os tipos de cada coluna
                df_dataprod['OP'] = df_dataprod['OP'].astype(str)
                df_dataprod['LINE'] = df_dataprod['LINE'].astype('int16')
                df_dataprod['LINE'] = df_dataprod['LINE'].astype(str)
                df_dataprod['TIMESTAMP'] = pd.to_datetime(df_dataprod['TIMESTAMP'])
                df_dataprod['BATCH'] = df_dataprod['BATCH'].astype(str)
                df_dataprod['TIMER'] = df_dataprod['TIMER'].astype(str)
                df_dataprod['TOTMIN'] = df_dataprod['TOTMIN'].astype(float)
                df_dataprod['STSID'] = df_dataprod['STSID'].astype('int16')
                df_dataprod['STSDS'] = df_dataprod['STSDS'].astype(str)
                df_dataprod['PC'] = df_dataprod['PC'].astype(float)
                df_dataprod['GOOD'] = df_dataprod['GOOD'].astype(float)
                df_dataprod['REJECT'] = df_dataprod['REJECT'].astype(float)

                # Definindo a Data de Produção DTPROD
                df_dataprod.insert(0, 'DTPROD', None)
                df_dataprod['DTPROD'] = df_dataprod.apply(lambda row:func.calc_dtprod(row['TIMESTAMP']),axis=1)
                df_dataprod['DTPROD'] = df_dataprod['DTPROD'].astype(str)
                
                # Definindo a ID do Turno de Produção IDTURNO
                df_dataprod.insert(1, 'IDTURNO', 0)
                df_dataprod['IDTURNO'] = df_dataprod.apply(lambda row:func.calc_idturno(row['TIMESTAMP']),axis=1)
                df_dataprod['IDTURNO'] = df_dataprod['IDTURNO'].astype(int)

                # Seleciona apenas o periodo referente ao DTPROD
                df_dataprod = df_dataprod.loc[ df_dataprod['DTPROD'] == param_execution_date ]

                # Columns Name
                df_dataprod.rename(columns = {'PC':'QTDPCS', 'GOOD':'QTDGOOD', 'REJECT':'QTDREJECT' }, inplace = True)            

                # reindex
                df_dataprod = df_dataprod.reset_index(drop=True)

                # Verifica se existe arquivos no path para deletar
                gcs.delete_blob(pathName=f"processing-zone/{source}/DTPROD={param_execution_date}")

                # Gravando na Processing-Zone
                gcs.write_pandas_to_parquet(path=f"edc-pa-i4-data/processing-zone/{source}", df=df_dataprod, partitionCols=['DTPROD','LINE'])

            # Se houve algum erro até este ponto é lançada uma exceção
            if msg_error is not None:
                raise Exception( msg_error )

            print('\n')