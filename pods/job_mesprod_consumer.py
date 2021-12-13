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

# bucketName
bucketName='edc-pa-i4-data'

# Funções de integração com o Cloud Storage
from utilGCS import utilGCS
gcs = utilGCS(bucketName=bucketName)

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

    ## Inicio do processo

    # Se nenhum erro reportado
    if msg_error is None:
        
        # Caregando os dados da processing - dataop
        source = "dataop"
        folder = f"{bucketName}/processing-zone/{source}"
        df_dataop = gcs.read_parquet_to_pandas(path=folder, filters=[('DTPROD', '=', param_execution_date)])

        # Caregando os dados da processing - dataconfirm
        source = "dataconfirm"
        folder = f"{bucketName}/processing-zone/{source}"
        df_dataconfirm = gcs.read_parquet_to_pandas(path=folder, filters=[('DTPROD', '=', param_execution_date)])

        # Caregando os dados da processing - dataprod
        source = "dataprod"
        folder = f"{bucketName}/processing-zone/{source}"
        df_dataprod = gcs.read_parquet_to_pandas(path=folder, filters=[('DTPROD', '=', param_execution_date)])

        # Definindo o tipo para DTPROD e LINEpara os dataframes
        df_dataop['DTPROD'] = df_dataop['DTPROD'].astype(str)
        df_dataconfirm['DTPROD'] = df_dataconfirm['DTPROD'].astype(str)        
        df_dataprod['DTPROD'] = df_dataprod['DTPROD'].astype(str)

        df_dataprod['LINE'] = df_dataprod['LINE'].astype(str)


        if (df_dataop is not None) and (df_dataconfirm is not None) and (df_dataprod is not None):

            # Copiando o DF dataop
            df_prod = df_dataop.copy()

            # Merge do DF dataop, com a consolidação do DF dataconfirm referente aos dados de Apontamento de produção
            df_prod = df_prod.merge(
                pd.DataFrame(
                    df_dataconfirm.groupby(
                        ['DTPROD', 'BATCH']
                    ).agg(
                        {
                            'PACKID': 'count',
                            'QTDUN': 'sum',
                            'KGPACK': 'sum',
                            'KGUNMED': 'mean'
                        }
                    )        
                ).reset_index(), 
                how='inner',
                on=['DTPROD', 'BATCH']
            )

            # Merge com a consolidação do DF df_prod referente aos dados de registro de produção das linhas
            df_prod = df_prod.merge( 
                pd.DataFrame(
                    df_dataprod.where((df_dataprod.STSID>=4)).groupby(
                        ['DTPROD','BATCH']
                    ).agg(
                        {
                            'QTDPCS': 'sum',
                            'QTDGOOD': 'sum',
                            'QTDREJECT': 'sum',
                            'TIMESTAMP': 'max'
                        }
                    )        
                ).reset_index(), 
                how='inner',
                on=['DTPROD', 'BATCH']
            )

            # Gerando a verificação de produto em processo WIP
            df_prod['TMAXLIMIT'] = df_prod.apply(lambda row:func.end_time(row['DTPROD'], 3),axis=1)
            df_prod['TMAXLIMIT'] = pd.to_datetime(df_prod['TMAXLIMIT'])
            df_prod['TMAXDIFF'] = (df_prod['TMAXLIMIT']-df_prod['TIMESTAMP']).dt.seconds/60
            df_prod['QTDWIP'] = df_prod.apply(lambda row:func.round_down(row['TMAXDIFF'], 0),axis=1)

            # renomeando as colunas
            df_prod.rename(columns = {'PACKID':'QTDPACKS', 'QTDUN':'QTDCONFIRM', 'KGPACK':'TOTKGPACK'}, inplace = True)

            # calculando a diferença entre o apontamento realizado e o registro de contagem de unidades boas
            df_prod['QTDIFF'] = df_prod['QTDCONFIRM'] - df_prod['QTDGOOD']

            # Estruturando as colunas para gravação no Data Lake
            df_prod = df_prod[['DTPROD', 'OP', 'DTINI', 'DTFIM', 'CODMAT', 'BATCH', 'QTDPLAN', 'QTDCONFIRM', 'QTDIFF', 'QTDPCS', 'QTDGOOD', 'QTDREJECT', 'QTDWIP', 'QTDPACKS', 'TOTKGPACK', 'KGUNMED']]

            # colocando o resultado no LOG para apoio na verificação
            print('DEBUG -> DF mesprod')
            print(df_prod)

            # save parquet - gcp

            # MES-PROD
            # Verifica se existe arquivos no path para deletar
            source = "mesprod"
            gcs.delete_blob(pathName=f"consumer-zone/{source}/DTPROD={param_execution_date}")

            # Gravando na Consumer-Zone
            gcs.write_pandas_to_parquet(path=f"edc-pa-i4-data/consumer-zone/{source}", df=df_prod, partitionCols=['DTPROD'])

            print(f'DEBUG -> DF {source} saved.')


            # DATA-OP
            # Verifica se existe arquivos no path para deletar
            source = "dataop"
            gcs.delete_blob(pathName=f"consumer-zone/{source}/DTPROD={param_execution_date}")

            # Gravando na Processing-Zone
            gcs.write_pandas_to_parquet(path=f"edc-pa-i4-data/consumer-zone/{source}", df=df_prod, partitionCols=['DTPROD'])

            print(f'DEBUG -> DF {source} saved.')


            # DATA-CONFIRM
            # Verifica se existe arquivos no path para deletar
            source = "dataconfirm"
            gcs.delete_blob(pathName=f"consumer-zone/{source}/DTPROD={param_execution_date}")

            # Gravando na Processing-Zone
            gcs.write_pandas_to_parquet(path=f"edc-pa-i4-data/consumer-zone/{source}", df=df_prod, partitionCols=['DTPROD'])

            print(f'DEBUG -> DF {source} saved.')


            # DATA-PROD
            # Verifica se existe arquivos no path para deletar
            source = "dataprod"
            gcs.delete_blob(pathName=f"consumer-zone/{source}/DTPROD={param_execution_date}")

            # Gravando na Processing-Zone
            gcs.write_pandas_to_parquet(path=f"edc-pa-i4-data/consumer-zone/{source}", df=df_prod, partitionCols=['DTPROD'])

            print(f'DEBUG -> DF {source} saved.')

            
        else:
            msg_error = f'ERROR -> Dataframe dataop, dataconfirm or dataprod is NULL for DTPROD={param_execution_date}'
            raise Exception( msg_error )
            