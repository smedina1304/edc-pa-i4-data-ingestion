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
        
        # Caregando os dados da processing - dataprod
        source = "dataprod"
        folder = f"{bucketName}/processing-zone/{source}"
        df_dataprod = gcs.read_parquet_to_pandas(path=folder, filters=[('DTPROD', '=', param_execution_date)])

        # # Verifica se os DF foi carregado com dados
        if (df_dataprod is not None):

            # Definindo o tipo para DTPROD e LINEpara os dataframes
            df_dataprod['DTPROD'] = df_dataprod['DTPROD'].astype(str)
            df_dataprod['LINE'] = df_dataprod['LINE'].astype(str)

            # OEE-LINE
            # Consolidando dados de registro de Produção por status da linha
            df_sts = pd.DataFrame(df_dataprod.groupby(
                ['DTPROD', 'IDTURNO', 'LINE', 'STSID', 'STSDS']
            ).agg(
                {
                    'TOTMIN': 'sum',
                    'QTDPCS': 'sum',
                    'QTDGOOD': 'sum',
                    'QTDREJECT': 'sum'
                }
            )).reset_index()

            # Clonando a base do DATAPROD
            df = df_dataprod.copy()

            # Preparando para consolidação de dados por Data, turno e Linha 
            df['TMIN'] = df['TIMESTAMP']
            df['TMAX'] = df['TIMESTAMP']
            df.drop(['TIMESTAMP'], axis=1, inplace=True)

            # Consolidando dados de registro de Produção por linha com status = 4 (WORKING)
            df = pd.DataFrame(
                df.where((df.STSID>=4)).groupby(
                    ['DTPROD', 'IDTURNO', 'LINE']
                ).agg(
                    {
                        'TOTMIN': 'sum',
                        'QTDPCS': 'sum',
                        'QTDGOOD': 'sum',
                        'QTDREJECT': 'sum',
                        'TMIN': 'min',
                        'TMAX': 'max'
                    }
                )
            ).reset_index()

            # Tratamento das informações
            df['TMAXLIMIT'] = df.apply(lambda row:func.end_time(row['DTPROD'], row['IDTURNO']),axis=1)
            df['TMAXLIMIT'] = pd.to_datetime(df['TMAXLIMIT'])
            df['TMAXDIFF'] = (df['TMAXLIMIT']-df['TMAX']).dt.seconds/60
            df['QTDWIP'] = df.apply(lambda row:func.round_down(row['TMAXDIFF'], 0),axis=1)

            df['TMINLIMIT'] = df.apply(lambda row:func.start_time(row['DTPROD'], row['IDTURNO']),axis=1)
            df['TMINLIMIT'] = pd.to_datetime(df['TMINLIMIT'])
            df['TMINDIFF'] = (df['TMIN']-df['TMINLIMIT']).dt.seconds/60

            df['DIFF'] = df['TMAXDIFF'].diff().fillna(0)

            df['TOTMINADJUSTED'] = round(df['TOTMIN'] + df['DIFF'],0)

            df.drop(['TMAX', 'TMAXLIMIT', 'TMAXDIFF'], axis=1, inplace=True)
            df.drop(['TMIN', 'TMINLIMIT', 'TMINDIFF', 'DIFF'], axis=1, inplace=True)

            # Totalizando Tempo turno
            df['TPTOTAL'] = 480
            print('DEBUG -> [ANTES AJUSTE]:','TOTMINADJUSTED=',df['TOTMINADJUSTED'].sum(),'\n\r',df['TOTMINADJUSTED'])
            df['TOTMINADJUSTED'] = df.apply(lambda row:(row['TOTMINADJUSTED'] if row['TOTMINADJUSTED']<480 else 480),axis=1)
            print('DEBUG -> [DEPOIS AJUSTE]:','TOTMINADJUSTED=',df['TOTMINADJUSTED'].sum(),'\n\r',df['TOTMINADJUSTED'])

            # Totalizando Tempo = WORKING
            df['TPWORKING'] = df.apply(
                lambda row:df_sts.where(
                (df_sts.STSID>=4) &
                (df_sts.DTPROD==row['DTPROD']) &
                (df_sts.IDTURNO==row['IDTURNO']) &
                (df_sts.LINE==row['LINE']) 
                ).agg(
                    {'TOTMIN':'sum'}
                )
                ,axis=1)

            print('DEBUG -> [ANTES AJUSTE]:','TPWORKING=',df['TPWORKING'].sum(),'\n\r',df['TPWORKING'])
            df['TPWORKING'] = df.apply(lambda row:(row['TPWORKING'] if row['TPWORKING']<=row['TOTMINADJUSTED'] else row['TOTMINADJUSTED']),axis=1)
            print('DEBUG -> [DEPOIS AJUSTE]:','TPWORKING=',df['TPWORKING'].sum(),'\n\r',df['TPWORKING'])

            # Totalizando Tempo = TPSTOPPLAN
            df['TPSTOPPLAN'] = df.apply(
                lambda row:df_sts.where(
                (df_sts.STSID==3) &
                (df_sts.DTPROD==row['DTPROD']) &
                (df_sts.IDTURNO==row['IDTURNO']) &
                (df_sts.LINE==row['LINE']) 
                ).agg(
                    {'TOTMIN':'sum'}
                )
                ,axis=1)

            # Totalizando Tempo = TPNOALLOC
            df['TPNOALLOC'] = df.apply(
                lambda row:df_sts.where(
                (df_sts.STSID==0) &
                (df_sts.DTPROD==row['DTPROD']) &
                (df_sts.IDTURNO==row['IDTURNO']) &
                (df_sts.LINE==row['LINE']) 
                ).agg(
                    {'TOTMIN':'sum'}
                )
                ,axis=1)  

            # Tempo Programado para produzir
            df['TPPROG'] = df['TPTOTAL'] - (df['TPNOALLOC']+df['TPSTOPPLAN'])

            # Tempo de Ociosidade
            df['TPIDLE'] = df.apply(
                lambda row:df_sts.where(
                (df_sts.STSID>=1) &
                (df_sts.STSID<3) &
                (df_sts.DTPROD==row['DTPROD']) &
                (df_sts.IDTURNO==row['IDTURNO']) &
                (df_sts.LINE==row['LINE']) 
                ).agg(
                    {'TOTMIN':'sum'}
                )
                ,axis=1)

            df['TPIDLE'] = ((df['TPPROG']-(df['TPWORKING'] + df['TPIDLE']))+df['TPIDLE'])

            # Unidades - Produção Teórica
            df['QTDPCSTHEOR'] = df['TPWORKING'] * 1

            # Unidades - Perda por performance
            df['QTDPCSLOSS'] = df['QTDPCSTHEOR'] - df['QTDPCS']

            # OEE Calc 
            df['OEEDISP'] = round(df['TPWORKING'] / df['TPPROG'],3)
            df['OEEPERF'] = round(df['QTDPCS'] / df['QTDPCSTHEOR'],3)
            df['OEEQUAL'] = round(df['QTDGOOD'] / df['QTDPCS'],3)
            df['OEE'] = round(df['OEEDISP'] * df['OEEPERF'] * df['OEEQUAL'],3)

            # colocando o resultado no LOG para apoio na verificação
            print('DEBUG -> DF mesoeeline')
            print(df)


            # OEE-DT
            # Consolidando os dados de OEE por DIA
            df_oee = pd.DataFrame(
                df.groupby(
                    ['DTPROD']
                ).agg(
                    {
                        'TPTOTAL': 'sum',
                        'TPPROG': 'sum',
                        'TPWORKING': 'sum',
                        'TPSTOPPLAN': 'sum',
                        'TPNOALLOC': 'sum',
                        'TPIDLE': 'sum',
                        'QTDPCS': 'sum',
                        'QTDGOOD': 'sum',
                        'QTDREJECT': 'sum',
                        'QTDPCSTHEOR': 'sum',
                        'QTDPCSLOSS': 'sum',

                    }
                )
            ).reset_index()

            df_oee['OEEDISP'] = round(df_oee['TPWORKING']/df_oee['TPPROG'],3)
            df_oee['OEEPERF'] = round(df_oee['QTDPCS'] / df_oee['QTDPCSTHEOR'],3)
            df_oee['OEEQUAL'] = round(df_oee['QTDGOOD'] / df_oee['QTDPCS'],3)
            df_oee['OEE'] = round(df_oee['OEEDISP'] * df_oee['OEEPERF'] * df_oee['OEEQUAL'],3)


            # colocando o resultado no LOG para apoio na verificação
            print('DEBUG -> DF mesoeedt')
            print(df)


            # save parquet - gcp

            # MES-OEE-LINE
            # Verifica se existe arquivos no path para deletar
            source = "mesoeeline"
            gcs.delete_blob(pathName=f"consumer-zone/{source}/DTPROD={param_execution_date}")

            # Gravando na Consumer-Zone
            gcs.write_pandas_to_parquet(path=f"edc-pa-i4-data/consumer-zone/{source}", df=df, partitionCols=['DTPROD'])

            print(f'DEBUG -> DF {source} saved.')


            # MES-OEE-DT
            # Verifica se existe arquivos no path para deletar
            source = "mesoeedt"
            gcs.delete_blob(pathName=f"consumer-zone/{source}/DTPROD={param_execution_date}")

            # Gravando na Processing-Zone
            gcs.write_pandas_to_parquet(path=f"edc-pa-i4-data/consumer-zone/{source}", df=df_oee, partitionCols=['DTPROD'])

            print(f'DEBUG -> DF {source} saved.')

            
        else:
            msg_error = f'ERROR: Dataframe dataop, dataconfirm or dataprod is NULL for DTPROD={param_execution_date}'
            #raise Exception( msg_error )
            print( "DEBUG ->", msg_error )
            