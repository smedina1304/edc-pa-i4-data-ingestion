# Classe: utilGCS

# Importação das Bibliotecas:
import io
import pandas as pd
import gcsfs
import pyarrow
import pyarrow.parquet as pq
from google.cloud import storage


# Declaração da Classe Principal
class utilGCS:
    """
    ### Classe: utilGCS

    Implementação de funções para facilitar a interação com os recursos utilizados do Google Cloud Storage
    
    by Sérgio C. Medina
    """

    # Metodo inicial de definição
    def __init__(self, bucketName=None, debugMode=True):
        """
        Metodo inicial de definição

        Parametro:
        - bucketName: (Nome do Bucket)
        - debugMode: (False/True = Para habilitar mensagens no console)
        """

        self.__debugMode = debugMode if debugMode is not None else debugMode

        self.__clearAll()

        if bucketName is not None:
            self.initBucket(bucketName)



    def __clearAll(self):
        """
        Metodo utilizado para limpar o conteúdo dos elementos de memória
        utilizados nesta classe de processamento

        Parametro:
        - None
        """

        self.storage_client = None
        self.bucket = None
        self.gs = None


    def initBucket(self, bucketName):
        """
        Metodo utilizado para inicializar Conexão com o Cloud Storage e criação do objeto bucket referente a área de dados.

        Necessário que a variével de ambiente 'GOOGLE_APPLICATION_CREDENTIALS' esteja definida para o acesso as credenciais de autenticação e permissão

        Parametro:
        - bucketName: (Nome do Bucket)
        """

        self.storage_client = storage.Client()
        self.bucket = self.storage_client.get_bucket(bucketName)
        self.gs = gcsfs.GCSFileSystem()

    
    def delete_blob(self, pathName):
        """
        Metodo utilizado para deletar a lista de blobs (arquivos) contidos em uma determinada pasta no Bucket do GCS.

        Parametro:
        - pathName: (caminho da pasta no bucket)
        """        
        if pathName is not None:
            if self.__debugMode:
                print('DEBUG ->','SEARCHING TO DELETE:', pathName)
            blobs = self.bucket.list_blobs(prefix=pathName)
            for blob in blobs:
                if blob.exists():
                    if self.__debugMode:
                        print('DEBUG ->','DELETE:',blob.path)
                    blob.delete()
        else:
            print('ERROR ->','ERROR: pathName is None!')


    
    def read_csv_to_df(self, folder, dtexec, lineprod=None, sep=";"):
        """
        Metodo utilizado para leitura do arquivo CSV no GCS convertando para Pandas Dataframe. 
        Os parametros(dtexec, lineprod) são utilizados para no padrão de nomenclatura do projeto. 

        Parametro:
        - folder: (caminho da pasta no bucket)
        - dtexec: (data de identificação do arquivo)
        - lineprod: (identificação da linha de produção)
        - sep: (separador utilizado no CSV)
        """ 

        # formata o padrão do nome do arquivo
        file_name = None
        if (dtexec is not None and lineprod is None):
            file_name = f"{dtexec}.csv"
        elif (dtexec is not None and lineprod is not None):
            file_name = f"{lineprod}-{dtexec}.csv"
        else:
            raise Exception("Invalid parameters.")

        df = None

        try:
            # define o objeto blob com o path para o arquivo no GCS
            blob = self.bucket.blob(f"{folder}/{file_name}")

            # Faz o download no formato em bytes e posiciona no index 0
            byte_stream = io.BytesIO()
            blob.download_to_file(byte_stream)
            byte_stream.seek(0)

            # Converte para o Dataframe em Pandas
            df = pd.read_csv(byte_stream, sep=sep)
        except Exception as e:
            print('ERROR ->',"Exception:", e)

        return df


    def write_pandas_to_parquet(self, path, df, partitionCols):
        """
        Metodo utilizado para gravar o conteúdo de um Pandas Dataframe em formato Parquet com definições de partição.

        Parametro:
        - path: (caminho da pasta no bucket)
        - df: (Pandas Dataframe)
        - partitionCols: (lista de campos que será realizado a partição no padrão Parquet)
        """ 

        # save parquet - gcp
        # https://gist.github.com/lpillmann/fa1874c7deb8434ca8cba8e5a045dde2
        # https://blog.datasyndrome.com/python-and-parquet-performance-e71da65269ce

        table = pyarrow.Table.from_pandas(df)

        pq.write_to_dataset(
            table,
            path,
            partition_cols=partitionCols,
            filesystem=self.gs,
            compression='snappy',
            flavor='spark'
        )

        if self.__debugMode:
            print('DEBUG ->',"SUCCESS:", path)


    
    def read_parquet_to_pandas(self, path, filters=None):
        """
        Metodo utilizado para carregar o conteúdo de um Parquet com definições de partição e converter e formato Pandas Dataframe.

        Parametro:
        - path: (caminho da pasta no bucket)
        - filters: (lista de filtros para seleção no padrão Parquet)
        """ 
        dataset = None
        df = None

        try:
            if filters is not None:
                dataset = pq.ParquetDataset(
                    path_or_paths=path, 
                    filesystem=self.gs,
                    filters=filters
                #    filters=[('DTPROD', '=', '2021-11-08')]
                #    use_threads=True
                )
            else:
                dataset = pq.ParquetDataset(
                    path_or_paths=path, 
                    filesystem=self.gs
                )              

            df = dataset.read_pandas().to_pandas()
        except:
            df = None

        return df

