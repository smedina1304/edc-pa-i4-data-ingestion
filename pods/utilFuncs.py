# Classe: utilFuncs

# Importação das Bibliotecas:
import os
import math
import pandas as pd
from datetime import datetime, timedelta

# Declaração da Classe Principal
class utilFuncs:
    """
    ### Classe: utilFuncs

    Implementação de funções diversas de manipulação de dados comuns para o projeto
    
    by Sérgio C. Medina
    """

    # Metodo inicial de definição
    def __init__(self, debugMode=True):
        """
        Metodo inicial de definição

        Parametro:
        - debugMode: (False/True = Para habilitar mensagens no console)
        """

        self.__debugMode = debugMode if debugMode is not None else debugMode

    

    def calc_dtprod(self, dt, dateFormat='%Y-%m-%d', timeFormat='%H:%M:%S'):
        """
        Metodo utilizado para calcular data turno com base em uma data e hora

        Parametro:
        - dt: (data/hora = formato esperado "%Y-%m-%d %H:%M:%S")
        - dateFormat: (formatação da data - default '%Y-%m-%d')
        - timeFormat: (formatação da hora - default '%H:%M:%S')
        """

        # Se dt for string
        if isinstance(dt,str):
            dt = datetime.strptime(dt, f"{dateFormat} {timeFormat}")

        # separa a data e hora
        my_date = dt.date()
        my_hora = dt.time()

        # Formato hora 
        FMT = '%H:%M:%S'

        # Verifica periodo entre 06:00:00 e 23:59:59
        start_time = datetime.strptime('06:00:00', f"{timeFormat}").time()
        end_time = datetime.strptime('23:59:59', f"{timeFormat}").time()

        if (my_hora >= start_time and my_hora <= end_time):
            return my_date
        elif (my_hora < start_time):
            prevDay = my_date + timedelta(days=-1)
            return prevDay
        else:
            return None



    def calc_idturno(self, dt, dateFormat='%Y-%m-%d', timeFormat='%H:%M:%S'):
        """
        Metodo utilizado para calcular o ID do TURNO baseado em uma data e hora

        Parametro:
        - dt: (data/hora = formato esperado "%Y-%m-%d %H:%M:%S")
        - dateFormat: (formatação da data - default '%Y-%m-%d')
        - timeFormat: (formatação da hora - default '%H:%M:%S')
        """

        # Se dt for string
        if isinstance(dt,str):
            dt = datetime.strptime(dt, f"{dateFormat} {timeFormat}")

        # separa a hora
        my_hora = dt.time()

        id = None

        if self.is_between(my_hora,   '06:00:00', '13:59:59'):
            id = 1
        elif self.is_between(my_hora, '14:00:00', '21:59:59'):
            id = 2
        elif self.is_between(my_hora, '22:00:00', '23:59:59'):
            id = 3
        elif self.is_between(my_hora, '00:00:00', '05:59:59'):
            id = 3

        return id



    def is_between(self, check_time, start_time, end_time, format_str_time='%H:%M:%S'):
        """
        Metodo utilizado para verificar se uma data está dentro do período definido

        Parametro:
        - check_time: (hora que será verificada)
        - start_time: (hora inicial do período)
        - end_time: ( hora final do período)
        - format_str_time: (formatação da hora - default '%H:%M:%S')
        """

        if isinstance(check_time,str):
            check_time = datetime.strptime(check_time, format_str_time).time()
        
        if isinstance(start_time,str):
            start_time = datetime.strptime(start_time, format_str_time).time()
        
        if isinstance(end_time,str):
            end_time = datetime.strptime(end_time, format_str_time).time()

        return (start_time <= check_time <= end_time)



    def end_time(self, dtprd, idturno):
        """
        Metodo que retorno a data e hora de final do turno

        Parametro:
        - dtprd: (data de produção esperada no formado "%Y-%m-%d")
        - idturno: (id do turno que pode ser 1,2 ou 3)
        """

        # Se dtprd for string
        if isinstance(dtprd,str):
            dtprd = datetime.strptime(dtprd, "%Y-%m-%d").date()

        if idturno == 1:
            dt = f'{datetime.strftime(dtprd, "%Y-%m-%d")} 13:59:59'
        elif idturno == 2:
            dt = f'{datetime.strftime(dtprd, "%Y-%m-%d")} 21:59:59'
        elif idturno == 3:
            dtprd = dtprd + timedelta(days=1)
            dt = f'{datetime.strftime(dtprd, "%Y-%m-%d")} 05:59:59'

        return dt


    def start_time(self, dtprd, idturno):
        """
        Metodo que retorno a data e hora de inicio do turno

        Parametro:
        - dtprd: (data de produção esperada no formado "%Y-%m-%d")
        - idturno: (id do turno que pode ser 1,2 ou 3)
        """

        # Se dtprd for string
        if isinstance(dtprd,str):
            dtprd = datetime.strptime(dtprd, "%Y-%m-%d").date()

        if idturno == 1:
            dt = f'{datetime.strftime(dtprd, "%Y-%m-%d")} 06:00:00'
        elif idturno == 2:
            dt = f'{datetime.strftime(dtprd, "%Y-%m-%d")} 14:00:00'
        elif idturno == 3:
            dt = f'{datetime.strftime(dtprd, "%Y-%m-%d")} 22:00:00'

        return dt




    def compate_dtprod(self, di, df):
        """
        Metodo que compara se a data inicio e final informada estão na mesma data de produção
        se sim retorna a data de produção, senão retorna None

        Parametro:
        - di: (data de inicio "%Y-%m-%d")
        - df: (data de final "%Y-%m-%d")
        """

        di = self.calc_dtprod(di)
        df = self.calc_dtprod(df)

        if (di == df):
            return di
        else:
            return None


    # defining a function for round down.
    def round_down(self, n, decimals=0):
        """
        Metodo utilizado para fazer o arredondamento para baixo.

        Parametro:
        - n: (valor para ser arredondado)
        - decimals: (quantidade de casas decimais - default = 0)
        """        
        multiplier = 10 ** decimals
        return math.floor(n * multiplier) / multiplier