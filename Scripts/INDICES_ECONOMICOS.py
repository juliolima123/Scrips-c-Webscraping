import psycopg2 
import requests
import pandas as pd
from io import StringIO
from selenium import webdriver
from time import sleep
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import os
import shutil
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def email_falha(log_erro):
    msg = MIMEMultipart()
    msg['From'] = 'juliocesarrilima17@gmail.com'
    emails = ['julio.lima@grupolaredo.com.br', 'yurisantana@grupolaredo.com.br']
    msg['To'] = ', '.join(emails)
    msg['Subject'] = 'ERRO - INDICADORES ECONÔMICOS'
    message = 'Bom dia a todos, tudo bem?\n\n'
    message += 'Ocorreu um erro ao exportar algum dos indicadores econômicos ao banco,\n\n'
    message += 'Log de erro:\n\n' + log_erro
    
    msg.attach(MIMEText(message))

    mailserver = smtplib.SMTP('smtp.gmail.com', 587)
    mailserver.ehlo()
    mailserver.starttls()
    mailserver.login('juliocesarrilima17@gmail.com', 'yuugqcwnzgklilbk')
    mailserver.sendmail('juliocesarrilima17@gmail.com', emails, msg.as_string())
    mailserver.quit()

def ipca():

    try: 

        # COLETANDO DADOS DO IPCA PELA API DO BANCO CENTRAL
        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados?formato=json"
        url_ipca = url.format(codigo_serie=433)

        response = requests.get(url_ipca)

        if response.status_code == 200:
                data = response.json()

        # CRIANDO E MANIPULANDO O DATAFRAME

        df = pd.DataFrame(data)

        df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')

        df = df.rename(columns={'valor': 'variacao_mensal'})
        df['variacao_mensal'] = (df['variacao_mensal'].astype(float))

        df = df.drop(range(12), axis=0).reset_index(drop=True)

        df['acumulado_12meses'] = df['variacao_mensal'].rolling(window=12, min_periods=1)\
                                                                .apply(lambda x: (1 + x / 100)\
                                                                .prod() - 1) * 100

        df['acumulado_12meses'] = df['acumulado_12meses'].map(lambda x: round(x, 2))

        # SE CONECTANDO COM O BANCO DW, E UPANDO DADOS

        conn = psycopg2.connect(
            database="DW_LAREDO",
            user="dwadmin",
            password="dwldfs@admin2022",
            host="10.10.0.48",
            port=5432
        )

        cur = conn.cursor()

        sio = StringIO()
        sio.write(df.to_csv(index=None, header=None)) 
        sio.seek(0)

        cur.execute("DELETE FROM financeiro.ipca")

        with conn.cursor() as c:
            c.copy_expert("COPY financeiro.ipca FROM STDIN WITH CSV", sio)
            conn.commit()
    
    except Exception as e:
        email_falha(str(e))

################################################

def cdi():

    try: 

        # COLETANDO DADOS DO CDI PELA API DO BANCO CENTRAL

        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados?formato=json"
        url_cdi = url.format(codigo_serie=12)

        response = requests.get(url_cdi)

        # CRIANDO E MANIPULANDO DF COM OS DADOS

        if response.status_code == 200:
            data = response.json()

            df = pd.DataFrame(data)
            df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')

            df = df.rename(columns={'valor': 'Variação Mensal (%)'})
            df['Variação Mensal (%)'] = df['Variação Mensal (%)'].astype(float)\
                                                                .map(lambda x: round(x, 2))\
                                                                .astype(float)

        # SE CONECTANDO COM O BANCO DW, E UPANDO DADOS

        conn = psycopg2.connect(
            database="DW_LAREDO",
            user="dwadmin",
            password="dwldfs@admin2022",
            host="10.10.0.48",
            port=5432
        )

        cur = conn.cursor()

        sio = StringIO()
        sio.write(df.to_csv(index=None, header=None)) 
        sio.seek(0)

        cur.execute("DELETE FROM financeiro.cdi")

        with conn.cursor() as c:
            c.copy_expert("COPY financeiro.cdi FROM STDIN WITH CSV", sio)
            conn.commit()

    except Exception as e:
        email_falha(str(e))

################################################

def inpc():

    try: 

        # COLETANDO DADOS DO INPC PELA API DO BANCO CENTRAL

        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados?formato=json"
        url_inpc = url.format(codigo_serie=188)

        response = requests.get(url_inpc)

        if response.status_code == 200:
            data = response.json()

        # CRIANDO E MANIPULANDO DF COM OS DADOS

            df = pd.DataFrame(data)
            df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')

            df['Variação Mensal (%)'] = df['valor'].rolling(window=12, min_periods=1)\
                                                .apply(lambda x: (1 + x / 100).prod() - 1) * 100
            
            df['Variação Mensal (%)'] = df['Variação Mensal (%)'].map(lambda x: round(x, 2))\
                                                                .astype(float)

            df = df.drop(columns=['valor'])


        # SE CONECTANDO COM O BANCO DW, E UPANDO DADOS

        conn = psycopg2.connect(
            database="DW_LAREDO",
            user="dwadmin",
            password="dwldfs@admin2022",
            host="10.10.0.48",
            port=5432
        )

        cur = conn.cursor()

        sio = StringIO()
        sio.write(df.to_csv(index=None, header=None)) 
        sio.seek(0)

        cur.execute("DELETE FROM financeiro.inpc")

        with conn.cursor() as c:
            c.copy_expert("COPY financeiro.inpc FROM STDIN WITH CSV", sio)
            conn.commit()

    except Exception as e:
        email_falha(str(e))

################################################

def selic():

    try: 

        # COLETANDO DADOS DO SELIC PELA API DO BANCO CENTRAL

        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados?formato=json"
        url_ipca = url.format(codigo_serie=4390)

        response = requests.get(url_ipca)

        #CRIANDO E MANIPULANDO DF COM OS DADOS

        if response.status_code == 200:
            data = response.json()

            df = pd.DataFrame(data)
            df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')

            df = df.rename(columns={'valor': 'Variação Mensal (%)'})
            
            df['Acumulado 12 Meses (%)'] = df['Variação Mensal (%)'].rolling(window=12, min_periods=1)\
                                                                    .apply(lambda x: (1 + x / 100).prod() - 1) * 100
            
            df['Acumulado 12 Meses (%)'] = df['Acumulado 12 Meses (%)'].map(lambda x: round(x, 2))\
                                                                    .astype(float)

        df.head(7)


        # SE CONECTANDO COM O BANCO DW, E UPANDO DADOS

        conn = psycopg2.connect(
            database="DW_LAREDO",
            user="dwadmin",
            password="dwldfs@admin2022",
            host="10.10.0.48",
            port=5432
        )

        cur = conn.cursor()

        sio = StringIO()
        sio.write(df.to_csv(index=None, header=None)) 
        sio.seek(0)

        cur.execute("DELETE FROM financeiro.selic")

        with conn.cursor() as c:
            c.copy_expert("COPY financeiro.selic FROM STDIN WITH CSV", sio)
            conn.commit()

    except Exception as e:
        email_falha(str(e))

################################################

def ipca15():

    try: 

        # COLETANDO DADOS DO IPCA-15 PELA API DO BANCO CENTRAL

        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados?formato=json"
        url_ipca15 = url.format(codigo_serie=7478)

        response = requests.get(url_ipca15)

        if response.status_code == 200:
            data = response.json()

        #estruturando as colunas mensais e acumuladas

            df = pd.DataFrame(data)
            df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')
            df = df.rename(columns={'valor': 'Variação Mensal (%)'})
            
            df['Variação Mensal (%)'] = df['Variação Mensal (%)'].astype(float)
            df['Acumulado 12 Meses (%)'] = df['Variação Mensal (%)'].rolling(window=12, min_periods=1)\
                                                                    .apply(lambda x: (1 + x / 100).prod() - 1) * 100

            
            df['Acumulado 12 Meses (%)'] = df['Acumulado 12 Meses (%)'].map(lambda x: round(x, 2)).astype(float)

        df.head(7)


        # SE CONECTANDO COM O BANCO DW, E UPANDO DADOS

        conn = psycopg2.connect(
            database="DW_LAREDO",
            user="dwadmin",
            password="dwldfs@admin2022",
            host="10.10.0.48",
            port=5432
        )

        cur = conn.cursor()

        sio = StringIO()
        sio.write(df.to_csv(index=None, header=None)) 
        sio.seek(0)

        cur.execute("DELETE FROM financeiro.ipca15")

        with conn.cursor() as c:
            c.copy_expert("COPY financeiro.ipca15 FROM STDIN WITH CSV", sio)
            conn.commit()

    except Exception as e:
            email_falha(str(e))

################################################

def igpm():

    try:

        # REALIZANDO SCRAPING E COLETANDO DADOS DO IGPM

        #IGPM

        def excluir_arquivos():
            folder = '/mnt/biprivado/12 - Diversos/IGPM'
            for filename in os.listdir(folder):
                    file_path = os.path.join(folder, filename) 
                    try:
                            if os.path.isfile(file_path) or os.path.islink(file_path):
                                os.unlink(file_path)
                            elif os.path.isdir(file_path):
                                shutil.rmtree(file_path)
                    except Exception as e:
                        print('Exclusão falha %s. Motivo: %s' % (file_path, e))

        excluir_arquivos()

        options = Options()
        options.add_experimental_option("prefs", {
            "download.default_directory": "/mnt/biprivado/12 - Diversos/IGPM",
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })

        options.add_argument("--disable-extensions")
        options.add_argument("--proxy-server='direct://'")
        options.add_argument("--proxy-bypass-list=*")
        options.add_argument("--start-maximized")
        options.add_argument('--headless=new')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--ignore-certificate-errors')

        # options.binary_location = '/mnt/biprivado/12 - Diversos/PWA/chrome-linux64'

        driver = webdriver.Chrome(options=options)

        driver.get('https://sindusconpr.com.br/igp-m-fgv-309-p')

        download = '/html/body/div[8]/div/div/div/div[1]/div[1]/div/div/div[3]/div/div[2]/div/table/tbody/tr/td[3]/a'
        download_element = driver.find_element(By.XPATH, download)
        download_element.click()
        sleep(5)
        
        driver.quit()

        sleep(5)

        pasta = '/mnt/biprivado/12 - Diversos/IGPM/'

        arquivos_na_pasta = os.listdir(pasta)

        if len(arquivos_na_pasta) == 1:
            nome_arquivo_antigo = arquivos_na_pasta[0]
            caminho_antigo = os.path.join(pasta, nome_arquivo_antigo)

            novo_nome = 'IGPM.xlsx'
            caminho_novo = os.path.join(pasta, novo_nome)

            os.rename(caminho_antigo, caminho_novo)
            
        base = '/mnt/biprivado/12 - Diversos/IGPM/IGPM.xlsx'

        df = pd.read_excel(base)

        df = df.drop(range(2), axis=0).reset_index(drop=True)
        df = df.rename(columns={'IGP-M': 'data', 'Unnamed: 1': 'Índice (%)', 'Unnamed: 2': 'No mês (%)', 'Unnamed: 4': 'Acumulado 12 Meses (%)'})\
            .drop(['Unnamed: 5', 'Unnamed: 6', 'Unnamed: 7', 'Unnamed: 3'], axis=1)\
            .drop(df.tail(1).index)

        def float(coluna):
            return pd.to_numeric(coluna, errors='coerce')

        i = ['Índice (%)', 'No mês (%)', 'Acumulado 12 Meses (%)']

        df[i] = df[i].apply(float)
        df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')

        # SE CONECTANDO COM O BANCO DW, E UPANDO DADOS

        conn = psycopg2.connect(
            database="DW_LAREDO",
            user="dwadmin",
            password="dwldfs@admin2022",
            host="10.10.0.48",
            port=5432
        )

        cur = conn.cursor()

        sio = StringIO()
        sio.write(df.to_csv(index=None, header=None)) 
        sio.seek(0)

        cur.execute("DELETE FROM financeiro.igpm")

        with conn.cursor() as c:
            c.copy_expert("COPY financeiro.igpm FROM STDIN WITH CSV", sio)
            conn.commit()

    except Exception as e:
        email_falha(str(e))

if __name__ == "__main__":
        ipca()
        cdi()
        inpc()
        selic()
        ipca15()
        igpm() 

################################################

# CRIANDO DAG PARA ORQUESTRAÇÃO

local_tz = pendulum.timezone('America/Fortaleza')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 11, tzinfo=local_tz),  
    'retries': 0, 
    'retry_delay': timedelta(minutes=5),  
}

with DAG(
    'ÍNDICES_ECONÔMICOS',
    default_args=default_args,
    description='Atualização das tabelas de índices econômicos',
    schedule='0 5,22 * * *', 
    catchup=False,
    max_active_runs=1,
    tags=['financeiro']
) as dag: 

    ipca1 = PythonOperator(
        task_id='ipca',
        python_callable=ipca,
        dag=dag,
    )

    cdi1 = PythonOperator(
        task_id='cdi',
        python_callable=cdi,
        dag=dag,
    )

    inpc1 = PythonOperator(
        task_id='inpc',
        python_callable=inpc,
        dag=dag,
    )

    selic1 = PythonOperator(
        task_id='selic',
        python_callable=selic,
        dag=dag,
    )

    ipca151 = PythonOperator(
        task_id='ipca15',
        python_callable=ipca15,
        dag=dag,
    )

    igpm1 = PythonOperator(
        task_id='igpm',
        python_callable=igpm,
        dag=dag,
    )

    ipca1 >> cdi1 >> inpc1 >> selic1 >> ipca151 >> igpm1













