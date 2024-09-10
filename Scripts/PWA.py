from selenium import webdriver
import pandas as pd
from time import sleep
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import time
from selenium.webdriver.common.keys import Keys
import shutil
from selenium.webdriver.common.action_chains import ActionChains
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import sys
import openpyxl

# EXCLUSÃO DOS ARQUIVOS PRESENTES NA PASTA

def excluir_arquivos():
    folder = '/mnt/biprivado/12 - Diversos/PWA/BASES'
    for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print('Exclusão falha %s. Motivo: %s' % (file_path, e))

# WEBSCRAPING

def main():
    # try:
        def esperar_elemento_xpath(xpath):
            return WebDriverWait(driver, 300).until(EC.presence_of_element_located((By.XPATH, xpath)))
        
        options = Options()
        options.add_experimental_option("prefs", {
            "download.default_directory": "/mnt/biprivado/12 - Diversos/PWA/BASES",
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })

        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-extensions")
        options.add_argument("--proxy-server='direct://'")
        options.add_argument("--proxy-bypass-list=*")
        options.add_argument("--start-maximized")
        options.add_argument('--headless=new')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--enable-logging --v=1')

        # options.binary_location = '/mnt/biprivado/12 - Diversos/PWA/chrome-linux64'

        driver = webdriver.Chrome(options=options)

        driver.get('https://pwa.alcis.com.br/laredo/')

    
        #Realizar Login
        USUARIO = '#ucLogin_txtloginusuario'
        SENHA = '#ucLogin_txtloginsenha'
        BOTAO = '#ucLogin_btnLogar'

        usuario_element =  driver.find_element(By.CSS_SELECTOR, USUARIO)
        senha_element =  driver.find_element(By.CSS_SELECTOR, SENHA)
        botao_element =  driver.find_element(By.CSS_SELECTOR, BOTAO)

        usuario_element.send_keys('CDLAREDO')
        senha_element.send_keys('ALCIS12@')

        botao_element.click()

        sleep(1)

        #Acessando as consultas e realizando o download de cada uma

        def realizar_consulta(id_element_value, extensao):

            #REFRESH

            driver.refresh()

            #BUILDER

            builder = '//*[@id="TreeView1t28"]'
            builder_element = esperar_elemento_xpath(builder)
            builder_element.click()
            sleep(1)

            builder = '//*[@id="TreeView1t30"]'
            builder_element = esperar_elemento_xpath(builder)
            builder_element.click()
            sleep(1)

            #CONSULTA

            iframe_pai = esperar_elemento_xpath('//*[@id="conteudo"]/iframe[2]')
            driver.switch_to.frame(iframe_pai)

            iframe_filho = esperar_elemento_xpath('//*[@id="paiIframe"]')
            driver.switch_to.frame(iframe_filho)
            sleep(1)

            id_element = esperar_elemento_xpath('//*[@id="ASPxPageControl1_ASPxGridView1_DXFREditorcol2_I"]')
            id_element.click()
            id_element.send_keys(id_value)
            sleep(5)

            scroll = esperar_elemento_xpath('/html')
            scroll.click()
            scroll = ActionChains(driver)
            scroll.send_keys(Keys.SPACE).perform()
            sleep(1)

            selecao_element = esperar_elemento_xpath('//*[@id="ASPxPageControl1_ASPxGridView1_DXSelBtn0_D"]')
            sleep(2)
            selecao_element.click()

            executar_element = esperar_elemento_xpath('//*[@id="ASPxPageControl1_ASPxButton2_CD"]/span')
            executar_element.click()
            sleep(2)

            if extensao == 'xlsx':
                download_element = esperar_elemento_xpath('//*[@id="ASPxPageControl1_ImageButton5"]')
            elif extensao == 'csv':
                download_element = esperar_elemento_xpath('//*[@id="ASPxPageControl1_ImageButton2"]')

            download_element.click()
            sleep(5)

        base = '/mnt/biprivado/12 - Diversos/PWA/Consultas_PWA.xlsx'
        df = pd.read_excel(base)

        df = df[['Id', 'Nome Arquivo', 'Local do Arquivo', 'Extensões']]

        ids = df['Id'].unique().astype(str)
        nome_value = df['Nome Arquivo'].unique().astype(str)
        extensao = df['Extensões'].unique().astype(str)

        # Loop para realizar consultas para cada ID
        for id_value in ids:
            df_value = df[df['Id']== int(id_value)]
            nome_value = df_value['Nome Arquivo'].values[0]
            extensao = df_value['Extensões'].values[0]
        
            realizar_consulta(id_value, extensao)
            time.sleep(2)

            if extensao == 'xlsx':
                nome_consulta = '/mnt/biprivado/12 - Diversos/PWA/BASES/Consulta.xlsx'
            elif extensao == 'csv':
                nome_consulta = '/mnt/biprivado/12 - Diversos/PWA/BASES/Consulta.csv'

            time.sleep(5)
            
            novo_nome = f'/mnt/biprivado/12 - Diversos/PWA/BASES/{nome_value}.{extensao}'
            time.sleep(7)

            os.rename(nome_consulta, novo_nome)
            time.sleep(4)

            local = df_value['Local do Arquivo'].values[0]
            time.sleep(4)
            
            shutil.copy(novo_nome, local)

# WEBSCRAPING COM CONSULTAS DE ALTO CUSTO

def baixar():
        def esperar_elemento_xpath(xpath):
            return WebDriverWait(driver, 120).until(EC.presence_of_element_located((By.XPATH, xpath)))

        options = Options()
        options.add_experimental_option("prefs", {
            "download.default_directory": "/mnt/biprivado/12 - Diversos/PWA/BASES",
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })

        options.add_argument('--no-sandbox')
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-extensions")
        options.add_argument("--proxy-server='direct://'")
        options.add_argument("--proxy-bypass-list=*")
        options.add_argument("--start-maximized")
        options.add_argument('--headless=new')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--ignore-certificate-errors')

        # options.binary_location = '/mnt/biprivado/12 - Diversos/PWA/chrome-linux64'

        driver = webdriver.Chrome(options=options)

        driver.get('https://pwa.alcis.com.br/laredo/')

    
        #Realizar Login
        USUARIO = '#ucLogin_txtloginusuario'
        SENHA = '#ucLogin_txtloginsenha'
        BOTAO = '#ucLogin_btnLogar'

        usuario_element =  driver.find_element(By.CSS_SELECTOR, USUARIO)
        senha_element =  driver.find_element(By.CSS_SELECTOR, SENHA)
        botao_element =  driver.find_element(By.CSS_SELECTOR, BOTAO)

        usuario_element.send_keys('CDLAREDO')
        senha_element.send_keys('ALCIS12@')

        botao_element.click()

        sleep(1)

        def consultas(x, extensao):

            #REFRESH

            driver.refresh()

            #BUILDER

            builder = '//*[@id="TreeView1t28"]'
            builder_element = esperar_elemento_xpath(builder)
            builder_element.click()
            sleep(1)

            builder = '//*[@id="TreeView1t30"]'
            builder_element = esperar_elemento_xpath(builder)
            builder_element.click()
            sleep(1)

            #CUSTOMIZADOS

            customizados = '//*[@id="TreeView1t31"]'
            customizados_element = esperar_elemento_xpath(customizados)
            customizados_element.click()
            sleep(1)

            scroll = ActionChains(driver)
            scroll.send_keys(Keys.SPACE).perform()
            scroll = ActionChains(driver)
            scroll.send_keys(Keys.SPACE).perform()

            sleep(3)

            consulta = esperar_elemento_xpath(x)
            consulta.click()
            sleep(5)

            iframe_pai = esperar_elemento_xpath('/html/body/form/div[4]/div[2]/div/div[2]/iframe[3]')
            driver.switch_to.frame(iframe_pai)
            sleep(3)

            iframe_filho = esperar_elemento_xpath('/html/body/form/div[3]/div/div[2]/div/div[3]/iframe')
            driver.switch_to.frame(iframe_filho)

            sleep(30)

            if extensao == 'xlsx':
                download_element = esperar_elemento_xpath('/html/body/form/div[3]/div[1]/fieldset/div/input[4]')
            elif extensao == 'csv':
                download_element = esperar_elemento_xpath('/html/body/form/div[3]/div[1]/fieldset/div/input[1]')
            
            download_element.click()
            sleep(5)

        base = '/mnt/biprivado/12 - Diversos/PWA/PWA_2.xlsx'
        df = pd.read_excel(base)

        df = df[['Id', 'Nome Arquivo', 'Local do Arquivo', 'Extensões', 'Xpath', 'Nome Consulta']]

        xpath = df['Xpath'].unique().astype(str)
        nome_value = df['Nome Arquivo'].unique().astype(str)
        extensao = df['Extensões'].unique().astype(str)
        consulta = df['Nome Consulta'].unique().astype(str)

        for x in xpath:
            df_value = df[df['Xpath']== str(x)]
            nome_value = df_value['Nome Arquivo'].values[0]
            extensao = df_value['Extensões'].values[0]
            consulta = df_value['Nome Consulta'].values[0]

            consultas(x, extensao)
    
            if extensao == 'xlsx':
                            consulta = f'/mnt/biprivado/12 - Diversos/PWA/BASES/{consulta}.xlsx'
            elif extensao == 'csv':
                            consulta = f'/mnt/biprivado/12 - Diversos/PWA/BASES/{consulta}.csv'

            # consulta = '/mnt/biprivado/12 - Diversos/PWA/BASES/Consulta.xlsx'
            time.sleep(5)
            
            novo_nome = f'/mnt/biprivado/12 - Diversos/PWA/BASES/{nome_value}.{extensao}'
            time.sleep(7)

            os.rename(consulta, novo_nome)
            time.sleep(4)

            local = df_value['Local do Arquivo'].values[0]
            time.sleep(4)
            
            shutil.copy(novo_nome, local)

def validade():

    # Bases do PWA
    ARTIKEL_PRD = '/mnt/biprivado/12 - Diversos/PWA/BASES/ARTIKEL_PRD.xlsx'
    QUANTEN_EST = '/mnt/biprivado/12 - Diversos/PWA/BASES/QUANTEN_EST.xlsx'

    # Tratando dados do ARTIKEL_PRD
    df_prd = pd.read_excel(ARTIKEL_PRD)
    df_prd = df_prd[['BEZ_1', 'ID_ARTIKEL', 'ID_KLIENT']] \
            .rename(columns={'BEZ_1':'DESCPROD'})

    # Tratando dados do QUANTEN_EST
    df_est = pd.read_excel(QUANTEN_EST)
    df_est = df_est[['ORT', 'BEREICH', 'PLATZ', 'ID_ARTIKEL', 'MNG_FREI','TRENN_1', 'DATUM_VERFALL', 'ID_KLIENT', 'NR_LE_1',]]

    # Criando tabela única e manipulando dados
    df = pd.merge(df_est, df_prd, on=['ID_ARTIKEL', 'ID_KLIENT'], how='inner') \
        .rename(columns={'ORT':'DEP', 'BEREICH':'AREA','PLATZ':'ENDEREÇO', 'ID_ARTIKEL':'CODPROD','MNG_FREI':'QTDISP', 'DATUM_VERFALL':'VALIDADE', 'NR_LE_1':'UZ'})

    df['AMBIENTE'] = df['DEP'].apply(lambda x: 'RESFRIADO' if x == '1' else ('SECO' if x == '7' else 'CONGELADO'))
    df['CARAC. VALIDADE'] = pd.to_datetime(df['TRENN_1'], format='%Y%m%d', errors='coerce')
    df['CODPROD & DESC'] = df.apply(lambda row: f"{row['CODPROD']} - {row['DESCPROD']}", axis=1)

    df = df.loc[(df['TRENN_1'] != 0) & 
                (df['VALIDADE'] != df['CARAC. VALIDADE']) & 
                (df['UZ'].notnull()) & 
                (df['UZ'] != 0) & 
                (df['AREA'] == 'PP')].reset_index(drop=True) \

    df = df[['DEP', 'AREA', 'ENDEREÇO', 'AMBIENTE', 'CODPROD', 'DESCPROD', 'QTDISP', 'CARAC. VALIDADE', 'VALIDADE', 'UZ', 'CODPROD & DESC']]

    df.to_excel('/mnt/biprivado/1 - Credenciais/WMS/Controle de Validade RSFU/BASE_VAL_EST.xlsx', index=False, engine='openpyxl')

if __name__ == "__main__":
        excluir_arquivos()
        main()
        baixar()
        validade()

##########################################################################################
        
local_tz = pendulum.timezone('America/Fortaleza')

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 30, 9, 30, tzinfo=local_tz),
    'email': ['yurisantana@grupolaredo.com.br'],
    'retries': 0,
    'retry_delay': timedelta(seconds=10),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    dag_id='PWA',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    catchup=False,
    max_active_runs=1,
    schedule='30 6,13 * * *',
    tags=['alcis']
) as dag:

    exclusao = PythonOperator(
        task_id='exclusão',
        python_callable=excluir_arquivos,
        dag=dag,
    )

    base = PythonOperator(
        task_id='bases',
        python_callable=main,
        dag=dag,
    )

    base2 = PythonOperator(
        task_id='bases_alto_custo',
        python_callable=baixar,
        dag=dag,
    )

    base3 = PythonOperator(
          task_id='base_validade_estoque',
          python_callable=validade,
          dag=dag
    )

    exclusao >> base >> base2 >> base3
