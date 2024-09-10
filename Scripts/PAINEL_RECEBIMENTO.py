#IMPORTANDO BIBLIOTECAS
import requests
import pandas as pd
import cx_Oracle
import numpy as np
from datetime import datetime, timedelta
import pytz
import os
from selenium import webdriver
import pandas as pd
from time import sleep
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
from selenium.webdriver.common.keys import Keys
import shutil
from selenium.webdriver.common.action_chains import ActionChains
import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum


def antigo():

    #Puxando excel
    df_pwa = pd.read_excel('/mnt/biprivado/1 - Credenciais/WMS/KPI ARMAZEM/PAINEL_REC/RECEBIMENTO_CAIXAS_ANTIGO.xlsx',
                        dtype = {
                                'CODPROD': np.dtype("str"),
                                'USUARIO': np.dtype("str"),
                                'DEPOSITANTE': np.dtype("str"),
                                'QTD': np.dtype("float"),
                                'DEPPROV': np.dtype("str"),
                                'PESO': np.dtype("float")},
                        decimal=',')

    # Verificar se um arquivo de controle existe
    if os.path.exists('/mnt/biprivado/1 - Credenciais/WMS/KPI ARMAZEM/PAINEL_REC/TXT/primeira_execucao.txt'):
        # Se o arquivo existe, considerar uma coluna
        ultima_att = df_pwa[df_pwa['DTREC'] == df_pwa['DTREC'].max()] 
    else:
        # Se o arquivo não existe, considerar outra coluna
        ultima_att = df_pwa[df_pwa['DTREC'] == df_pwa['DTREC'].min()] 
        ultima_att['DTREC'] = ultima_att['DTREC'] - timedelta(days=1)

    # Utilizar a variável coluna_a_considerar em seu código
    # ...

    # Após a execução, criar o arquivo de controle para indicar que não é a primeira execução
    if not os.path.exists('/mnt/biprivado/1 - Credenciais/WMS/KPI ARMAZEM/PAINEL_REC/TXT/primeira_execucao.txt'):
        with open('/mnt/biprivado/1 - Credenciais/WMS/KPI ARMAZEM/PAINEL_REC/TXT/primeira_execucao.txt', 'w') as f:
            f.write('Arquivo de controle para verificar a primeira execução.')

    #Deixando apenas as colunas desejadas
    colunas_desejadas = ['DTREC']
    ultima_att = ultima_att[colunas_desejadas]
    #Criando Novo Excel
    caminho = '/mnt/biprivado/1 - Credenciais/WMS/KPI ARMAZEM/PAINEL_REC/RECEBIMENTO_CAIXAS_ANTIGO.xlsx'
    ultima_att.to_excel(caminho,index=False)
    print(ultima_att)



def RECEBIMENTO():
    def excluir():
        arquivo = os.path.join('/mnt/biprivado/12 - Diversos/PWA/RECEBIMENTO_CX', 'RECEBIMENTO_CAIXAS.xlsx')
        if os.path.exists(arquivo):
            os.remove(arquivo)
        else:
            print('erro')
    excluir()

    def scraping():
        def esperar_elemento_xpath(xpath):
            return WebDriverWait(driver, 180).until(EC.presence_of_element_located((By.XPATH, xpath)))

        options = Options()
        options.add_experimental_option("prefs", {
            "download.default_directory": "/mnt/biprivado/12 - Diversos/PWA/RECEBIMENTO_CX",
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

        # options.binary_location = '/usr/bin/google-chrome'

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

        sleep(5)


        #REFRESH

        driver.refresh()

        #BUILDER

        builder = '//*[@id="TreeView1t28"]'
        builder_element = esperar_elemento_xpath(builder)
        builder_element.click()
        sleep(5)

        builder = '//*[@id="TreeView1t30"]'
        builder_element = esperar_elemento_xpath(builder)
        builder_element.click()
        sleep(5)

        #CUSTOMIZADOS

        customizados = '//*[@id="TreeView1t31"]'
        customizados_element = esperar_elemento_xpath(customizados)
        customizados_element.click()
        sleep(5)

        scroll = ActionChains(driver)
        scroll.send_keys(Keys.SPACE).perform()
        scroll = ActionChains(driver)
        scroll.send_keys(Keys.SPACE).perform()
        scroll = ActionChains(driver)
        scroll.send_keys(Keys.SPACE).perform()

        sleep(10)

        consulta = driver.find_element(By.CSS_SELECTOR, '#TreeView1t136')
        consulta.click()

        # consulta = driver.find_element('xpath')
        # driver.execute_script('arguments[0].click();', consulta)

        sleep(5)

        iframe_pai = esperar_elemento_xpath('/html/body/form/div[4]/div[2]/div/div[2]/iframe[3]')
        driver.switch_to.frame(iframe_pai)
        sleep(3)

        iframe_filho = esperar_elemento_xpath('/html/body/form/div[3]/div/div[2]/div/div[3]/iframe')
        driver.switch_to.frame(iframe_filho)

        sleep(30)

        download_element = esperar_elemento_xpath('/html/body/form/div[3]/div[1]/fieldset/div/input[4]')
        download_element.click()
        sleep(5)



        shutil.copy('/mnt/biprivado/12 - Diversos/PWA/RECEBIMENTO_CX/RECEBIMENTO_CAIXAS.xlsx', '/mnt/biprivado/1 - Credenciais/WMS/KPI ARMAZEM/PAINEL_REC/RECEBIMENTO_CAIXAS.xlsx')

        
    scraping()

def dados():
        #PUXANDO DADOS PWA
    df_pwa = pd.read_excel('/mnt/biprivado/1 - Credenciais/WMS/KPI ARMAZEM/PAINEL_REC/RECEBIMENTO_CAIXAS.xlsx',
                        dtype = {
                                'CODPROD': np.dtype("str"),
                                'USUARIO': np.dtype("str"),
                                'DEPOSITANTE': np.dtype("str"),
                                'QTD': np.dtype("float"),
                                'DEPPROV': np.dtype("str"),
                                'PESO': np.dtype("float")},
                        decimal=',')
    df_usuarios = pd.read_excel('/mnt/biprivado/1 - Credenciais/WMS/KPI ARMAZEM/USUARIOS.xlsx')
    df_usuarios = df_usuarios[df_usuarios['TURNO']=='DIA'] 

    #FAZENDO A CONEXÃO COM O BANCO

    # cx_Oracle.init_oracle_client(lib_dir='/opt/oracle/instantclient_12_2')

    dsn = cx_Oracle.makedsn('192.168.0.125', '1521', 'ORCLBI')

    conn = cx_Oracle.connect('laredobi', 'laredo01bi', '192.168.0.125:1521/ORCL')

    cur = conn.cursor()

    #PUXANDO OS DADOS DO BANCO

    sql = "SELECT p.CODPROD, p.DESCRICAO, p.PESOBRUTO, p.PESOVARIAVEL  FROM LAREDO.PCPRODUT p WHERE 1=1"
    cur.execute(sql)
    tab = cur.fetchall()
    colunas = ['CODPROD','DESCRICAO','PESOBRUTO','PESOVARIAVEL']
    df = pd.DataFrame(tab, columns=colunas)
    df['DEPOSITANTE'] = '2'
    df['CODPROD'] = df['CODPROD'].astype(str)

    conn.commit()
    cur.close()
    #UNINDO TABELAS
    df = df_pwa.merge(df[['CODPROD','PESOBRUTO','PESOVARIAVEL','DEPOSITANTE']], left_on = ['CODPROD','DEPOSITANTE'], right_on = ['CODPROD','DEPOSITANTE'],how = 'left')
    df1 = df.merge(df_usuarios[['NOME','USUARIO']], left_on = ['USUARIO'], right_on = ['USUARIO'], how= 'left') 

    df1['PESOTOTAL'] = (df1['PESOBRUTO']*df1['QTD'])

    #PEGANDO PESO CORRETO
    df1['PESO'] = np.where(df1['PESOVARIAVEL'] == 'N', df1['PESOTOTAL'], df1['PESO'])

    df1['NOME'] = np.where(df1['NOME'].isna(), df1['USUARIO'], df1['NOME']) 

    #Última Separacao do arquivo anterior   
    ultima_att = pd.read_excel('/mnt/biprivado/1 - Credenciais/WMS/KPI ARMAZEM/PAINEL_REC/RECEBIMENTO_CAIXAS_ANTIGO.xlsx',
                        decimal=',')

    #Atualizando novo arquivo para assim criar o fluxo de dados
    caminho = '/mnt/biprivado/1 - Credenciais/WMS/KPI ARMAZEM/PAINEL_REC/RECEBIMENTO_CAIXAS_ANTIGO.xlsx'

    df1.to_excel(caminho,index=False)

    #Filtrando dados df2
    #
    df2 = df1[df1['DTREC'].gt(ultima_att['DTREC'].max())]
    colunas_desejadas = ['NOME','PESO']
    df2 = df2[colunas_desejadas]
    # Converte o DataFrame para um formato adequado para enviar via POST
    dados_para_enviar = df2.to_json(orient='records')

    # Substitua 'sua_url_api' pela URL real da sua API
    url_api = 'https://api.powerbi.com/beta/886ba64f-2dd9-4a27-ba3e-a04556f96107/datasets/919546c9-814a-45a7-8ac5-766590b81b83/rows?experience=power-bi&key=M3SN3zl5Occ%2FhItKbHSK9z3oyL%2BouyOAkjBKUVBdgaYfvCywdXkp%2FPPcG4tA2wXwkf85vJW4qx35aQ%2B6Pqkxew%3D%3D'
    # Define o cabeçalho da requisição (opcional, dependendo da API)
    headers = {'Content-Type': 'application/json'}

    # Envia os dados para a API
    response = requests.post(url_api, data=dados_para_enviar, headers=headers)

    # Verifica o código de status da resposta
    if response.status_code == 200:
        print("Dados enviados com sucesso!")
    else:
        print(f"Falha ao enviar dados. Código de status: {response.status_code}")
    print(response.text)
    df2.head()


if __name__ == "__main__":
    RECEBIMENTO()
    antigo()
    dados()

################################################
    
local_tz = pendulum.timezone('America/Fortaleza')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 16, 11, 0, 0, tzinfo=local_tz),  
    'retries': 0, 
    'retry_delay': timedelta(minutes=5),  
}


with DAG(
    'PAINEL_REC',
    default_args=default_args,
    description='Projeto de injestão para API - Recebimento de Caixas',
    schedule='*/5 7-18 * * 0-6', 
    catchup=False,
    max_active_runs=1,
    tags=['armazém']
) as dag: 

    scraping = PythonOperator(
        task_id='Webscraping',
        python_callable=RECEBIMENTO,
        dag=dag,
    )
  
    historico = PythonOperator(
        task_id='Histórico',
        python_callable=antigo,
        dag=dag,
    ) 

    dados1 = PythonOperator(
        task_id='Ingestão_api',
        python_callable=dados,
        dag=dag,
    )

scraping >> historico >> dados1
