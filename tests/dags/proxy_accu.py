from airflow.decorators import dag, task
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.selenium_plugin import SeleniumOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.models.taskinstance import _CURRENT_CONTEXT
# from airflow.task.context import get_current_context
from airflow.utils.dates import days_ago
# from airflow.hooks.filesystem import FSHook
# from plugins.smart_file_sensor import SmartFileSensor
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.proxy import Proxy, ProxyType
import logging
import json
import pandas as pd
import boto3
import io
import os
import traceback
import time
from datetime import datetime, timedelta
from lxml import html
from selenium import webdriver

default_args = {
    'start_date': days_ago(1),
    'provide_context': True
}

cwd = os.getcwd()
local_downloads = '/usr/local/airflow/downloads'
sel_downloads = '/home/seluser/downloads'


options = Options()
options.add_argument("--headless")
options.add_argument("--window-size=1920x1080")
options.add_argument("--incognito")
options.add_argument("--lang={}".format('es-ES')) 
options.add_argument("--ignore-certificate-errors")
options.add_experimental_option("prefs", {"profile.default_content_settings.cookies": 2}) # noqa
#chrome_driver = '{}:4444/wd/hub'.format('http://localhost') # This is only required for local development
#chrome_driver = '{}:4444/wd/hub'.format('http://ec2-52-31-168-109.eu-west-1.compute.amazonaws.com') # This is only required for local development
capabilities=DesiredCapabilities.CHROME
capabilities['acceptSslCerts'] = True
capabilities['acceptInsecureCerts'] = True
# options.add_argument("--disable-extensions")
# # options.add_argument("--disable-dev-shm-usage")
# options.add_argument('--no-sandbox')
# chrome_driver = '{}:4444/wd/hub'.format('http://selenium-hub') # This is only required for local development

def get_proxies(driver=None):
    global PROXIES, options
    """Returns a list of proxies

    Args:
        chrome_opts ([webdriver.ChromeOptions]): Chrome Driver Options.

    Returns:
        List: set of proxies
    """
    
    PROXIES = []
    while len(PROXIES) == 0:
        chrome_driver = driver.command_executor
        if driver:
            driver.quit()
        while True:
            try:
                driver = webdriver.Remote(command_executor=chrome_driver,desired_capabilities=capabilities,options=options)
                print('remote ready')
                break
            except:

                print(traceback.print_exc(),'remote not ready, sleeping for ten seconds.')
                time.sleep(10)
        driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
        params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': sel_downloads}}
        command_result = driver.execute("send_command", params)
        print('outta loop')

        try:
            driver.get("https://free-proxy-list.net/")
            proxies = driver.find_elements_by_css_selector("tr[role='row']")
            for p in proxies[1:]:
                result = p.text.split(" ")
                if result[-1] == 'ago' and result[-4] == "yes":
                    PROXIES.append(result[0]+":"+result[1])
                elif result[-1] == 'yes':
                    PROXIES.append(result[0]+":"+result[1])
        except:
            print(traceback.print_exc(),'error proxies')
    return [driver,PROXIES]


# def proxy_driver(driver=None,PROXIES=[]):
def proxy_driver(PROXIES=[]):
    global options, driver
    """[summary]

    Args:
        PROXIES ([type]): [description]
        co ([type], optional): [description]. Defaults to co.

    Returns:
        [type]: [description]
    """
    if len(PROXIES) < 1:
        print("--- Proxies used up (%s)" % len(PROXIES))
        driver,PROXIES = get_proxies()
        print(PROXIES)
    pxy = PROXIES.pop(random.randrange(len(PROXIES)))

    des_cap = DesiredCapabilities.CHROME
    des_cap['acceptSslCerts'] = True
    des_cap['acceptInsecureCerts'] = True
    prox = Proxy()
    prox.proxy_type = ProxyType.MANUAL
    prox.httpProxy = pxy
    prox.ftpProxy = pxy
    prox.sslProxy = pxy
    prox.add_to_capabilities(des_cap)
    chrome_driver = driver.command_executor
    driver.quit()
    del driver
    driver = webdriver.Remote(command_executor=chrome_driver,desired_capabilities=des_cap,options=options)
    driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
    params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': sel_downloads}}
    command_result = driver.execute("send_command", params)
    try:
        driver.get("https://api.ipify.org")
    except:
        driver.quit()
        del driver
        proxy_driver()
    return [driver,PROXIES]

#def indexAccuStations(driver,row, days=5):
def indexAccuStations(driver,row, local_downloads):
    days = 5
    accuDF = pd.DataFrame()
    url_template = 'https://www.accuweather.com/es/es/Madrid/Madrid/hourly-weather-forecast/{}?day={}' # noqa
    accuMapValues = {}
    idStation = row['ESTACION']
    accuMapValues['ESTACION'] = idStation
    htmlDay = None

    for day in range(1, days):
        print(f'day:{day}, station: {row["ESTACION_STR"]}')
        url = url_template.format(row["KEY"], day)
        elems = []
        while len(elems)==0:
            try:
                print(html.fromstring(driver.page_source, 'lxml').text)
                driver.get(url)
                driver.implicitly_wait(20)
                print(html.fromstring(driver.page_source, 'lxml').text)
                print('elems:', len(driver.find_elements_by_xpath('/html/body/div[1]/div[5]/div[1]/div[1]/div[1]/div[@class="accordion-item hourly-card-nfl hour non-ad"]')))
                if len(driver.find_elements_by_xpath('/html/body/div[1]/div[5]/div[1]/div[1]/div[1]/div[@class="accordion-item hourly-card-nfl hour non-ad"]')) < 1:
                    raise Exception('None accu elements')
                if len(driver.find_elements_by_xpath('/html/body/div[1]/div[5]/div[1]/div[1]/div[1]/div[@class="accordion-item hourly-card-nfl hour non-ad"]')) is None:
                    raise Exception('None accu elements')
                elems = driver.find_elements_by_xpath('/html/body/div[1]/div[5]/div[1]/div[1]/div[1]/div[@class="accordion-item hourly-card-nfl hour non-ad"]')
                if len(elems) == 0:
                    driver, PROXIES = get_proxies(driver)
            except:  # noqa
                print(traceback.print_exc())
                driver, PROXIES = get_proxies(driver)
                driver, PROXIES = proxy_driver(driver, PROXIES)
        print(f"elems {elems} {type(elems)}")
        print('okkk')
        htmlDay = html.fromstring(driver.page_source, 'lxml')
        accuDF = pd.read_json(indexAccuHours(accuDF, accuMapValues, htmlDay))
#     print(accuDF.drop_duplicates())
    return accuDF.drop_duplicates().to_parquet(f"{local_downloads}/{idStation}.parquet")

def indexAccuHours(accuDF, accuMapValues, htmlDay):
    print('about to fuck')
    for index, hourDiv in enumerate(htmlDay.xpath(
            '/html/body/div[1]/div[5]/div[1]/div[1]/div[1]/div[@class="accordion-item hourly-card-nfl hour non-ad"]')):
        print('fuckkkkkkkkk')
        header = hourDiv.xpath('.//div[1]/div')[0]
        data = hourDiv.xpath('.//div[2]/div')[0]
        accuMapValues['hour'], accuMapValues['dayMonth'] = [
            elem.text_content().strip() for elem in header.xpath(
                './/h2[@class="date"]/span')]
        print(accuMapValues)
        print(header.xpath('.//div[1]//text()'))
        accuMapValues['airTemp(cels)'] = float(header.xpath(
            './/div[1]//text()')[0].replace(u'\xa0', u' ').strip()[:-1])
        values = data.text_content()
        values = values.replace('Viento', 'Viento ').replace('Humedad', 'Humedad ').replace('rocío', 'rocío ')
        values = list(map(lambda x: x.strip(), values.split()))
        idWindSpd = values.index('Viento')+1
        idHumidity = values.index('Humedad')+1
        accuMapValues['humidity%'] = float(values[idHumidity])
        idDewPoint = values.index('rocío')+1
        accuMapValues['dewPointTemp(cels)'] = float(values[idDewPoint].replace('°', ''))
        if values[idDewPoint+1] == 'F':
            accuMapValues['dewPointTemp(cels)'] = (accuMapValues['dewPointTemp(cels)'] - 32) * 5/9
            accuMapValues['airTemp(cels)'] = (accuMapValues['airTemp(cels)'] - 32) * 5/9
            values[idWindSpd+1] = f"{float(values[idWindSpd+1]) * 0.6214}"

        accuMapValues['wind'] = ' '.join(values[idWindSpd:idWindSpd+3])
        accuDF = accuDF.append(accuMapValues, ignore_index=True)
        print(accuMapValues)
        accuDF = accuDF.append(accuMapValues, ignore_index=True)
    return accuDF.to_json()

with DAG(dag_id='proxyAccuData', description='Accuweather', start_date=days_ago(1),end_date=None, schedule_interval='@hourly',  concurrency=4, max_active_runs=1, default_args=default_args) as dag:
    buffer = io.BytesIO()
    bucket_name = 'accuayto'
    path_file = 'aytoMetClust.parquet'
    s3 = boto3.resource('s3',
        endpoint_url='http://minio:9000',
        config=boto3.session.Config(signature_version='s3v4'),aws_access_key_id='alboroto',
        aws_secret_access_key='!Alboroto7'
    )
    s3.Bucket(bucket_name).Object(path_file).download_fileobj(buffer)

    accuStations = pd.read_parquet(buffer)
    accu = accuStations.rename({'CODIGO_CORTO': 'ESTACION'}, axis=1)
    resDF = pd.DataFrame()
    for index, row in accu.iterrows():
        tid = 'AccuStation_' + f"{accu.iloc[index]['ESTACION_STR'].replace(' ','_')}"
        task1 = SeleniumOperator(
                script = indexAccuStations,
                script_args = [accu.iloc[index],local_downloads],
                task_id = tid)
        
        # resDF = resDF.append(pd.read_parquet(f"{local_downloads}/row['CODIGO_CORTO'].parquet"),ignore_index=True)
        # resDF = resDF.append(pd.read_json(task1.xcom_pull(key='return_value',context=_CURRENT_CONTEXT)),ignore_index=True)

    # return resDF.to_json()
    
    # accuData = initAccu()
# dag = fetchAccuData()


'''@dag(schedule_interval='@hourly', default_args=default_args, catchup=False)
def fetchAccuData():
    @task
    def initAccu():
        buffer = io.BytesIO()
        bucket_name = 'accuayto'
        path_file = 'aytoMetClust.parquet'
        s3 = boto3.resource('s3',
            endpoint_url='http://minio:9000',
            config=boto3.session.Config(signature_version='s3v4'),aws_access_key_id='alboroto',
            aws_secret_access_key='!Alboroto7'
        )
        s3.Bucket(bucket_name).Object(path_file).download_fileobj(buffer)

        accuStations = pd.read_parquet(buffer)
        accu = accuStations.rename({'CODIGO_CORTO': 'ESTACION'}, axis=1)
        resDF = pd.DataFrame()
        for index, row in accu.iterrows():
            tid = 'AccuStation_' + f"{accu.iloc[index]['ESTACION_STR'].replace(' ','_')}"
            task1 = SeleniumOperator(
                    script = indexAccuStations,
                    script_args = [accu.iloc[index],local_downloads],
                    task_id = tid)
            resDF = resDF.append(pd.read_parquet(f"{local_downloads}/row['CODIGO_CORTO'].parquet"),ignore_index=True)
            # resDF = resDF.append(pd.read_json(task1.xcom_pull(key='return_value',context=_CURRENT_CONTEXT)),ignore_index=True)

        return resDF.to_json()
    
    accuData = initAccu()
dag = fetchAccuData()
'''
