from airflow.decorators import dag, task
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from plugins.selenium_plugin import SeleniumOperator
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
import numpy as np
import boto3
import io
import os
import traceback
import time
import pickle
import wget
# import unicodedata
from datetime import datetime, timedelta
from lxml import html
from selenium import webdriver
from sqlalchemy import create_engine
from sqlalchemy import text

default_args = {
    'start_date': days_ago(1),
    'provide_context': True
}

cwd = os.getcwd()
sel_downloads = '/opt/airflow/downloads'
#if ~os.path.exists(sel_downloads):
#    os.mkdir(sel_downloads)
local_downloads = '/opt/airflow/seluser/downloads'
#if ~os.path.exists(local_downloads):
#    os.mkdir(local_downloads)
# sel_downloads = '/home/seluser/downloads'


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
            print('loopa')
            #driver.quit()
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
    '''if len(PROXIES) < 1:
        print("--- Proxies used up (%s)" % len(PROXIES))
        driver,PROXIES = get_proxies()
        print(PROXIES)
    pxy = PROXIES.pop(random.randrange(len(PROXIES)))
    '''
    des_cap = DesiredCapabilities.CHROME
    des_cap['acceptSslCerts'] = True
    des_cap['acceptInsecureCerts'] = True
    '''prox = Proxy()
    prox.proxy_type = ProxyType.MANUAL
    prox.httpProxy = pxy
    prox.ftpProxy = pxy
    prox.sslProxy = pxy
    prox.add_to_capabilities(des_cap)'''
    chrome_driver = driver.command_executor
    #driver.quit()
    #del driver
    #driver = webdriver.Remote(command_executor=chrome_driver,desired_capabilities=des_cap,options=options)
    #driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
    #params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': sel_downloads}}
    #command_result = driver.execute("send_command", params)
    try:
        driver.get("https://api.ipify.org")
    except:
        print('pass')
        #driver.quit()
        #del driver
        #proxy_driver()
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
                driver.implicitly_wait(10)
                print(html.fromstring(driver.page_source, 'lxml').text)
                print('elems:', len(driver.find_elements_by_xpath('/html/body/div[1]/div[5]/div[1]/div[1]/div[1]/div[@class="accordion-item hourly-card-nfl hour non-ad"]')))
                if len(driver.find_elements_by_xpath('/html/body/div[1]/div[5]/div[1]/div[1]/div[1]/div[@class="accordion-item hourly-card-nfl hour non-ad"]')) < 1:
                    raise Exception('None accu elements')
                if len(driver.find_elements_by_xpath('/html/body/div[1]/div[5]/div[1]/div[1]/div[1]/div[@class="accordion-item hourly-card-nfl hour non-ad"]')) is None:
                    raise Exception('None accu elements')
                elems = driver.find_elements_by_xpath('/html/body/div[1]/div[5]/div[1]/div[1]/div[1]/div[@class="accordion-item hourly-card-nfl hour non-ad"]')
                if len(elems) == 0:
                    print('zero')
                    #driver, PROXIES = get_proxies(driver)
            except:  # noqa
                print(traceback.print_exc())
                #driver, PROXIES = get_proxies(driver)
                #driver, PROXIES = proxy_driver(driver, PROXIES)
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

def compassToDeg(accuDF):
    df = accuDF.copy()
    compToDegMap = dict(zip(["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSO", "SO", "OSO", "O", "ONO", "NO", "NNO"], np.arange(0, 350, 22.5)))
    df['compass'] = df['wind'].apply(lambda x: x.split(' ')[0])
    df['windSpdRate(m/s)'] = df['wind'].apply(
        lambda x: float(x.split(' ')[1]) if(x.split(' ')[0] != '0') else 0.0)
    df['windDirAngle(clockwise)'] = df['compass'].apply(lambda x: compToDegMap[x] if x != '0' else 0)
    df['day'] = df['dayMonth'].apply(lambda x: x.split('/')[0])
    df['month'] = df['dayMonth'].apply(lambda x: x.split('/')[1])
    df['DATETIME'] = '2020-'+df['month']+'-'+df['day']
    df['DATETIME'] = pd.to_datetime(df['DATETIME'])
    df['DATETIME'] = df['DATETIME']+pd.to_timedelta(df['hour'].astype(str)+' hours')
    df = df.drop(['compass', 'wind', 'day', 'month', 'hour', 'dayMonth'],
                 axis=1)
    df.columns = list(map(lambda x: x.upper(), df.columns))
    return df

def download_ayto():
    aytoURL = "http://www.mambiente.madrid.es/opendata/horario.txt"
    fname = '/tmp/horario.txt'
    if os.path.exists(fname):
        os.remove(fname)
    fname = wget.download(aytoURL,out='/tmp/horario.txt',bar=None)
    df = pd.read_csv(fname, encoding='ISO-8859-1', sep=',',
                    names=headers).drop(['PUNTO_MUESTREO', 'TXTH'], axis=1)
    allvalues = df.copy()
    for col in allvalues.filter(regex='V\d+', axis=1).columns:
        allvalues[col] = allvalues[col].apply(lambda x: x == 'V')
    allvalues = allvalues.pivot_table(
        index=['PROVINCIA', 'MUNICIPIO', 'ESTACION', 'ANO', 'MES', 'DIA', 'MAGNITUD']).T.unstack().reset_index()
    status = allvalues[~allvalues['level_7'].str.startswith('H')].rename(
        {0: 'VERIFIED'}, axis=1).drop(['level_7'], axis=1)
    values = allvalues[~allvalues.index.isin(status.index)].rename(
        {0: 'VALOR', 'level_7': 'HORA'}, axis=1)
    values['VERIFIED'] = status['VERIFIED'].astype(bool).values
    allvalues = values.reset_index(drop=True)
    allvalues['HORA'] = allvalues['HORA'].apply(
        lambda x: "0"+str(int(x.replace('H', ''))-1))
    allvalues['ANO'] = allvalues['ANO'].astype(str)
    allvalues['MES'] = allvalues['MES'].astype(str)
    allvalues['DIA'] = allvalues['DIA'].astype(str)
    allvalues['DATETIME'] = allvalues['ANO']+"-"+allvalues['MES']+"-"+allvalues['DIA']+" "+allvalues['HORA']+":00:00"
    allvalues['DATETIME'] = pd.to_datetime(allvalues['DATETIME'])+pd.Timedelta('1 hours')  # pd.offsets.Hour(1)
    allvalues = allvalues.drop(['ANO', 'MES', 'DIA', 'HORA'], axis=1)
    allvalues = allvalues[list(allvalues.columns[:4])+['VERIFIED', 'DATETIME', 'VALOR']]
    allvalues.to_parquet('/usr/local/airflow/downloads/ayto.parquet')

def mapAytoCalLoc():
    aytoCalData = pd.read_parquet('/usr/local/airflow/downloads/ayto.parquet')
    aytoCal = aytoCalData.copy()
    aytoCal.loc[(aytoCal['VALOR'] == 0) & (~aytoCal['VERIFIED']), 'VALOR'] = np.nan
    aytoCal['MAGNITUD'] = aytoCal['MAGNITUD'].astype(str)
    aytoCal['VALOR'] = pd.to_numeric(aytoCal['VALOR'], errors='coerce')
    aytoCal = aytoCal.drop(['PROVINCIA', 'MUNICIPIO', 'VERIFIED'], axis=1).groupby(['DATETIME', 'ESTACION', 'MAGNITUD']).mean().unstack()
    aytoCal.columns = list(map(lambda x: f'm{x[1]}', aytoCal.columns))
    aytoCal.columns = list(map(lambda x: x.upper(), aytoCal.columns))
    aytoCal = aytoCal.reset_index().drop(['M22'], axis=1)
    accuData = pd.DataFrame()
    for index, row in accu.iterrows():
        accuData = accuData.append(pd.read_parquet(f'/usr/local/airflow/downloads/{row["ESTACION"]}.parquet'),ignore_index=True)
    accuData = compassToDeg(accuData.copy())
    accuData = accuData.drop_duplicates(keep='last', subset=['ESTACION', 'DATETIME'])
    aytoCalData = aytoCalData.drop_duplicates(keep='last', subset=['ESTACION', 'DATETIME'])
    ayto1 = aytoCal.merge(accuData, on=['ESTACION', 'DATETIME'], how='inner')
    ayto2 = aytoCal.merge(accuData, on=['ESTACION', 'DATETIME'], how='outer')
    aytoFull = pd.concat([ayto1, ayto2]).drop_duplicates(keep='first', subset=['ESTACION', 'DATETIME']).sort_values(by=['ESTACION', 'DATETIME'])
    aytoFull.to_parquet('/usr/local/airflow/downloads/full.parquet')

def insertToPostgres(tablename='sensordata'):
    newData = pd.read_parquet('/usr/local/airflow/downloads/full.parquet')
    newData.columns = newData.columns.str.lower()
    tablename = tablename.lower()
    eng = create_engine('postgresql://airflow:airflow@airflow-postgresql:5432/airflow')
    with eng.connect() as conn:
        # conn.execute(text(f"DROP TABLE IF EXISTS {tablename};"))
        conn.execute(text("DROP TABLE IF EXISTS temp_table;"))
        newData.columns = newData.columns.map(lambda x: x.lower().split('(')[0].split('%')[0])
        if tablename not in eng.table_names():
            newData.to_sql(tablename, con=conn, index=False, if_exists='replace',)
            conn.execute(
                text(
                    f"ALTER TABLE {tablename} ADD PRIMARY KEY (datetime,estacion);"
                )
            )
        else:

            # step 1 - create temporary table and upload DataFrame
            conn.execute(
                text(
                    f"CREATE TEMPORARY TABLE temp_table as select * from {tablename} with no data;"
                )
            )
            newData.to_sql("temp_table", conn, index=False, if_exists="append")

            # step 2 - merge temp_table into main_table
            conn.execute(
                text(
                    f"INSERT INTO {tablename} (datetime, estacion, m1, m10, m12, m14, m20, m30, m35,\
                            m42, m43, m44, m6, m7, m8, m9, airtemp, dewpointtemp, humidity, windspdrate, winddirangle) \
                        SELECT * FROM temp_table ON CONFLICT(datetime, estacion) \
                            DO UPDATE SET m1 = COALESCE({tablename}.m1,excluded.m1), m10 = COALESCE({tablename}.m10,excluded.m10), m12 = COALESCE({tablename}.m12,excluded.m12), \
                            m14 = COALESCE({tablename}.m14,excluded.m14), m20 = COALESCE({tablename}.m20,excluded.m20), m30 = COALESCE({tablename}.m30,excluded.m30), \
                            m35 = COALESCE({tablename}.m35,excluded.m35), m42 = COALESCE({tablename}.m42,excluded.m42), m43 = COALESCE({tablename}.m43,excluded.m43), \
                            m44 = COALESCE({tablename}.m44,excluded.m44), m6 = COALESCE({tablename}.m6,excluded.m6), m7 = COALESCE({tablename}.m7,excluded.m7), \
                            m8 = COALESCE({tablename}.m8,excluded.m8), m9 = COALESCE({tablename}.m9,excluded.m9), airtemp = COALESCE({tablename}.airtemp,excluded.airtemp), \
                            dewpointtemp = COALESCE({tablename}.dewpointtemp,excluded.dewpointtemp), humidity = COALESCE({tablename}.humidity,excluded.humidity), \
                            windspdrate = COALESCE({tablename}.windspdrate,excluded.windspdrate), winddirangle = COALESCE({tablename}.winddirangle,excluded.winddirangle);"
                )
            )

        # step 3 - confirm results
        result = conn.execute(text(f"SELECT * FROM {tablename}  ORDER BY datetime, estacion;")).fetchall()
        print(result)  # [(1, 'row 1 new text'), (2, 'new row 2 text')]


with DAG(dag_id='MadridDag', description='Accuweather + Pollution Pipelines', start_date=days_ago(1),end_date=None, schedule_interval='@hourly',  concurrency=4, max_active_runs=1, default_args=default_args) as dag:

    buffer = io.BytesIO()
    bucket_name = 'accuayto'
    path_file = 'aytoMetClust.parquet'
    s3 = boto3.resource('s3',
        endpoint_url='http://airflow-minio:9000',
        config=boto3.session.Config(signature_version='s3v4'),aws_access_key_id='alboroto',
        aws_secret_access_key='!Alboroto7'
    )
    s3.Bucket(bucket_name).Object(path_file).download_fileobj(buffer)

    accuStations = pd.read_parquet(buffer)
    # accuStations['ESTACION_STR'] = accuStations['ESTACION_STR'].apply(lambda x: unicodedata.normalize('NFKD', x).encode('ASCII', 'ignore').decode().replace(' ','_').replace('.',''))
    # accuStations['MET_STATION'] = accuStations['MET_STATION'].apply(lambda x: unicodedata.normalize('NFKD', x).encode('ASCII', 'ignore').decode().replace(' ','_').replace('.',''))
    headers = pickle.loads(s3.Bucket(bucket_name).Object("txtHeaders.pkl").get()['Body'].read())
    accu = accuStations.rename({'CODIGO_CORTO': 'ESTACION'}, axis=1)
    resDF = pd.DataFrame()
    start = DummyOperator(task_id="start")
    with TaskGroup("section_accu", tooltip="Accu Tasks") as section_accu:
        for index, row in accu.iterrows():
            tid = 'AccuStation_' + f"{accu.iloc[index]['ESTACION_STR']}"
            task1 = PythonOperator(
                    python_callable = indexAccuStations,
                    op_args = [accu.iloc[index],local_downloads],
                    task_id = tid
                    )
            #task1 = SeleniumOperator(
            #        script = indexAccuStations,
            #        script_args = [accu.iloc[index],local_downloads],
            #        task_id = tid)

    some_other_task = DummyOperator(task_id="some-other-task")

    with TaskGroup("section_map", tooltip="Ayto+Accu Data") as section_all:
        task1 = PythonOperator(
                    python_callable = download_ayto,
                    op_args = [],
                    task_id = 'download_ayto')
        
        task2 = PythonOperator(
                    python_callable = mapAytoCalLoc,
                    op_args = [],
                    task_id = 'full_pack')

        task3 = PythonOperator(
                    python_callable = insertToPostgres,
                    op_args = [],
                    task_id = 'postgres_upsert')
        
        task1 >> task2 >> task3
        
        # resDF = resDF.append(pd.read_parquet(f"{local_downloads}/row['CODIGO_CORTO'].parquet"),ignore_index=True)
        # resDF = resDF.append(pd.read_json(task1.xcom_pull(key='return_value',context=_CURRENT_CONTEXT)),ignore_index=True)
    end = DummyOperator(task_id='end')
 
    start >> section_accu >> some_other_task >> section_all >> end
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
            endpoint_url='http://airflow-minio:9000',
            config=boto3.session.Config(signature_version='s3v4'),aws_access_key_id='alboroto',
            aws_secret_access_key='!Alboroto7'
        )
        s3.Bucket(bucket_name).Object(path_file).download_fileobj(buffer)

        accuStations = pd.read_parquet(buffer)
        accu = accuStations.rename({'CODIGO_CORTO': 'ESTACION'}, axis=1)
        # accuStations['ESTACION_STR'].apply(lambda x: unicodedata.normalize('NFKD', x).encode('ASCII', 'ignore').decode().replace(' ','_').replace('.',''))
        # accuStations['MET_STATION'].apply(lambda x: unicodedata.normalize('NFKD', x).encode('ASCII', 'ignore').decode().replace(' ','_').replace('.',''))

        resDF = pd.DataFrame()
        for index, row in accu.iterrows():
            tid = 'AccuStation_' + f"{accu.iloc[index]['ESTACION_STR']}"
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
