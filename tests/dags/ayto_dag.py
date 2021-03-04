from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
# from airflow.hooks.filesystem import FSHook
# from plugins.smart_file_sensor import SmartFileSensor

import logging
import json

default_args = {
    'start_date': days_ago(1),
}

@dag(schedule_interval='@hourly', default_args=default_args, catchup=False)
def smartsensor():
#     FILENAME = 'bitcoin.json'

#     sensors = [
#         SmartFileSensor(
#             task_id=f'waiting_for_file_{sensor_id}',
#             filepath=FILENAME,
#             fs_conn_id='fs_default'
#         ) for sensor_id in range(1, 10)
#     ]

    @task
    def fetchAytoData():
        import os
        import wget
        import pandas as pd
        import pickle
        import boto3
        # from dateutil.relativedelta import relativedelta
        aytoURL = "http://www.mambiente.madrid.es/opendata/horario.txt"
        fname = '/tmp/horario.txt'
        if os.path.exists(fname):
            os.remove(fname)
        fname = wget.download(aytoURL,out='/tmp/horario.txt',bar=None)
        bucket_name = 'accuayto'
        path_file = 'txtHeaders.pkl'
        s3 = boto3.resource('s3',
            endpoint_url='http://minio:9000',
            config=boto3.session.Config(signature_version='s3v4'),aws_access_key_id='alboroto',
            aws_secret_access_key='!Alboroto7'
        )
        headers = pickle.loads(s3.Bucket(bucket_name).Object(path_file).get()['Body'].read())
        #headers = pickle.load(open('/usr/local/airflow/downloads/txtHeaders.pkl', 'rb'))
        # headers = pickle.load(open('txtHeaders.pkl', 'rb'))
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
    #     display('status',status,status.dtypes)
    #     display('values',values,values.dtypes)
        values['VERIFIED'] = status['VERIFIED'].astype(bool).values
        allvalues = values.reset_index(drop=True)
        allvalues['HORA'] = allvalues['HORA'].apply(
            lambda x: "0"+str(int(x.replace('H', ''))-1))
        allvalues['ANO'] = allvalues['ANO'].astype(str)
        allvalues['MES'] = allvalues['MES'].astype(str)
        allvalues['DIA'] = allvalues['DIA'].astype(str)
        allvalues['DATETIME'] = allvalues['ANO']+"-"+allvalues['MES']+"-"+allvalues['DIA']+" "+allvalues['HORA']+":00:00"
        allvalues['DATETIME'] = pd.to_datetime(allvalues['DATETIME'])+pd.Timedelta('1 hours')  # pd.offsets.Hour(1)
        # print(allvalues['DATETIME'].unique())
        allvalues = allvalues.drop(['ANO', 'MES', 'DIA', 'HORA'], axis=1)
        allvalues = allvalues[list(allvalues.columns[:4])+['VERIFIED', 'DATETIME', 'VALOR']]
    #     display('allvalues',allvalues.dtypes)
        return allvalues.to_json()
    
#     sensors >> () >> storing()
    aytoData = fetchAytoData()

dag = smartsensor()
