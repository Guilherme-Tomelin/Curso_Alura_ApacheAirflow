import os
import pandas as pd
from os.path import join
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()


start_date = datetime.today()
final_date = start_date + timedelta(days=7)

start_date = start_date.strftime('%Y-%m-%d')
final_date = final_date.strftime('%Y-%m-%d')

city = 'Boston'
key = os.getenv('KEY')

URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
            f'{city}/{start_date}/{final_date}?unitGroup=metric&include=days&key={key}&contentType=csv')

data = pd.read_csv(URL)
print(data.head())

file_path = f'C:\\Users\\guilhermesantos\\Desktop\\Curso\\Apache Airflow\\Curso_Alura_ApacheAirflow\\datapipeline\\semana={start_date}'
os.mkdir(file_path)

data.to_csv(file_path + 'raw_data.csv')
data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv')
data[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')
