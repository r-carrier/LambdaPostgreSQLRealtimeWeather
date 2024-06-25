import psycopg2
from psycopg2 import sql
import json
import urllib3
from botocore.config import Config
import boto3

secret_name = 'fishing-secrets'
insert_query = '''
    insert into fw1.realtime_weather
    (request_date, location_name, temperature, cloud_cover, precip_probability, rain_intensity, weather_code)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    '''

http = urllib3.PoolManager()


def get_secret():
    secret_client = boto3.client('secretsmanager')
    secret_response = secret_client.get_secret_value(
        SecretId=secret_name)
    secret_dict = json.loads(secret_response['SecretString'])
    return secret_dict


def connect_to_db(secret_dict):
    conn = psycopg2.connect(
        host=secret_dict['host'],
        port=secret_dict['port'],
        dbname=secret_dict['db_name'],
        user=secret_dict['db_username'],
        password=secret_dict['db_password']
    )
    return conn


def get_weather(secret_dict):
    fields = {'apikey': secret_dict['tomorrow_io_apikey'],
              'location': secret_dict['rt_location'],
              'units': secret_dict['rt_units']}
    response = http.request('GET',
                            'https://api.tomorrow.io/v4/weather/realtime',
                            fields=fields)
    data = json.loads(response.data)
    return data


def insert_into_db(conn, query, data):
    cur = conn.cursor()
    cur.execute(query, data)
    conn.commit()


def lambda_handler(event, context):
    secret_dict = get_secret()

    weather_data = get_weather(secret_dict)
    request_time = 'NOW()'
    location_name = weather_data['location']['name']
    temperature = weather_data['data']['values']['temperature']
    cloud_cover = weather_data['data']['values']['cloudCover']
    precip_probability = weather_data['data']['values']['precipitationProbability']
    rain_intensity = weather_data['data']['values']['precipitationProbability']
    weather_code = weather_data['data']['values']['weatherCode']

    insert_query_data = (request_time, location_name, temperature, cloud_cover, precip_probability, rain_intensity, weather_code)

    db_connection = connect_to_db(secret_dict)
    insert_into_db(db_connection, insert_query, insert_query_data)
    db_connection.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
