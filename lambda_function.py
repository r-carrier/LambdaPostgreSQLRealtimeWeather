import psycopg2
from psycopg2 import sql
import json
import urllib3
from botocore.config import Config
import boto3
import logging

secret_name = 'fishing-secrets'
insert_query = '''
    insert into fw1.t_realtime_weather
    (request_date, location_name, temperature, cloud_cover, precip_probability, rain_intensity, weather_code)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    '''

http = urllib3.PoolManager()
logging.basicConfig(level=logging.INFO)

def get_secret():
    try:
        secret_client = boto3.client('secretsmanager')
        secret_response = secret_client.get_secret_value(
            SecretId=secret_name)
        secret_dict = json.loads(secret_response['SecretString'])
        return secret_dict
    except Exception as e:
        logging.error(f"Error getting secret: {e}")
        raise


def connect_to_db(secret_dict):
    try:
        conn = psycopg2.connect(
            host=secret_dict['host'],
            port=secret_dict['port'],
            dbname=secret_dict['db_name'],
            user=secret_dict['db_username'],
            password=secret_dict['db_password']
        )
        return conn
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        raise


def get_weather(secret_dict):
    try:
        fields = {'apikey': secret_dict['tomorrow_io_apikey'],
                  'location': secret_dict['rt_location'],
                  'units': secret_dict['rt_units']}
        response = http.request('GET',
                                'https://api.tomorrow.io/v4/weather/realtime',
                                fields=fields)
        data = json.loads(response.data)
        return data
    except Exception as e:
        logging.error(f"Error getting weather: {e}")
        raise


def insert_into_db(conn, query, data):
    try:
        cur = conn.cursor()
        cur.execute(query, data)
        conn.commit()
    except Exception as e:
        logging.error(f"Error inserting the data: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()


def lambda_handler(event, context):
    try:
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
            'body': json.dumps('Successfully ran LambdaRealtimeWeather!')
        }
    except Exception as e:
        logging.error(f"Error in lambda_handler: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Internal Server Error')
        }
