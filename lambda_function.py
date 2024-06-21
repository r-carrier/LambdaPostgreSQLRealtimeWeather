import psycopg2
from psycopg2 import sql
import json
import urllib3
from botocore.config import Config
from botocore.exceptions import ClientError
import boto3

secret_name = ''
fields = {'apikey': '',
          'location': '',
          'units': 'imperial'}
insert_query = '''
    insert into fw1.realtime_weather
    (request_date, location_name, temperature, cloud_cover)
    VALUES (%s, %s, %s, %s);
    '''

http = urllib3.PoolManager()


def get_secret():
    try:
        print("trying to get secrets")
        secret_client = boto3.client('secretsmanager')
        secret_response = secret_client.get_secret_value(
            SecretId=secret_name)
        secret_dict = json.loads(secret_response['SecretString'])
    except Exception:
        print("There was an error loading the secret from AWS Secrets Manager:\n" + Exception)
    return secret_dict


def connect_to_db(secret_dict):
    conn = psycopg2.connect(
        host=secret_dict['host'],
        port=secret_dict['port'],
        dbname=secret_dict['dbname'],
        user=secret_dict['db_username'],
        password=secret_dict['db_password']
    )

    return conn


def get_weather():
    response = http.request('GET',
                            'https://api.tomorrow.io/v4/weather/realtime',
                            fields=fields)
    data = json.loads(response.data)
    return data


def insert_into_db(conn, query, data):
    cur = conn.cursor()
    cur.execute(query, data)
    conn.commit()
    print("executed insert statement...")


def lambda_handler(event, context):
    weather_data = get_weather()
    request_time = weather_data['data']['time']
    location_name = weather_data['location']['name']
    temperature = weather_data['data']['values']['temperature']
    cloud_cover = weather_data['data']['values']['cloudCover']
    print(cloud_cover)

    insert_query_data = (request_time, location_name, temperature, cloud_cover)
    secret_dict = get_secret()
    db_connection = connect_to_db(secret_dict)
    insert_into_db(db_connection, insert_query, insert_query_data)
    db_connection.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
