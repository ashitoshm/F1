from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.models.param import Param
from datetime import datetime, timedelta
import json

POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'jolpicaf1_api'

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1)
}

with DAG(
    dag_id='drivers_etl_pipeline',
    default_args=default_args,
    description='Fetch F1 drivers data from jolpica-f1 API and insert into Postgres',
    schedule=None,  
    catchup=False,
    tags=['f1', 'etl'],
    params={
        'season': Param(
            default='2024',
            type='string',
            description='F1 season year to fetch data for'
        )
    }
) as dag:

    @task()
    def fetch_drivers_data(season):
        """Extract drivers data from jolpica API using Airflow connection."""
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        endpoint = f'{season}/drivers.json' 
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}")

    @task()
    def transform_drivers_data(raw_data, season):
        """Transform raw driver data for DB insertion."""
        transformed_drivers = []

        drivers_data = raw_data.get('MRData', {}).get('DriverTable', {}).get('Drivers', [])

        for driver in drivers_data:
            transformed_driver = {
                'driver_id': driver.get('driverId'),
                'code': driver.get('code'),
                'first_name': driver.get('givenName'),
                'last_name': driver.get('familyName'),
                'date_of_birth': driver.get('dateOfBirth'),
                'nationality': driver.get('nationality'),
                'url': driver.get('url'),
                'season': season,  
                'extracted_at': datetime.now().isoformat()
            }
            transformed_drivers.append(transformed_driver)

        return transformed_drivers

    @task()
    def load_drivers_data(transformed_data):
        """Load transformed drivers data into PostgreSQL."""
        if not transformed_data:
            return "No data to load"

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS f1_drivers (
            id SERIAL PRIMARY KEY,
            driver_id VARCHAR(50),
            code VARCHAR(3),
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            date_of_birth DATE,
            nationality VARCHAR(100),
            url TEXT,
            season VARCHAR(4),
            extracted_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(driver_id, season)
        );
        """
        pg_hook.run(create_table_sql)

        upsert_sql = """
        INSERT INTO f1_drivers (driver_id, code, first_name, last_name, date_of_birth, 
                                nationality, url, season, extracted_at)
        VALUES (%(driver_id)s, %(code)s, %(first_name)s, %(last_name)s, %(date_of_birth)s,
                %(nationality)s, %(url)s, %(season)s, %(extracted_at)s)
        ON CONFLICT (driver_id, season) 
        DO UPDATE SET
            code = EXCLUDED.code,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            date_of_birth = EXCLUDED.date_of_birth,
            nationality = EXCLUDED.nationality,
            url = EXCLUDED.url,
            extracted_at = EXCLUDED.extracted_at,
            updated_at = CURRENT_TIMESTAMP;
        """

        for driver in transformed_data:
            pg_hook.run(upsert_sql, parameters=driver)

        return f"Successfully loaded {len(transformed_data)} drivers for season {transformed_data[0]['season']}."

   
    season = "{{ params.season }}"  
    raw_data = fetch_drivers_data(season)
    transformed_data = transform_drivers_data(raw_data, season)
    load_drivers_data(transformed_data)