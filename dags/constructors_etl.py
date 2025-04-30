from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.models.param import Param
from datetime import datetime, timedelta

POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'jolpicaf1_api'

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1)
}

with DAG(
    dag_id='constructors_etl_pipeline',
    default_args=default_args,
    description='Fetch F1 constructors data from jolpica-f1 API and insert into Postgres',
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
    def fetch_constructors_data(season):
        """Extract constructors data from jolpica API using Airflow connection."""
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        endpoint = f'{season}/constructors.json'
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}")

    @task()
    def transform_constructors_data(raw_data, season):
        """Transform raw constructors data for DB insertion."""
        transformed_constructors = []

        constructors_data = raw_data.get('MRData', {}).get('ConstructorTable', {}).get('Constructors', [])

        for constructor in constructors_data:
            transformed_constructor = {
                'constructor_id': constructor.get('constructorId'),
                'constructor_name': constructor.get('name'),
                'constructor_nationality': constructor.get('nationality'),
                'url': constructor.get('url'),
                'season': season,  
                'extracted_at': datetime.now().isoformat()
            }
            transformed_constructors.append(transformed_constructor)

        return transformed_constructors

    @task()
    def load_constructors_data(transformed_data):
        """Load transformed constructors data into PostgreSQL."""
        if not transformed_data:
            return "No data to load"

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS f1_constructors (
            id SERIAL PRIMARY KEY,
            constructor_id VARCHAR(50),
            constructor_name VARCHAR(100),
            constructor_nationality VARCHAR(100),
            url TEXT,
            season VARCHAR(4),
            extracted_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(constructor_id, season)
        );
        """
        pg_hook.run(create_table_sql)

        upsert_sql = """
        INSERT INTO f1_constructors (constructor_id, constructor_name, constructor_nationality, url, season, extracted_at)
        VALUES (%(constructor_id)s, %(constructor_name)s, %(constructor_nationality)s, %(url)s, %(season)s, %(extracted_at)s)
        ON CONFLICT (constructor_id, season)
        DO UPDATE SET
            constructor_name = EXCLUDED.constructor_name,
            constructor_nationality = EXCLUDED.constructor_nationality,
            url = EXCLUDED.url,
            extracted_at = EXCLUDED.extracted_at,
            updated_at = CURRENT_TIMESTAMP;
        """

        for constructor in transformed_data:
            pg_hook.run(upsert_sql, parameters=constructor)

        return f"Successfully loaded {len(transformed_data)} constructors for season {transformed_data[0]['season']}."

   
    season = "{{ params.season }}"  
    raw_data = fetch_constructors_data(season)
    transformed_data = transform_constructors_data(raw_data, season)
    load_constructors_data(transformed_data)