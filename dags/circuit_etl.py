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
    dag_id='circuits_etl_pipeline',
    default_args=default_args,
    description='Fetch F1 circuits data from jolpica-f1 API and insert into Postgres',
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
    def fetch_circuits_data(season):
        """Extract circuits data from jolpica API using Airflow connections"""
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        endpoint = f'{season}/circuits.json'
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}")

    @task()
    def transform_circuits_data(raw_data, season):
        """Transform raw circuits data for DB insertion"""
        transformed_circuits = []

        circuits_data = raw_data.get('MRData', {}).get('CircuitTable', {}).get('Circuits', [])

        for circuit in circuits_data:
            location = circuit.get('Location', {})
            transformed_circuit = {
                'circuit_id': circuit.get('circuitId'),
                'circuit_name': circuit.get('circuitName'),
                'location': json.dumps({
                    'lat': location.get('lat'),
                    'long': location.get('long'),
                    'locality': location.get('locality'),
                    'country': location.get('country')
                }),
                'url': circuit.get('url'),
                'season': season,
                'extracted_at': datetime.now().isoformat()
            }
            transformed_circuits.append(transformed_circuit)

        return transformed_circuits

    @task()
    def load_circuits_data(transformed_data):
        """Load transformed circuits data into PostgreSQL."""
        if not transformed_data:
            return "No data to load"

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS f1_circuits (
            id SERIAL PRIMARY KEY,
            circuit_id VARCHAR(50),
            circuit_name VARCHAR(100),
            location JSONB,
            url TEXT,
            season VARCHAR(4),
            extracted_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(circuit_id, season)
        );
        """
        pg_hook.run(create_table_sql)

        upsert_sql = """
        INSERT INTO f1_circuits (circuit_id, circuit_name, location, url, season, extracted_at)
        VALUES (%(circuit_id)s, %(circuit_name)s, %(location)s, %(url)s, %(season)s, %(extracted_at)s)
        ON CONFLICT (circuit_id, season)
        DO UPDATE SET
            circuit_name = EXCLUDED.circuit_name,
            location = EXCLUDED.location,
            url = EXCLUDED.url,
            extracted_at = EXCLUDED.extracted_at,
            updated_at = CURRENT_TIMESTAMP;
        """

        for circuit in transformed_data:
            pg_hook.run(upsert_sql, parameters=circuit)

        return f"Successfully loaded {len(transformed_data)} circuits for season {transformed_data[0]['season']}."

    season = "{{ params.season }}"
    raw_data = fetch_circuits_data(season)
    transformed_data = transform_circuits_data(raw_data, season)
    load_circuits_data(transformed_data)
