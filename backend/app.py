from flask import Flask, request, jsonify, render_template
import requests
from datetime import datetime
import psycopg2
import os
import logging
import traceback
import sys
from dotenv import load_dotenv

load_dotenv()

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder='templates')

# Configuration using environment variables for Docker compatibility
AIRFLOW_API_URL = os.environ.get("AIRFLOW_API_URL", "http://localhost:8080/api/v2")
AIRFLOW_USERNAME = os.environ.get("AIRFLOW_USERNAME", "airflow")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_PASSWORD", "airflow")

DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "port": int(os.environ["DB_PORT"]),
    "database": os.environ["DB_NAME"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "connect_timeout": 5
}
   

# Log environment variables (without sensitive data)
logger.info(f"Starting with DB_HOST: {DB_CONFIG['host']}, DB_NAME: {DB_CONFIG['database']}, DB_USER: {DB_CONFIG['user']}")
logger.info(f"AIRFLOW_API_URL: {AIRFLOW_API_URL}")

# Create a database connection with better error handling
def get_db_connection():
    try:
        logger.debug(f"Attempting database connection to {DB_CONFIG['host']} with user {DB_CONFIG['user']}")
        conn = psycopg2.connect(**DB_CONFIG)
        logger.debug("Database connection successful")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Database connection error: {e}")
        # Print full traceback for debugging
        logger.error(traceback.format_exc())
        raise Exception(f"Could not connect to database: {e}")

@app.route('/')
def index():
    logger.debug("Serving index page")
    return render_template('index.html')

# Simplified diagnostic endpoint to test DB connection
@app.route('/diagnose-db', methods=['GET'])
def diagnose_db():
    """Diagnostic endpoint to test database connection and tables"""
    logger.info("Running database diagnostics")
    
    results = {
        "timestamp": datetime.now().isoformat(),
        "db_config": {
            "host": DB_CONFIG["host"],
            "database": DB_CONFIG["database"],
            "user": DB_CONFIG["user"],
            "connect_timeout": DB_CONFIG["connect_timeout"]
        },
        "connection_test": None,
        "tables": {}
    }
    
    try:
        # Test connection
        logger.debug("Testing database connection")
        conn = psycopg2.connect(**DB_CONFIG)
        results["connection_test"] = "success"
        
        cursor = conn.cursor()
        
        # Check PostgreSQL version
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        results["postgres_version"] = version[0] if version else "Unknown"
        
        # Check for tables
        tables_to_check = ['f1_constructors', 'f1_drivers']
        for table in tables_to_check:
            logger.debug(f"Checking if table '{table}' exists")
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = '{table}'
                );
            """)
            exists = cursor.fetchone()[0]
            results["tables"][table] = {"exists": exists}
            
            if exists:
                # Check schema if table exists
                cursor.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = '{table}'
                    ORDER BY ordinal_position;
                """)
                columns = cursor.fetchall()
                results["tables"][table]["columns"] = [
                    {"name": col[0], "type": col[1]} for col in columns
                ]
                
                # Check row count
                cursor.execute(f"SELECT COUNT(*) FROM {table};")
                count = cursor.fetchone()[0]
                results["tables"][table]["row_count"] = count
                
        cursor.close()
        conn.close()
        logger.info("Database diagnostics completed successfully")
        
    except Exception as e:
        error_details = str(e)
        logger.error(f"Database diagnostics failed: {error_details}")
        logger.error(traceback.format_exc())
        results["connection_test"] = "failed"
        results["error"] = error_details
        results["traceback"] = traceback.format_exc()
    
    return jsonify(results)

@app.route('/trigger-f1-data', methods=['POST'])
def trigger_f1_data():
    """Endpoint to trigger Airflow DAG for a specific F1 season"""
    logger.info("Trigger F1 data endpoint called")
    
    # Check if request is JSON
    if not request.is_json:
        logger.warning("Request is not JSON")
        return jsonify({"error": "Request must be JSON"}), 400
    
    data = request.json
    logger.debug(f"Request data: {data}")
    
    season = data.get('season')
    data_type = data.get('type', 'constructors')  # Default to constructors if not specified

    if not season:
        logger.warning("Season parameter missing")
        return jsonify({"error": "Season parameter is required"}), 400

    # Validate season
    try:
        year = int(season)
        if year < 1950 or year > datetime.now().year:
            logger.warning(f"Invalid season: {season}")
            return jsonify({"error": f"Season must be between 1950 and {datetime.now().year}"}), 400
    except ValueError:
        logger.warning(f"Non-numeric season: {season}")
        return jsonify({"error": "Season must be a valid year"}), 400

    # DAG selection logic
    dag_id = "constructors_etl_pipeline" if data_type == "constructors" else "drivers_etl_pipeline"

    try:
        logger.info(f"Triggering Airflow DAG: {dag_id} for season {season}")
        response = requests.post(
            f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns",
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            json={
                "conf": {"season": season},
                "dag_run_id": f"{dag_id}_run_{season}_{datetime.now().isoformat()}"
            },
            headers={"Content-Type": "application/json"}
        )

        logger.debug(f"Airflow API response: {response.status_code}")
        
        if response.status_code == 200:
            logger.info("DAG triggered successfully")
            return jsonify({
                "success": True,
                "message": f"DAG triggered successfully for {data_type} data, season {season}",
                "dag_run_id": response.json().get("dag_run_id")
            })
        else:
            logger.error(f"Airflow API error: {response.status_code} - {response.text}")
            return jsonify({
                "success": False,
                "error": f"Failed to trigger DAG: {response.text}"
            }), response.status_code

    except Exception as e:
        logger.error(f"Error triggering DAG: {e}")
        logger.error(traceback.format_exc())
        return jsonify({"success": False, "error": str(e), "traceback": traceback.format_exc()}), 500

@app.route('/get-f1-constructors/<season>', methods=['GET'])
def get_f1_constructors(season):
    """Retrieve F1 constructors for a specific season from database"""
    logger.info(f"Getting F1 constructors for season: {season}")
    
    try:
        logger.debug("Attempting database connection")
        conn = get_db_connection()
        cursor = conn.cursor()
        logger.debug("Connection successful, checking if table exists")

        # Check if table exists first to prevent errors
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'f1_constructors'
            );
        """)
        
        table_exists = cursor.fetchone()[0]
        if not table_exists:
            logger.warning("Table 'f1_constructors' does not exist")
            cursor.close()
            conn.close()
            return jsonify({
                "error": "Database table 'f1_constructors' does not exist",
                "message": "You need to run the ETL pipeline first"
            }), 404

        logger.debug("Table exists, executing query")
        query = """
        SELECT constructor_id, constructor_name, constructor_nationality, url
        FROM f1_constructors
        WHERE season = %s
        ORDER BY constructor_name
        """

        cursor.execute(query, (season,))
        constructors = []
        logger.debug("Query executed, fetching results")

        for row in cursor.fetchall():
            constructors.append({
                "constructor_id": row[0],
                "name": row[1],
                "nationality": row[2],
                "url": row[3]
            })

        cursor.close()
        conn.close()
        logger.debug(f"Found {len(constructors)} constructors for season {season}")

        if not constructors:
            logger.info(f"No constructor data found for season {season}")
            return jsonify({
                "message": f"No constructor data found for season {season}. You may need to trigger data fetch first.",
                "constructors": []
            })

        logger.info(f"Successfully retrieved {len(constructors)} constructors")
        return jsonify({
            "season": season,
            "constructors": constructors
        })

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error fetching constructors: {error_msg}")
        logger.error(traceback.format_exc())
        
        # More detailed error response
        return jsonify({
            "error": error_msg,
            "traceback": traceback.format_exc(),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/get-f1-drivers/<season>', methods=['GET'])
def get_f1_drivers(season):
    """Retrieve F1 drivers for a specific season from database"""
    logger.info(f"Getting F1 drivers for season: {season}")
    
    try:
        logger.debug("Attempting database connection")
        conn = get_db_connection()
        cursor = conn.cursor()
        logger.debug("Connection successful, checking if table exists")

        # Check if table exists first to prevent errors
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'f1_drivers'
            );
        """)
        
        table_exists = cursor.fetchone()[0]
        if not table_exists:
            logger.warning("Table 'f1_drivers' does not exist")
            cursor.close()
            conn.close()
            return jsonify({
                "error": "Database table 'f1_drivers' does not exist",
                "message": "You need to run the ETL pipeline first"
            }), 404

        logger.debug("Table exists, executing query")
        query = """
        SELECT driver_id, code, first_name, last_name, date_of_birth, nationality, url
        FROM f1_drivers
        WHERE season = %s
        ORDER BY last_name, first_name
        """

        cursor.execute(query, (season,))
        drivers = []
        logger.debug("Query executed, fetching results")

        for row in cursor.fetchall():
            drivers.append({
                "driver_id": row[0],
                "code": row[1],
                "first_name": row[2],
                "last_name": row[3],
                "date_of_birth": row[4].strftime('%Y-%m-%d') if row[4] else None,
                "nationality": row[5],
                "url": row[6]
            })

        cursor.close()
        conn.close()
        logger.debug(f"Found {len(drivers)} drivers for season {season}")

        if not drivers:
            logger.info(f"No driver data found for season {season}")
            return jsonify({
                "message": f"No driver data found for season {season}. You may need to trigger data fetch first.",
                "drivers": []
            })

        logger.info(f"Successfully retrieved {len(drivers)} drivers")
        return jsonify({
            "season": season,
            "drivers": drivers
        })

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error fetching drivers: {error_msg}")
        logger.error(traceback.format_exc())
        
        # More detailed error response
        return jsonify({
            "error": error_msg,
            "traceback": traceback.format_exc(),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/get-available-seasons', methods=['GET'])
def get_available_seasons():
    """Get all seasons available in the database"""
    logger.info("Getting available seasons")
    
    try:
        logger.debug("Attempting database connection")
        conn = get_db_connection()
        cursor = conn.cursor()
        logger.debug("Connection successful, checking if tables exist")

        # Check if tables exist first
        constructor_table_exists = False
        driver_table_exists = False
        
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'f1_constructors'
            );
        """)
        constructor_table_exists = cursor.fetchone()[0]
        logger.debug(f"f1_constructors table exists: {constructor_table_exists}")
        
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'f1_drivers'
            );
        """)
        driver_table_exists = cursor.fetchone()[0]
        logger.debug(f"f1_drivers table exists: {driver_table_exists}")
        
        # Initialize with empty arrays
        constructor_seasons = []
        driver_seasons = []
        
        # Only query if tables exist
        if constructor_table_exists:
            logger.debug("Querying constructor seasons")
            cursor.execute("SELECT DISTINCT season FROM f1_constructors ORDER BY season DESC")
            constructor_seasons = [row[0] for row in cursor.fetchall()]
            logger.debug(f"Found constructor seasons: {constructor_seasons}")
        
        if driver_table_exists:
            logger.debug("Querying driver seasons")
            cursor.execute("SELECT DISTINCT season FROM f1_drivers ORDER BY season DESC")
            driver_seasons = [row[0] for row in cursor.fetchall()]
            logger.debug(f"Found driver seasons: {driver_seasons}")

        cursor.close()
        conn.close()

        all_seasons = sorted(set(constructor_seasons + driver_seasons), reverse=True)
        logger.info(f"Successfully retrieved seasons: {all_seasons}")

        return jsonify({
            "all_seasons": all_seasons,
            "constructor_seasons": constructor_seasons,
            "driver_seasons": driver_seasons
        })

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error fetching available seasons: {error_msg}")
        logger.error(traceback.format_exc())
        
        # More detailed error response
        return jsonify({
            "error": error_msg,
            "traceback": traceback.format_exc(),
            "timestamp": datetime.now().isoformat()
        }), 500

# Add a health check endpoint
@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint"""
    logger.info("Health check requested")
    
    health_data = {
        "status": "checking",
        "timestamp": datetime.now().isoformat(),
        "components": {
            "application": "running",
            "database": "unknown"
        }
    }
    
    try:
        # Test database connection
        logger.debug("Testing database connection for health check")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Basic query to validate connection
        cursor.execute("SELECT 1;")
        cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        health_data["status"] = "healthy"
        health_data["components"]["database"] = "connected"
        logger.info("Health check successful")
        
        return jsonify(health_data)
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Health check failed: {error_msg}")
        logger.error(traceback.format_exc())
        
        health_data["status"] = "unhealthy"
        health_data["components"]["database"] = "disconnected"
        health_data["error"] = error_msg
        health_data["traceback"] = traceback.format_exc()
        
        return jsonify(health_data), 500

if __name__ == '__main__':
    logger.info("Starting F1 Data API")
    app.run(debug=True, host='0.0.0.0')