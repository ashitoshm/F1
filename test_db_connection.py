import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def test_db_connection():
    try:
        # Connect to your PostgreSQL database
        conn = psycopg2.connect(
            host=os.getenv('DATABASE_HOST'),
            port=os.getenv('DATABASE_PORT'),
            dbname=os.getenv('DATABASE_NAME'),
            user=os.getenv('DATABASE_USER'),
            password=os.getenv('DATABASE_PASSWORD')
        )

        # If the connection is successful, print a success message
        print("Database connection successful!")

        # Close the connection
        conn.close()

    except Exception as e:
        # If there is an error, print the error message
        print(f"Error: Unable to connect to the database. {str(e)}")

# Call the function to test the connection
if __name__ == '__main__':
    test_db_connection()
