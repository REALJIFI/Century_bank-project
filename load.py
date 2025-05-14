import os
import psycopg2
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession

# Load environment variables from a .env file
load_dotenv()

# ----------------- PostgreSQL Connection ----------------- #
def get_db_connection():
    """
    Establishes and returns a PostgreSQL database connection.
    """
    try:
        connection = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT", 5432)  # Default port fallback
        )
        return connection
    except Exception as e:
        raise ConnectionError(f"Database connection failed: {e}")

# ----------------- Create Schema ----------------- #
def create_database_schema():
    """
    Creates the necessary schema and tables in the PostgreSQL database.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        create_table_query = '''
            CREATE SCHEMA IF NOT EXISTS loan_project;

            DROP TABLE IF EXISTS loan_project.Fact_table;
            DROP TABLE IF EXISTS loan_project.Transaction_Table;
            DROP TABLE IF EXISTS loan_project.Customer_Table;
            DROP TABLE IF EXISTS loan_project.Employee_Table;

            CREATE TABLE loan_project.Transaction_Table (
                Transaction_ID BIGINT PRIMARY KEY,
                Transaction_Date DATE,
                Amount NUMERIC(12, 2),
                Transaction_Type VARCHAR(50)
            );

            CREATE TABLE loan_project.Customer_Table (
                Customer_ID BIGINT PRIMARY KEY,
                Customer_Name VARCHAR(100),
                Customer_Address TEXT,
                Customer_City VARCHAR(100),
                Customer_State VARCHAR(100),
                Customer_Country VARCHAR(100),
                Email VARCHAR(255),
                Phone_Number VARCHAR(50)
            );

            CREATE TABLE loan_project.Employee_Table (
                Employee_ID BIGINT PRIMARY KEY,
                Company VARCHAR(150),
                Job_Title VARCHAR(150),
                Gender VARCHAR(10),
                Marital_Status VARCHAR(20)
            );

            CREATE TABLE loan_project.Fact_table (
                Transaction_ID BIGINT,
                Customer_ID BIGINT,
                Employee_ID BIGINT,
                Credit_Card_Number VARCHAR(50),
                IBAN VARCHAR(50),
                Currency_Code VARCHAR(50),
                Random_Number INTEGER,
                Category VARCHAR(100),
                "Group" VARCHAR(100),
                Is_Active BOOLEAN,
                Last_Updated TIMESTAMP,
                Description TEXT,
                PRIMARY KEY (Transaction_ID),
                FOREIGN KEY (Transaction_ID) REFERENCES loan_project.Transaction_Table(Transaction_ID),
                FOREIGN KEY (Customer_ID) REFERENCES loan_project.Customer_Table(Customer_ID),
                FOREIGN KEY (Employee_ID) REFERENCES loan_project.Employee_Table(Employee_ID)
            );
        '''
        cursor.execute(create_table_query)
        conn.commit()
        print(" Database schema and tables created successfully.")
    except Exception as e:
        print(f" Error creating database schema: {e}")
    finally:
        cursor.close()
        conn.close()

# ----------------- Load DataFrame to PostgreSQL ----------------- #
def load_dataframe_to_postgres(df: DataFrame, table_name: str, spark_session: SparkSession):
    """
    Loads a Spark DataFrame into a PostgreSQL table.

    Args:
        df (DataFrame): The Spark DataFrame to load.
        table_name (str): The full PostgreSQL table name (schema.table).
        spark_session (SparkSession): The active SparkSession.
    """
    url = f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', 5432)}/{os.getenv('DB_NAME')}"
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }

    try:
        df.write.jdbc(url=url, table=table_name, mode="append", properties=properties)
        print(f" DataFrame '{table_name}' loaded successfully to PostgreSQL.")
    except Exception as e:
        print(f" Error loading DataFrame '{table_name}' to PostgreSQL: {e}")

# ----------------- Main Execution Block ----------------- #
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("LoadToPostgreSQL") \
        .config("spark.jars", r"postgresql-42.7.4.jar") \
        .getOrCreate()

    # Create schema and tables
    create_database_schema()

    # Check and load DataFrames
    for df_name, table_name in {
        'Transaction': 'loan_project.Transaction_Table',
        'Customer': 'loan_project.Customer_Table',
        'Employee': 'loan_project.Employee_Table',
        'Fact_table': 'loan_project.Fact_table'
    }.items():
        df = globals().get(df_name)
        if isinstance(df, DataFrame):
            load_dataframe_to_postgres(df, table_name, spark)
        else:
            print(f"  Error: DataFrame '{df_name}' not found.")

    spark.stop()
