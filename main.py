from pyspark.sql import SparkSession
import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


## set jave home to avoid java running with the previous version
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk1.8.0_202'

#ENABLING FIREWALLS BLOCKING
#import os
os.environ["PYSPARK_ALLOW_INSECURE_GATEWAY"] = "1"

# initialize my spark seesion with allowed security
from pyspark.sql import SparkSession # type: ignore

spark = SparkSession.builder \
    .appName("GWIHR_PROJECT") \
    .config("spark.jars", r"postgresql-42.7.4.jar") \
    .getOrCreate()
spark

from Extract import extract_csv_to_dataframe
from transform import transform_dataframe
from create_dataframe_table import create_dataframe_tables
from load import get_db_connection, create_database_schema,load_dataframe_to_postgres

def main():
    logger.info("Starting ETL pipeline...")

    try:
        # Extract: Load CSV data
        logger.info("Extracting data from CSV...")
        Century_bank_df = extract_csv_to_dataframe(r'Raw_data/Century_bank_transactions.csv')
        logger.info("Data loaded successfully.")

        # Transform
        logger.info("Transforming data...")
        cleaned_df = transform_dataframe(Century_bank_df)
        logger.info("Data transformed successfully.")

        # Create dimension and fact tables
        logger.info("Creating dimension and fact tables...")
        tables = create_dataframe_tables(cleaned_df)
        Transaction = tables["Transaction"]
        Customer = tables["Customer"]
        Employee = tables["Employee"]
        Fact_table = tables["Fact_table"]
        logger.info("Dimension and fact tables created.")

        # Connect to PostgreSQL
        logger.info("Connecting to PostgreSQL database...")
        conn = get_db_connection()
        logger.info("Connected to PostgreSQL database.")
        conn.close()  # Close right away since Spark handles JDBC loading

        # Create schema and tables
        logger.info("Creating schema and tables in PostgreSQL...")
        create_database_schema()
        logger.info("Database schema and tables created successfully.")

        # Load to PostgreSQL
        logger.info("Loading data to PostgreSQL...")
        load_dataframe_to_postgres(Transaction, "loan_project.Transaction_Table", spark)
        load_dataframe_to_postgres(Customer, "loan_project.Customer_Table", spark)
        load_dataframe_to_postgres(Employee, "loan_project.Employee_Table", spark)
        load_dataframe_to_postgres(Fact_table, "loan_project.Fact_table", spark)
        logger.info("All data loaded to PostgreSQL successfully.")

        logger.info(" ETL pipeline completed successfully.")

    except Exception as e:
        logger.error(f" ETL pipeline failed: {e}", exc_info=True)
    finally:
        logger.info("Stopping SparkSession...")
        spark.stop()
        logger.info("SparkSession stopped.")

if __name__ == "__main__":
    main()