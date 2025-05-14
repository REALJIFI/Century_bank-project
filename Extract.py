from pyspark.sql import SparkSession

def extract_csv_to_dataframe(file_path, header_option=True):
    """
    Reads a CSV file into a Spark DataFrame.

    Args:
        file_path (str): The path to the CSV file.
        header_option (bool): Indicates if the CSV file has a header row (default is True).

    Returns:
        pyspark.sql.dataframe.DataFrame: The resulting Spark DataFrame.
    """
    spark = SparkSession.builder.appName("CSVExtraction").getOrCreate()
    df = spark.read.csv(file_path, header=header_option)
    return df

# Example usage:
file_path = r'Raw_data\Century_bank_transactions.csv'
Century_bank_df = extract_csv_to_dataframe(file_path)



# If you are done with the SparkSession, you can stop it
# spark.stop()