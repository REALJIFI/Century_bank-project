from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date, round

def transform_dataframe(df: DataFrame) -> DataFrame:
    """
    Performs data cleaning and transformation on a Spark DataFrame.

    Args:
        df (DataFrame): The input Spark DataFrame.

    Returns:
        DataFrame: The cleaned and transformed Spark DataFrame.
    """
    print("Checking for null values:")
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        print(f"{column} nulls: {null_count}")

    # Copy dataframe to avoid altering the original dataset
    cleaned_df = df

    # Fill up missing or null values with necessary default values
    fill_values = {
        'Customer_Name': 'unknown',
        'Customer_Address': 'unknown',
        'Customer_City': 'unknown',
        'Customer_State': 'unknown',
        'Customer_Country': 'unknown',
        'Company': 'unknown',
        'Job_Title': 'unknown',
        'Email': 'unknown',
        'Phone_Number': 'unknown',
        'Credit_Card_Number': 0,
        'IBAN': 'unknown',
        'Currency_Code': 'unknown',
        'Random_Number': 0.0,
        'Category': 'unknown',
        'Group': 'unknown',
        'Is_Active': 'unknown',
        'Description': 'unknown',
        'Gender': 'unknown',
        'Marital_Status': 'unknown'
    }
    cleaned_df = cleaned_df.fillna(fill_values)

    # Drop rows where Last_Updated is null
    cleaned_df = cleaned_df.na.drop(subset=['Last_Updated'])
    
    # Standardize the date and amount columns
    Century_bank_clean = Century_bank_clean \
    .withColumn("Transaction_Date", to_date("Transaction_Date")) \
    .withColumn("Amount", round(col("Amount"), 2)) \
    .withColumn("Transaction_Type", col("Transaction_Type").cast("string"))

    print("\nData Quality Note:")
    print("--- Last_Updated is likely a timestamp or date column showing when a record was last modified or updated.")
    print("--- If that info is missing, the record might be incomplete, outdated, or unreliable, and could distort your analysis.")

    return cleaned_df
