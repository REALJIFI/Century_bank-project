from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id

def create_dataframe_tables(df: DataFrame) -> dict:
    """
    Creates several DataFrame tables (Transaction, Customer, Employee, Fact)
    based on the cleaned Century Bank data.

    Args:
        df (DataFrame): The cleaned Spark DataFrame (Century_bank_clean).

    Returns:
        dict: A dictionary containing the created DataFrames
              with keys 'Transaction', 'Customer', 'Employee', and 'Fact_table'.
    """

    # Transaction table
    Transaction = df.select('Transaction_Date', 'Amount', 'Transaction_Type')
    Transaction = Transaction.withColumn('Transaction_ID', monotonically_increasing_id())
    Transaction = Transaction.select('Transaction_ID', 'Transaction_Date', 'Amount', 'Transaction_Type')

    # Customer table
    Customer = df.select('Customer_Name', 'Customer_Address', 'Customer_City',
                         'Customer_State', 'Customer_Country', 'Email', 'Phone_Number').distinct()
    Customer = Customer.withColumn('Customer_ID', monotonically_increasing_id())
    Customer = Customer.select('Customer_ID', 'Customer_Name', 'Customer_Address', 'Customer_City',
                         'Customer_State', 'Customer_Country', 'Email', 'Phone_Number')

    # Employee table
    Employee = df.select('Company', 'Job_Title', 'Gender', 'Marital_Status').distinct()
    Employee = Employee.withColumn('Employee_ID', monotonically_increasing_id())
    Employee = Employee.select('Employee_ID', 'Company', 'Job_Title', 'Gender', 'Marital_Status')

    # Fact table
    Fact_table = df.join(Customer, ['Customer_Name', 'Customer_Address', 'Customer_City',
                                     'Customer_State', 'Customer_Country', 'Email', 'Phone_Number'], 'left') \
                   .join(Transaction, ['Transaction_Date', 'Amount', 'Transaction_Type'], 'left') \
                   .join(Employee, ['Company', 'Job_Title', 'Gender', 'Marital_Status'], 'left') \
                   .select('Transaction_ID', 'Customer_ID', 'Employee_ID', 'Credit_Card_Number', 'IBAN',
                           'Currency_Code', 'Random_Number', 'Category', 'Group', 'Is_Active', 'Last_Updated', 'Description')

    return {
        'Transaction': Transaction,
        'Customer': Customer,
        'Employee': Employee,
        'Fact_table': Fact_table
    }
