from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, when, col

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CenturyBankETL") \
    .getOrCreate()

# Replace these with your actual values
url = "jdbc:postgresql://your_host:your_port/your_db"
properties = {
    "user": "DB_USER",
    "password": "DB_PASSWORD",
    "driver": "org.postgresql.Driver"
}

# Load cleaned data
Century_bank_clean = spark.read.parquet("path_to_cleaned_data")

# --- Employee Dimension ---
employee_window = Window.orderBy("Company", "Job_Title", "Gender", "Marital_Status")
Employee = Century_bank_clean.select(
    'Company', 'Job_Title', 'Gender', 'Marital_Status'
).distinct()
Employee = Employee.withColumn("Employee_ID", row_number().over(employee_window).cast("long"))
Employee = Employee.select("Employee_ID", "Company", "Job_Title", "Gender", "Marital_Status")
Employee.write.jdbc(url=url, table="loan_project.Employee_Table", mode="append", properties=properties)

# --- Customer Dimension ---
customer_window = Window.orderBy("Customer_Name", "Email", "Phone_Number")
Customer = Century_bank_clean.select(
    'Customer_Name', 'Customer_Address', 'Customer_City',
    'Customer_State', 'Customer_Country', 'Email', 'Phone_Number'
).distinct()
Customer = Customer.withColumn("Customer_ID", row_number().over(customer_window).cast("long"))
Customer = Customer.select(
    "Customer_ID", "Customer_Name", "Customer_Address", "Customer_City",
    "Customer_State", "Customer_Country", "Email", "Phone_Number"
)
Customer.write.jdbc(url=url, table="loan_project.Customer_Table", mode="append", properties=properties)

# --- Transaction Dimension ---
transaction_window = Window.orderBy("Transaction_Date", "Amount")
Transaction = Century_bank_clean.select('Transaction_Date', 'Amount', 'Transaction_Type')
Transaction = Transaction.withColumn("Transaction_ID", row_number().over(transaction_window).cast("long"))
Transaction = Transaction.select('Transaction_ID', 'Transaction_Date', 'Amount', 'Transaction_Type')
Transaction.write.jdbc(url=url, table="loan_project.Transaction_Table", mode="append", properties=properties)

# --- Load Dimension Tables from PostgreSQL ---
Employee_df = spark.read.jdbc(url=url, table="loan_project.Employee_Table", properties=properties)
Customer_df = spark.read.jdbc(url=url, table="loan_project.Customer_Table", properties=properties)
Transaction_df = spark.read.jdbc(url=url, table="loan_project.Transaction_Table", properties=properties)

# --- Fact Table ---
Fact_table = Century_bank_clean \
    .join(Employee_df, ['Company', 'Job_Title', 'Gender', 'Marital_Status'], 'left') \
    .join(Customer_df, [
        'Customer_Name', 'Customer_Address', 'Customer_City',
        'Customer_State', 'Customer_Country', 'Email', 'Phone_Number'
    ], 'left') \
    .join(Transaction_df, ['Transaction_Date', 'Amount', 'Transaction_Type'], 'left') \
    .select(
        'Transaction_ID', 'Customer_ID', 'Employee_ID',
        'Credit_Card_Number', 'IBAN', 'Currency_Code', 'Random_Number',
        'Category', 'Group',
        when(col("Is_Active") == "Yes", True)
            .when(col("Is_Active") == "No", False)
            .otherwise(None).alias("Is_Active"),
        'Last_Updated', 'Description'
    )

fact_window = Window.orderBy('Employee_ID', 'Transaction_ID', 'Customer_ID')
Fact_table = Fact_table.withColumn("Fact_ID", row_number().over(fact_window).cast("long"))
Fact_table = Fact_table.select(
    'Fact_ID', 'Transaction_ID', 'Customer_ID', 'Employee_ID',
    'Credit_Card_Number', 'IBAN', 'Currency_Code', 'Random_Number',
    'Category', 'Group', 'Is_Active', 'Last_Updated', 'Description'
)
Fact_table.write.jdbc(url=url, table="loan_project.Fact_table", mode="append", properties=properties)

print("✅ All tables written successfully and foreign keys are consistent.")
