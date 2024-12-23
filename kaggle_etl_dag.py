import pandas as pd
from sqlalchemy import text
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook

# Function to download data from Kaggle
def download_kaggle_data(**kwargs):
    """
    Download dataset from Kaggle using the Kaggle API and return the file path.
    """
    os.environ['KAGGLE_CONFIG_DIR'] = "/opt/airflow/.kaggle"

    dataset_name = "ayushparwal2026/online-ecommerce"
    download_path = "/opt/airflow/data/online-ecommerce"
    os.makedirs(download_path, exist_ok=True)
    os.system(f"kaggle datasets download -d {dataset_name} -p {download_path} --unzip")

    file_path = os.path.join(download_path, "Online-eCommerce.csv")
    kwargs['ti'].xcom_push(key='file_path', value=file_path)
    print("Data downloaded to:", file_path)

# Function to extract data from the downloaded CSV file
def extract_data(**kwargs):
    """
    Load CSV data into a pandas DataFrame.
    """
    file_path = kwargs['ti'].xcom_pull(task_ids='download_kaggle_data', key='file_path')
    print(f"Extracting data from file: {file_path}")
    data = pd.read_csv(file_path)
    print("Data extracted successfully")
    kwargs['ti'].xcom_push(key='extracted_data', value=data.to_dict('records'))

# Function to load the extracted data into MySQL (staging area)
def load_data_to_mysql(**kwargs):
    """
    Load new data into the MySQL staging area after checking for duplicates.
    """
    extracted_data = pd.DataFrame(kwargs['ti'].xcom_pull(task_ids='extract_data', key='extracted_data'))

    if 'Order_Number' not in extracted_data.columns:
        raise ValueError("Column 'Order_Number' not found in the provided data.")

    # Use MySqlHook for MySQL connection
    mysql_hook = MySqlHook(mysql_conn_id='mysql_dibimbing')
    engine = mysql_hook.get_sqlalchemy_engine()

    with engine.connect() as connection:
        query = "SELECT Order_Number FROM kaggle_data_staging"
        existing_data = pd.read_sql(query, connection)
        new_data = extracted_data[~extracted_data['Order_Number'].isin(existing_data['Order_Number'])]

    if not new_data.empty:
        new_data.to_sql('kaggle_data_staging', con=engine, if_exists='append', index=False)
        print(f"{len(new_data)} new rows added to MySQL staging area.")
    else:
        print("No new data to add to MySQL.")
    
    kwargs['ti'].xcom_push(key='staged_data', value=new_data.to_dict('records'))

# Data transformation function
def transform_data(**kwargs):
    """
    Perform all necessary data transformations including formatting, renaming columns,
    handling missing values, and ensuring data consistency.
    """
    staged_data = pd.DataFrame(kwargs['ti'].xcom_pull(task_ids='load_data_to_mysql', key='staged_data'))

    # Check if 'Order_Date' exists in the data
    if 'Order_Date' not in staged_data.columns:
        print("Warning: 'Order_Date' column is missing!")
        return  # Skip the transformation if the column is missing
    
    # Remove duplicates
    staged_data.drop_duplicates(inplace=True)

    # Format Order_Date to YYYY-MM-DD
    staged_data['Order_Date'] = pd.to_datetime(staged_data['Order_Date'], format='%d/%m/%Y', errors='coerce').dt.strftime('%Y-%m-%d')

    # Rename columns for PostgreSQL compatibility
    staged_data.rename(columns={"Assigned Supervisor": "Assigned_Supervisor"}, inplace=True)

    # Drop rows with missing critical fields
    staged_data.dropna(subset=['Cost', 'Sales'], inplace=True)

    # Convert Cost and Sales to numeric values
    staged_data['Cost'] = pd.to_numeric(staged_data['Cost'], errors='coerce')
    staged_data['Sales'] = pd.to_numeric(staged_data['Sales'], errors='coerce')

    # Ensure Order_Number is treated as string
    staged_data['Order_Number'] = staged_data['Order_Number'].astype(int).astype(str)

    # Handle missing values in non-critical columns
    staged_data.fillna({
        'State_Code': 'Unknown',
        'Customer_Name': 'Unknown',
        'Status': 'Unknown',
        'Product': 'Unknown',
        'Category': 'Unknown',
        'Brand': 'Unknown',
        'Quantity': 0.0,
        'Total_Cost': 0.0,
        'Total_Sales': 0.0,
        'Assigned_Supervisor': 'Unknown'
    }, inplace=True)

    print("Data transformed successfully")
    kwargs['ti'].xcom_push(key='transformed_data', value=staged_data.to_dict('records'))

# Function to load transformed data into PostgreSQL
def load_transformed_data_to_postgresql(**kwargs):
    """
    Load transformed data from pandas DataFrame into PostgreSQL.
    """
    # Mendapatkan data yang ditransformasi menggunakan XCom
    transformed_data = pd.DataFrame(kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_data'))

    # Menggunakan PostgresHook untuk koneksi ke PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id="postgres_dibimbing")
    postgres_engine = postgres_hook.get_sqlalchemy_engine()

    upsert_sql = """
    INSERT INTO portofolio.online_order (
        Order_Number, State_Code, Customer_Name, Order_Date, Status, Product, 
        Category, Brand, Cost, Sales, Quantity, Total_Cost, Total_Sales, Assigned_Supervisor
    )
    VALUES (
        :Order_Number, :State_Code, :Customer_Name, :Order_Date, :Status, :Product, 
        :Category, :Brand, :Cost, :Sales, :Quantity, :Total_Cost, :Total_Sales, :Assigned_Supervisor
    )
    ON CONFLICT (Order_Number) DO NOTHING;
    """

    # Membuka koneksi dan melakukan eksekusi
    with postgres_engine.connect() as connection:
        # Iterasi melalui setiap baris data dan eksekusi upsert query
        for _, row in transformed_data.iterrows():
            connection.execute(text(upsert_sql), row.to_dict())

    print("Transformed data successfully loaded into PostgreSQL.")

# Define the DAG
with DAG(
    'kaggle_etl_dag',
    default_args={
        'owner': 'burqi',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='ETL process for Kaggle dataset',
    schedule_interval=None,
    start_date=datetime(2024, 12, 17),
    catchup=False,
) as dag:

    # Task 1: Download Data from Kaggle
    download_task = PythonOperator(
        task_id='download_kaggle_data',
        python_callable=download_kaggle_data,
    )

    # Task 2: Extract Data from CSV
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
    )

    # Task 3: Load Data to MySQL
    load_mysql_task = PythonOperator(
        task_id='load_data_to_mysql',
        python_callable=load_data_to_mysql,
        provide_context=True,
    )

    # Task 4: Transform Data
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    # Task 5: Load Transformed Data to PostgreSQL
    load_postgresql_task = PythonOperator(
        task_id='load_transformed_data_to_postgresql',
        python_callable=load_transformed_data_to_postgresql,
        provide_context=True,
    )

    # Set task dependencies
    download_task >> extract_task >> load_mysql_task >> transform_task >> load_postgresql_task
