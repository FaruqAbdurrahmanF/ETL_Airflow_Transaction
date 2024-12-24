# ETL Data Pipeline with Apache Airflow for Online E-Commerce

## Project Overview
This project demonstrates the implementation of an ETL (Extract, Transform, Load) pipeline using Apache Airflow. The pipeline processes e-commerce data from Kaggle and loads it into MySQL and PostgreSQL databases for further analysis. The project highlights the automation and orchestration of data workflows to streamline data processing.

## Features
- Automated data ingestion from Kaggle using `Kaggle API`.
- Data extraction from CSV files.
- Data transformation, including cleaning and reformatting.
- Loading data into MySQL for staging and PostgreSQL for processed data.
- Orchestration of tasks using Apache Airflow.

## Technologies Used
- **Apache Airflow**: Tool for managing, scheduling, and monitoring the ETL workflow.
- **Kaggle API**: Download datasets from Kaggle.
- **MySQL**: Store data in the staging area.
- **PostgreSQL**: Store transformed data.
- **Pandas**: Data manipulation and transformation.
- **Python**: Programming for writing ETL scripts.
- **VSCode**: Code editor for writing and managing scripts.
- **DBeaver**: Tool for managing and visualizing databases.

## Workflow Overview
The ETL process consists of the following steps:
1. **Download Data**: Use  Kaggle API to fetch the dataset from Kaggle.
2. **Extract Data**: Read CSV files using Pandas.
3. **Load Data to MySQL**: Filter new data and store it in MySQL.
4. **Transform Data**: Remove duplicates, change data types, and handle missing values.
5. **Load Data to PostgreSQL**: Store the final processed data in PostgreSQL.

### Workflow Diagram
```
Download Data → Extract Data → Load to MySQL → Transform Data → Load to PostgreSQL
```

## File Structure
```
├── dags
│   ├── kaggle_etl_dag.py         # Main DAG file for Airflow
├── data
│   ├── Online-eCommerce.csv      # Sample dataset
├── README.md                     # Project documentation
```

## Getting Started

### Prerequisites
- Apache Airflow
- MySQL and PostgreSQL installed and configured
- Kaggle API token

### Running the Pipeline
- Access the Airflow UI at `http://localhost:8080`.
- Trigger the DAG named `kaggle_etl_dag`.

## Results
- Raw data is stored in MySQL for staging.
- Transformed data is loaded into PostgreSQL for final use.
- Logs and task statuses can be monitored via the Airflow UI.

## Future Work
- Extend the pipeline to include real-time data processing.
- Integrate machine learning models for predictive analysis.
- Create dashboards for visualizing the processed data.


## Acknowledgments
- Kaggle for providing the e-commerce dataset.
- The open-source community for contributing to the tools used in this project.

##  Kaggle URL
https://www.kaggle.com/datasets/ayushparwal2026/online-ecommerce
