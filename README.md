# NYC Taxi Data Cleaning Pipeline

This project implements a monthly data cleaning pipeline for New York City Yellow Taxi trip data using Apache Spark, orchestrated with Apache Airflow.

## Dataset

The pipeline processes **NYC Yellow Taxi Trip Records** in Parquet format.  
Example file: `yellow_tripdata_2025-10.parquet`

These datasets are publicly available from the NYC Taxi & Limousine Commission (TLC) and contain detailed trip information such as pickup/dropoff times, locations, distances, fares, passenger count, etc.

Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Transformations Performed

The Spark job (`scripts/clean_taxi.py`) performs the following cleaning steps:

1. Reads the input Parquet file.
2. Converts all column names to lowercase.
3. Filters out invalid records:
   - Removes rows where `tpep_pickup_datetime` is null.
   - Removes rows where `trip_distance <= 0`.
4. Writes the cleaned DataFrame back to Parquet.
5. Computes and saves statistics to `stats.json`:
   ```json
   {
     "records_read": <total rows read>,
     "records_written": <rows after filtering>,
     "status": "success" | "failure"
   }

## Build and up the Docker
```bash
docker compose up -d --build
```


## How to Run

#### 1. Run the Spark Job Directly (Standalone):

```bash
# Access the webserver (or any airflow) container
docker compose exec airflow-webserver bash

# Run the Spark job manually
spark-submit /opt/airflow/scripts/clean_taxi.py \\
  --input /opt/airflow/data/input/yellow_tripdata_2025-10.parquet \\
  --output /opt/airflow/data/output/cleaned \\
  --stats /opt/airflow/data/stats.json
```

#### 2. Run via Airflow DAG:
Access the Airflow UI at: http://localhost:8080
**Login credentials:**
- **Username**: `admin`
- **Password**: `admin`

To trigger the DAG manually or to monitor its execution:

1. **Find the DAG**:
   - In the Airflow UI, navigate to the list of DAGs.
   - Look for the DAG named `taxi_clean_pipeline`.

2. **Toggle the DAG**:
   - Toggle the DAG to "On" by clicking the switch next to its name.

3. **Trigger the DAG**:
   - Click on the "play" (trigger) button to run the DAG manually, or wait for the next scheduled run (monthly schedule).

### What the DAG Does

The `taxi_clean_pipeline` DAG performs the following key steps:

1. **`run_spark_clean`**:
   - Submits a Spark job using the `SparkSubmitOperator` to clean the taxi data. The job processes the raw taxi data and produces cleaned data ready for analysis.

2. **`push_stats_to_xcom`**:
   - Reads the generated `stats.json` file (containing metrics such as the number of records processed, data quality scores, etc.).
   - Pushes key metrics to XCom for downstream tasks to access and use.

### Monitoring and Debugging

You can monitor the execution of each task in the Airflow UI. If any task fails, you can use the UI to view logs and troubleshoot the issue.

### Screenshots


<img width="1074" height="464" alt="image" src="https://github.com/user-attachments/assets/d7d6727f-ccdb-4af7-bc58-d34a1790147e" />


<img width="1517" height="277" alt="image" src="https://github.com/user-attachments/assets/912daf2b-ccae-4ccf-a4a5-1f78a8132a66" />

