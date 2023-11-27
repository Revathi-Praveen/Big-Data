from airflow import models
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


CLUSTER_NAME = 'dataproc-airflow-cluster'
REGION='us-west1' # region
PROJECT_ID='applied-groove-405905' #project name
PYSPARK_URI='gs://us-west1-airflow-project-19f5b649-bucket/Code/main_emr_gcp_git_1.py' # spark job location in cloud storage

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    }
}


PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

with models.DAG(
    "example_dataproc_airflow_gcp_to_gbq",
    schedule_interval=None,  
    start_date=days_ago(1),
    catchup=False,
    tags=["dataproc_airflow"],
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        job=PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    gsc_to_gbq_data = GCSToBigQueryOperator(
        task_id="transfer_data_to_bigquery_data",
        bucket="us-west1-airflow-project-19f5b649-bucket",
        source_objects=["output_files/data/*.parquet"],
        destination_project_dataset_table="Airflow_Test.brand_data",
        source_format="PARQUET",
        write_disposition='WRITE_TRUNCATE'
    )

    gsc_to_gbq_product_price = GCSToBigQueryOperator(
        task_id="transfer_data_to_bigquery_product_price",
        bucket="us-west1-airflow-project-19f5b649-bucket",
        source_objects=["output_files/product_price/*.parquet"],
        destination_project_dataset_table="Airflow_Test.product_price",  # BigQuery table for 'product_price'
        source_format="PARQUET",
        write_disposition = 'WRITE_TRUNCATE'
    )


    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", 
        project_id=PROJECT_ID, 
        cluster_name=CLUSTER_NAME, 
        region=REGION
    )

    # Update the task dependencies
    create_cluster >> submit_job >> [delete_cluster, gsc_to_gbq_data, gsc_to_gbq_product_price]
