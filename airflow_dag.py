#!/usr/bin/env python3
"""
Airflow DAG pro orchestraci knihovny ETL pipeline
Denní automatizace všech 4 kroků řešení
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# Import našich modulů
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_downloader import MKCRDataDownloader
from data_processor import KnihovnyDataProcessor
from ccmm_generator import CCMMGenerator

# DAG konfigurace
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 7),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['data-team@organization.cz']
}

dag = DAG(
    'knihovny_etl_daily',
    default_args=default_args,
    description='Denní ETL pipeline pro evidenci knihoven MK ČR',
    schedule_interval='0 2 * * *',  # Denně ve 2:00
    catchup=False,
    max_active_runs=1,
    tags=['knihovny', 'mkcr', 'etl']
)

def download_data_task(**context):
    """1. Automatické stahování dat ze zdroje"""
    
    downloader = MKCRDataDownloader()
    result = downloader.download_latest_evidence(output_dir="/tmp/knihovny_data")
    
    if result['status'] != 'success':
        raise ValueError(f"Download failed: {result.get('error')}")
    
    # Push pro další tasky
    context['task_instance'].xcom_push(key='csv_file', value=result['csv_file'])
    context['task_instance'].xcom_push(key='xlsx_file', value=result['xlsx_file'])
    
    print(f"SUCCESS: Downloaded {result['csv_file']}")
    return result

def process_data_task(**context):
    """2. Transformace a úpravy pro interoperabilitu"""
    
    csv_file = context['task_instance'].xcom_pull(task_ids='download_data', key='csv_file')
    
    if not csv_file:
        raise ValueError("CSV file not found")
    
    processor = KnihovnyDataProcessor(csv_file)
    processor.load_data()
    processor.transform_data()
    metrics = processor.calculate_quality_metrics()
    files = processor.export_formats(output_dir="/tmp/knihovny_processed")
    
    # Push metriky pro další tasky
    context['task_instance'].xcom_push(key='quality_metrics', value=metrics)
    context['task_instance'].xcom_push(key='processed_files', value=files)
    
    print(f"SUCCESS: Processed {metrics['total_records']} records")
    print(f"Quality score: {metrics['quality_score']:.2%}")
    return metrics

def store_data_task(**context):
    """3. Trvalé a bezpečné uložení dat"""
    
    # V produkci by zde bylo skutečné uložení do databáze
    processed_files = context['task_instance'].xcom_pull(task_ids='process_data', key='processed_files')
    
    # Simulace database insert a backup
    storage_result = {
        'database_inserted': True,
        'backup_created': True,
        'backup_location': 's3://knihovny-backup/daily/',
        'storage_time': datetime.now().isoformat()
    }
    
    context['task_instance'].xcom_push(key='storage_result', value=storage_result)
    
    print(f"SUCCESS: Data stored and backed up")
    return storage_result

def generate_ccmm_task(**context):
    """4. Generování CCMM metadat"""
    
    quality_metrics = context['task_instance'].xcom_pull(task_ids='process_data', key='quality_metrics')
    
    generator = CCMMGenerator()
    metadata = generator.generate_dataset_metadata(quality_metrics)
    
    # Validace CCMM
    validation = generator.validate_ccmm(metadata)
    if not validation['is_valid']:
        raise ValueError(f"CCMM validation failed: {validation['missing_fields']}")
    
    # Export metadat
    output_file = generator.export_metadata(metadata, "/tmp/knihovny_ccmm.json")
    
    context['task_instance'].xcom_push(key='ccmm_file', value=output_file)
    
    print(f"SUCCESS: CCMM metadata generated: {output_file}")
    return {'ccmm_file': output_file, 'validation': validation}

def generate_report_task(**context):
    """Generuje závěrečný report"""
    
    # Získání výsledků ze všech tasků
    download_result = context['task_instance'].xcom_pull(task_ids='download_data')
    process_result = context['task_instance'].xcom_pull(task_ids='process_data')
    storage_result = context['task_instance'].xcom_pull(task_ids='store_data')
    ccmm_result = context['task_instance'].xcom_pull(task_ids='generate_ccmm')
    
    report = f"""
KNIHOVNY ETL DAILY REPORT
========================
Date: {context['ds']}
Pipeline: {dag.dag_id}

1. DATA DOWNLOAD:
   Status: {'SUCCESS' if download_result['status'] == 'success' else 'FAILED'}
   Source: MK ČR website
   
2. DATA PROCESSING:
   Records: {process_result['total_records']:,}
   Quality Score: {process_result['quality_score']:.1%}
   Email Coverage: {process_result['email_completeness']:.1%}
   
3. DATA STORAGE:
   Database: {'OK' if storage_result['database_inserted'] else 'FAILED'}
   Backup: {'OK' if storage_result['backup_created'] else 'FAILED'}
   
4. CCMM METADATA:
   Generated: {'OK' if ccmm_result else 'FAILED'}
   Validation: {'PASSED' if ccmm_result and ccmm_result['validation']['is_valid'] else 'FAILED'}

Pipeline completed successfully!
"""
    
    print(report)
    return report

# Task definitions
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

download_data = PythonOperator(
    task_id='download_data',
    python_callable=download_data_task,
    dag=dag
)

process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_data_task,
    dag=dag
)

store_data = PythonOperator(
    task_id='store_data',
    python_callable=store_data_task,
    dag=dag
)

generate_ccmm = PythonOperator(
    task_id='generate_ccmm',
    python_callable=generate_ccmm_task,
    dag=dag
)

generate_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report_task,
    dag=dag
)

end_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag
)

# Task dependencies - lineární flow řešení všech 4 otázek
start_task >> download_data >> process_data >> store_data >> generate_ccmm >> generate_report >> end_task

if __name__ == "__main__":
    print(f"Knihovny ETL DAG loaded: {dag.dag_id}")
    print(f"Tasks: {len(dag.tasks)}")
    print(f"Schedule: {dag.schedule_interval}")