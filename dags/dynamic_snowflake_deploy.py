import os
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Dynamically resolve the absolute path to the 'sql' directory
# This assumes the structure is: project_root/dags/dynamic_dag.py and project_root/sql/...
DAGS_FOLDER = os.path.dirname(os.path.abspath(__file__))
SQL_DIR = os.path.join(DAGS_FOLDER, 'sql')

@dag(
    dag_id='dynamic_snowflake_infrastructure_deploy',
    schedule_interval=None, # Triggered manually for deployment
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['snowflake', 'dynamic', 'deployment']
)
def dynamic_snowflake_deploy():

    # 1. TaskFlow API & SnowflakeHook 
    @task
    def execute_sql_file(file_path: str):
        """Reads an SQL file and executes it via SnowflakeHook"""
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_query = f.read()
            
        # Using SnowflakeHook instead of SnowflakeOperator
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        hook.run(sql_query, autocommit=True)
        print(f"Successfully executed: {file_path}")

    # 2. Dynamic DAG Generation 
    if os.path.exists(SQL_DIR):
        # Sort folders to ensure execution order: 01_infrastructure -> 02_tables, etc.
        folders = sorted(os.listdir(SQL_DIR))
        previous_layer_tasks = []

        for folder in folders:
            if folder == '99_testing':
                continue
            folder_path = os.path.join(SQL_DIR, folder)
            
            if os.path.isdir(folder_path):
                current_layer_tasks = []
                # Sort files to ensure logical order within the folder
                files = sorted(os.listdir(folder_path))
                
                for filename in files:
                    if filename.endswith('.sql'):
                        file_path = os.path.join(folder_path, filename)
                        
                        # Generate a clean task_id (e.g., deploy_02_dim_passengers)
                        task_id = f"deploy_{folder[:2]}_{filename[:-4]}"
                        
                        # Dynamically create the task
                        task_obj = execute_sql_file.override(task_id=task_id)(file_path)
                        current_layer_tasks.append(task_obj)
                
                # 3. Set Dependencies
                # Ensure all tasks from the previous folder finish before the current folder starts
                if previous_layer_tasks and current_layer_tasks:
                    for p_task in previous_layer_tasks:
                        for c_task in current_layer_tasks:
                            p_task >> c_task
                
                if current_layer_tasks:
                    previous_layer_tasks = current_layer_tasks

# Initialize the DAG
deploy_dag = dynamic_snowflake_deploy()