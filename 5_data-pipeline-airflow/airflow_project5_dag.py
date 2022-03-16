# /opt/airflow/start.sh
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, 
                               PostgresOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past' : False,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5), # change to minutes=5
    'catchup_by_default' : False,
    'email_on_retry' : False,
}

dag = DAG('udac_example_dag_v5',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1,
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials='aws_credentials',
    target_table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    delimiter=',',
    json_format='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials='aws_credentials',
    target_table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    delimiter=',',
    json_format='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    database='dev',
    target_table='songplays',
    select_sql = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    database = 'dev',
    target_table = 'users',
    insert_mode = 'truncate',
    select_sql = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    database = 'dev',
    target_table = 'songs',
    insert_mode = 'truncate',
    select_sql = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    database = 'dev',
    target_table = 'artists',
    insert_mode = 'truncate',
    select_sql = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    database = 'dev',
    target_table = 'time',
    insert_mode = 'truncate',
    select_sql = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    quality_checks = {
        "SELECT COUNT(*) FROM songplays WHERE playid is null" : 0,
        "SELECT COUNT(*) FROM artists WHERE artistid is null" : 0,
        "SELECT COUNT(*) FROM songs WHERE songid is null" : 0,
        "SELECT COUNT(*) FROM users WHERE userid is null" : 0
    }
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables
create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator