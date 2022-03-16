from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = "redshift",
                 database='', # change the redshift connection with this or check if by run parametrisable
                 target_table='',
                 select_sql ='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.database = database
        self.target_table = target_table
        self.select_sql = select_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.schema = self.database
        self.log.info('Deleting rows from Fact Table')
        delete_sql = f"DELETE FROM {self.target_table}"
        redshift.run(delete_sql)
        
        self.log.info('Loading Fact Table using LoadFactOperator')
        insert_sql = f'INSERT INTO {self.target_table} {self.select_sql}'
        redshift.run(insert_sql)
