from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 database='',
                 target_table='',
                 insert_mode='delete-load', # if not, then just append
                 select_sql ='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.database = database
        self.target_table = target_table
        self.insert_mode = insert_mode
        self.select_sql = select_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.schema = self.database
        
        if self.insert_mode == 'delete-load':
            self.log.info('insert_mode = delete-load : deleting existing rows from target_table')
            delete_sql = f"DELETE FROM {self.target_table}"
            redshift.run(delete_sql)

        self.log.info('Using LoadDimensionOperator')
        insert_sql = f'INSERT INTO {self.target_table} {self.select_sql}'
        redshift.run(insert_sql)