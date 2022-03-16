from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields=["s3_key"]

    sql_copy = """
        copy {}
        from '{}'
        access_key_id '{}'
        secret_access_key '{}'
        format as json '{}'
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id='redshift',
                 aws_credentials='',
                 target_table='',
                 s3_bucket='',
                 s3_key='',
                 json_format='auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format

    def execute(self, context):
        self.log.info("Getting the AWS credentials")
        credentials = AwsHook(self.aws_credentials).get_credentials()
        self.log.info("Creating the Redshift connection with the PostgresHook")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Prepare full S3 path
        rendered_key = self.s3_key.format(**context)
        s3_path = f's3://{self.s3_bucket}/{rendered_key}'
        # Fill final sql copy statement
        sql_formatted = StageToRedshiftOperator.sql_copy.format(
            self.target_table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_format
            )
        self.log.info(f"COPYing data to {self.target_table}") 
        redshift.run(sql_formatted)
        self.log.info("COPYing finished: staging of table finalized")