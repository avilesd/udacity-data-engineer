from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 quality_checks= {},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.quality_checks = quality_checks
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        failed_tests = []

        self.log.info('Starting data quality checks from DataQualityOperator')
        for sql, expected_result in self.quality_checks.items():
            test_result = redshift.get_records(sql)[0][0]

            if test_result == expected_result:
                self.log.info(f"{sql} test ran successfully and matched expected return:{expected_result}")
            else:
                failed_tests.append(sql)
                raise ValueError(f"{sql} failed: expected result was:{expected_result} and got:{test_result}.")

        if len(failed_tests) > 0:
            self.log.info(f'The following tests failed: {failed_tests}')
        else:
            self.log.info("All tests passed successfully")
