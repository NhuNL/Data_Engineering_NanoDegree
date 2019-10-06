from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 table_info_dict="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.table_info_dict = table_info_dict
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        
        # AWS Hook
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)

       
        # Test each table 
        for table_dict in self.table_info_dict:
            table_name = table_dict["table"]
            column_that_should_not_be_null = table_dict["not_null"]

            # Check number of records (pass if > 0, else fail)
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {self.table} returned no results")
            elif records[0][0] < 1:
                raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
            else:
                # Now check is NOT NULL columns contain NULL
                null_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table} WHERE {column_that_should_not_be_null} IS NULL")
                if null_records[0][0] > 0:
                    col = column_that_should_not_be_null
                    raise ValueError(f"Data quality check failed. {self.table} contained {null_records[0][0]} null records for {col}")
                else:
                    self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")
    
   