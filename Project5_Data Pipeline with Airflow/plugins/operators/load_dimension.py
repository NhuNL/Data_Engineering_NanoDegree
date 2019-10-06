from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    # to insert records to dim table
    insert_sql = """
        INSERT INTO {}
        {}
        ;
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 table="",
                 delete_load=False,
                 sql_source="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.delete_load = delete_load
        self.sql_source = sql_source

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete_load:
           self.log.info("Remove data from Redshift table")
           redshift.run("DELETE FROM {}".format(self.table))

        RunningSQL = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.sql_source
        )
        self.log.info(f"Execute {RunningSQL} ...")
        redshift.run(RunningSQL)

        self.log.info('LoadDimensionOperator not implemented yet')
