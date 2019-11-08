from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql = """
        INSERT INTO {}
        {};
        COMMIT;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 append_data = True,
                 table="",
                 load_sql_stmt="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        seld.append_data = True
        self.table = table
        self.load_sql_stmt = load_sql_stmt

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Loading dimension table {self.table} in Redshift")
        if self.append_data = True:
            formatted_sql = 'INSERT INTO %s %s' % (self.table, self.load_sql_stmt)
            redshift.run(formatted_sql)
        else:
            formatted_sql = 'DELETE FROM %s' % self.table
            redshift.run(formatted_sql)
            
            formatted_sql = 'INSERT INTO %s %s' % (self.table, self.load_sql_stmt)
            redshift.run(formatted_sql)
