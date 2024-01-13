from pendulum import datetime
from airflow.models.dagrun import DagRun
from airflow.decorators import (
    dag,
    task,
)
# A function that sets sequential dependencies between tasks including lists of tasks
from airflow.models.baseoperator import chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


import pandas as pd

# PATH_TO_PYTHON_BINARY= os.path.join(os.path.dirname(os.path.normpath(os.path.dirname(os.path.abspath(__file__)))) ,"venv/scrapper/bin/python")

PATH_TO_PYTHON_BINARY= '/usr/local/airflow/scrapper_venv/bin/python3'


# When using the DAG decorator, The "dag_id" value defaults to the name of the function
# it is decorating if not explicitly set. In this example, the "dag_id" value would be "example_dag_basic".
@dag(
    # This defines how often your DAG will run, or the schedule by which your DAG runs. In this case, this DAG
    # will run daily
    schedule="@daily",
    # This DAG is set to run for the first time on January 1, 2023. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on the schedule
    start_date=datetime(2023, 1, 1),
    # When catchup=False, your DAG will only run the latest run that would have been scheduled. In this case, this means
    # that tasks will not be run between January 1, 2023 and 30 mins ago. When turned on, this DAG's first
    # run will be for the next 30 mins, per the its schedule
    catchup=False,
    default_args={
        "retries": 2,  # If a task fails, it will retry 2 times.
    },
    tags=["scrapper"],
)  # If set, this tag is shown in the DAG view of the Airflow UI


def Frn_futuretools():
    """
    Extract Data from AiToolHunt as a refrence
    """

    @task()
    def initTables():
        from modules.dbExecute import cretaTables


        """
        Init raw tabels if not exist
        """
        tables = {

          "ref_futuretools_allAis":

          """
          pageUrl VARCHAR(255),
          screenshotUrl VARCHAR(320),
          shortDes VARCHAR(1000),
          name VARCHAR(255),
          priveUrl VARCHAR(255),
          insertDate timestamp,
          removeDate timestamp

          """,

        }

        cretaTables(tables)


    # @task.external_python(task_id="aitoolhunt_allais", python=PATH_TO_PYTHON_BINARY)
    @task()
    def extractAllAis():
        """
        Extract all Ais inside each category
        """
        from modules.refFuturetools import scroll_down_to_load
        from modules.driver import createDriver

        # Open target site and
        URL_TARGET='https://www.futuretools.io'
        URL_SELENIUM="http://172.19.0.7:4444/wd/hub"

        driver = createDriver(URL_TARGET, URL_SELENIUM)
        Ais = scroll_down_to_load(driver)


        return Ais

    @task()
    def fetchData():
        """Fetch existing stored Ais"""
        from modules.dbExecute import fetchData

        result = fetchData(tableName='"DW_RAW"."ref_futuretools_allAis"', columns =['name', 'priveUrl'])

        return result


    @task()
    def findNewAis(df,ex):
        '''Incremental add new Ais'''

        if len(ex)==0:
            pass
        else :
            df = df[~df['priveUrl'].isin(ex['priveUrl'] )]
        return df

    @task()
    def loadData (df, table='"DW_RAW"."ref_futuretools_allAis"' ,dag_run: DagRun | None = None ):
        from modules.dbExecute import insertData

        df['insertDate'] = dag_run.queued_at
        insertData(df, table,)

    initTables()
    Ais = extractAllAis()
    ex = fetchData()
    newAis = findNewAis(Ais, ex)
    loadData(df= newAis)



Frn_futuretools()

