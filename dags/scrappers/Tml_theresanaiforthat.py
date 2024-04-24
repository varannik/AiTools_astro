from pendulum import datetime
from airflow.models.dagrun import DagRun

from airflow.decorators import (
    dag,
    task,
)
# A function that sets sequential dependencies between tasks including lists of tasks
from airflow.models.baseoperator import chain

import pandas as pd

@dag(
    # This defines how often your DAG will run, or the schedule by which your DAG runs. In this case, this DAG
    # will run daily
    # schedule="@daily",
    # This DAG is set to run for the first time on January 1, 2023. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on the schedule
    start_date=datetime(2023, 1, 1),
    # When catchup=False, your DAG will only run the latest run that would have been scheduled. In this case, this means
    # that tasks will not be run between January 1, 2023 and 30 mins ago. When turned on, this DAG's first
    # run will be for the next 30 mins, per the its schedule
    schedule_interval='0 2 * * *',
    catchup=False,
    default_args={
        "retries": 2,  # If a task fails, it will retry 2 times.
    },
    tags=["scrapper"],
)  # If set, this tag is shown in the DAG view of the Airflow UI


def Tml_theresanaiforthat():
    """
    Extract Data from timeline of Theresanaiforthat as a refrence
    """


    # @task.external_python(task_id="aitoolhunt_tabels", python=PATH_TO_PYTHON_BINARY)
    @task()
    def initTables():
        from modules.dbExecute import cretaTables


        """
        Init raw tabels if not exist
        """
        tables = {

          "ref_theresanaiforthat_timeline":

          """
          name          VARCHAR(255),
          task          VARCHAR(255),
          url_internal  VARCHAR(255),
          url_ai        VARCHAR(255),
          rel_date      timestamp,
          insert_date   timestamp,
          delete_date   timestamp

          """,

        }

        cretaTables(tables)


    @task()
    def extractAllAis():
        """
        Extract all Ais inside each category
        """
        from modules.refTheresanaiforthat import fullAis
        from modules.driver import createDriver

        # Open target site and
        URL_TARGET='https://theresanaiforthat.com/timeline/'
        URL_SELENIUM="http://172.19.0.11:4444/wd/hub"

        driver = createDriver(URL_TARGET, URL_SELENIUM)

        Ais = fullAis(driver)

        driver.quit()

        return Ais


    @task()
    def fetchExistingAis():
        """Fetch existing stored Ais"""
        from modules.dbExecute import fetchData

        result = fetchData(tableName='"DW_RAW"."ref_theresanaiforthat_timeline"', columns =['name', 'url_internal'])

        return result


    @task()
    def findNewAis(df,ex):
        '''Incremental add new Ais'''

        if len(ex)==0:
            pass
        else :
            df = df[~df['url_internal'].isin(ex['url_internal'] )]
        return df


    @task()
    def loadData (df, table='"DW_RAW"."ref_theresanaiforthat_timeline"' ,dag_run: DagRun | None = None ):
        from modules.dbExecute import insertData

        print(f"{len(df)} rows ready to import at {dag_run.queued_at}")

        df['insert_date'] = dag_run.queued_at
        insertData(df, table,)

    int = initTables()
    Ais = extractAllAis()
    ex = fetchExistingAis()
    ne = findNewAis(Ais, ex)
    ld = loadData(df= ne)
    # Ais = pd.read_excel('/usr/local/airflow/plugins/modules/fullais.xlsx', index_col=None)
    # loadData(df= Ais, table='"DW_RAW"."ref_aitoolhunt_allAis"')
    # initTables >> extractAllAis >> loadData
    int>>Ais>>ex>>ne>>ld

Tml_theresanaiforthat()

