from pendulum import datetime
from airflow.models.dagrun import DagRun

from airflow.decorators import (
    dag,
    task,
)
# A function that sets sequential dependencies between tasks including lists of tasks
from airflow.models.baseoperator import chain

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


def Frn_aitoolhunt():
    """
    Extract Data from AiToolHunt as a refrence
    """


    # @task.external_python(task_id="aitoolhunt_tabels", python=PATH_TO_PYTHON_BINARY)
    @task()
    def initTables():
        from modules.dbExecute import cretaTables


        """
        Init raw tabels if not exist
        """
        tables = {

          "ref_aitoolhunt_allAis":

          """
          cat VARCHAR(255),
          url_internal VARCHAR(255),
          insert_date timestamp,
          delete_date timestamp

          """,

        }

        cretaTables(tables)


    @task()
    def extractAllAis():
        """
        Extract all Ais inside each category
        """
        from modules.refAitoolhunt import fullAis , findCategories
        from modules.driver import createDriver

        # Open target site and
        URL_TARGET='https://www.aitoolhunt.com'
        URL_SELENIUM="http://172.19.0.6:4444/wd/hub"

        driver = createDriver(URL_TARGET, URL_SELENIUM)

        categories = findCategories(driver)
        Ais = fullAis(driver, categories, URL_TARGET)

        driver.quit()

        return Ais

    @task()
    def fetchExistingAis():
        """Fetch existing stored Ais"""
        from modules.dbExecute import fetchData

        result = fetchData(tableName='"DW_RAW"."ref_aitoolhunt_allAis"', columns =['cat', 'url_internal'])

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
    def loadData (df, table='"DW_RAW"."ref_aitoolhunt_allAis"' ,dag_run: DagRun | None = None ):
        from modules.dbExecute import insertData

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

Frn_aitoolhunt()

