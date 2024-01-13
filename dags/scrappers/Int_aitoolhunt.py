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


def Int_aitoolhunt():
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
          "ref_aitoolhunt_features":

          """
          pricing       VARCHAR(100),
          Semrush_rank  VARCHAR(100),
          location      VARCHAR(100),
          tech_used 	VARCHAR(255),
          features	    VARCHAR(10000),
          use_cases	    VARCHAR(10000),
          cat	        VARCHAR(100),
          url_internal	VARCHAR(255),
          url_ai	    VARCHAR(255),
          description   VARCHAR(2000),
          year	        varchar(6),
          likes	        int4,
          name	        VARCHAR(255),
          tags	        VARCHAR(100),
          url_screen_shot VARCHAR(255),
          insert_date   timestamp,
          delete_date   timestamp,

          UNIQUE(url_internal)
          """,

        }

        cretaTables(tables)


    @task()
    def extractFullfeatures(ais):
        """
        Extract all Ais inside each category
        """

        from modules.refAitoolhunt import aiFeatures
        from modules.driver import createDriver

        # Open target site and
        URL_TARGET='https://www.aitoolhunt.com'
        URL_SELENIUM="http://172.19.0.6:4444/wd/hub"

        driver = createDriver(URL_TARGET, URL_SELENIUM)
        print(f"{len(ais)} new Ais to discover their features")
        print(ais)
        features = aiFeatures (driver, URL_TARGET, ais)
        driver.quit()
        return features

    @task()
    def fetchExistingAis():
        """Fetch existing stored Ais"""
        from modules.dbExecute import fetchData

        result = fetchData(tableName='"DW_RAW"."ref_aitoolhunt_allAis"', columns =['cat', 'url_internal'])
        print(f"{len(result)} last existing Ais with duplicates")
        result = result.drop_duplicates(subset=['url_internal'], keep='first')
        print(f"{len(result)} last existing Ais unique" )
        return result

    @task()
    def fetchExistingAiFeatures():
        """Fetch existing stored Ais"""
        from modules.dbExecute import fetchData

        result = fetchData(tableName='"DW_RAW"."ref_aitoolhunt_features"', columns =['cat', 'url_internal'])

        return result

    @task()
    def findNewAis(exAi,exFe):
        '''Incremental add new Ais'''

        if len(exFe)==0:
            pass
        else :
            exAi = exAi[~exAi['url_internal'].isin(exFe['url_internal'] )]
        return exAi


    @task()
    def loadData (df, table='"DW_RAW"."ref_aitoolhunt_features"' ,dag_run: DagRun | None = None ):
        from modules.dbExecute import insertData

        df['insert_date'] = dag_run.queued_at
        insertData(df, table,)


    initTables()
    exAi = fetchExistingAis()
    exFe = fetchExistingAiFeatures()
    newAis = findNewAis(exAi, exFe)
    fullFeatures = extractFullfeatures(newAis)
    loadData(fullFeatures)

Int_aitoolhunt()

