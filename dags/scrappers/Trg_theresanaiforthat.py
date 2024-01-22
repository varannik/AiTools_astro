from pendulum import datetime
from airflow.models.dagrun import DagRun

from airflow.decorators import (
    dag,
    task,
)

import pandas as pd



# When using the DAG decorator, The "dag_id" value defaults to the name of the function
# it is decorating if not explicitly set. In this example, the "dag_id" value would be "example_dag_basic".
@dag(
    # This defines how often your DAG will run, or the schedule by which your DAG runs. In this case, this DAG
    # will run daily
    # schedule="@daily",
    # This DAG is set to run for the first time on January 1, 2023. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on the schedule
    start_date=datetime(2023, 1, 1),
    schedule_interval='20 6 * * *',
    # When catchup=False, your DAG will only run the latest run that would have been scheduled. In this case, this means
    # that tasks will not be run between January 1, 2023 and 30 mins ago. When turned on, this DAG's first
    # run will be for the next 30 mins, per the its schedule
    catchup=False,
    default_args={
        "retries": 2,  # If a task fails, it will retry 2 times.
    },
    tags=["scrapper"],
)  # If set, this tag is shown in the DAG view of the Airflow UI


def Trg_theresanaiforthat():
    """
    Extract Data from theresanaiforthat as a refrence
    """

    @task()
    def initTables():
        from modules.dbExecute import cretaTables
        """
        Init raw tabels if not exist
        """
        tables = {

          "ref_theresanaiforthat_target_features":

          """
          url_ai             VARCHAR(300),
          url_stb            VARCHAR(300),
          url_fav            VARCHAR(300),
          url_log            VARCHAR(300),
          path_screen_shot   VARCHAR(300),
          insert_date        timestamp,
          delete_date        timestamp

          """,


          "ref_theresanaiforthat_target_features_temp":

          """
          url_ai             VARCHAR(300),
          insert_date        timestamp,

          """,

        }

        cretaTables(tables)


    @task()
    def fetchExistingTargetFeatures():
        """Fetch existing stored features of target Ais"""
        from modules.dbExecute import fetchData

        result = fetchData(tableName='"DW_RAW"."ref_theresanaiforthat_target_features_temp"', columns =['url_ai'])

        return result


    @task()
    def fetchExistingAiFeatures():
        """Fetch existing stored Ais"""
        from modules.dbExecute import fetchData

        result = fetchData(tableName='"DW_RAW"."ref_theresanaiforthat_target_features"', columns =['url_ai'])

        return result


    @task()
    def findNewAis(exFe, exTF):
        '''Incremental add new Ais'''

        if len(exTF)==0:
            pass
        else :
            exFe = exFe[~exFe['url_ai'].isin(exTF['url_ai'] )]
            print(f"{len(exFe)} New Ais to descover ")
        return exFe


    @task()
    def extractTargetFeatures(Ais, table='"DW_RAW"."ref_theresanaiforthat_target_features"' ,dag_run: DagRun | None = None):
        """
        Extract all features on target Ai website(Logo, screen shot, ...)
        """
        from modules.globalElements import targetPageFeatures
        from modules.driver import createDriver
        from modules.dbExecute import insertData

        # Open target site and
        URL_TARGET='https://www.theresanaiforthat.com'
        URL_SELENIUM="http://172.19.0.6:4444/wd/hub"


        driver = createDriver(URL_TARGET, URL_SELENIUM)

        for index, row in Ais.iterrows():
            try:
                df = targetPageFeatures (driver, row)
                df['insert_date'] = dag_run.queued_at
                insertData(df, table,)
            except:
                driver.quit()
                driver = createDriver(URL_TARGET, URL_SELENIUM)

        driver.quit()




    initTables()
    exTF = fetchExistingTargetFeatures()
    exFe = fetchExistingAiFeatures()
    newAis = findNewAis(exFe, exTF)
    extractTargetFeatures(newAis)
    # loadData(fullTargetFeatures)

Trg_theresanaiforthat()

