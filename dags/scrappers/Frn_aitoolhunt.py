from pendulum import datetime
from airflow.models.dagrun import DagRun

from airflow.decorators import (
    dag,
    task,
)
# A function that sets sequential dependencies between tasks including lists of tasks
from airflow.models.baseoperator import chain

import pandas as pd


# When using the DAG decorator, The "dag_id" value defaults to the name of the function
# it is decorating if not explicitly set. In this example, the "dag_id" value would be "example_dag_basic".
@dag(

    start_date=datetime(2023, 1, 1),
    schedule_interval='0 2 * * *',
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

          "ref_aitoolhunt_allAis_temp":

          """
          cat VARCHAR(255),
          url_internal VARCHAR(255),
          insert_date timestamp

          """,

        }

        cretaTables(tables)


    @task()
    def extractAllAis():
        """
        Extract all Ais inside each category
        """
        from modules.refAitoolhunt import allAvailableAi , findCategories
        from modules.driver import createDriver
        from modules.dbExecute import fetchData, insertData

        # Open target site and
        URL_TARGET='https://www.aitoolhunt.com'
        URL_SELENIUM="http://172.19.0.12:4444/wd/hub"  #chrome-2

        driver = createDriver(URL_TARGET, URL_SELENIUM)

        cats = findCategories(driver)

        # Fetch all cats that already exist temporarily
        tempCats = fetchData(tableName='"DW_RAW"."ref_aitoolhunt_allAis_temp"', columns =['cat'])

        # Keep uniqe cats
        tempCats = list(set(tempCats['cat']))
        if len(tempCats)>0:
            cats = [x for x in cats if x not in tempCats]
        print(f"{len(cats)} new tasks")

        # loop over all cats
        for cat in cats:
            ais = allAvailableAi(driver, cat, URL_TARGET)
            insertData(ais, table='"DW_RAW"."ref_aitoolhunt_allAis_temp"',)
        driver.quit()

        finalAis = fetchData(tableName='"DW_RAW"."ref_aitoolhunt_allAis_temp"', columns =['cat', 'url_internal'])

        return finalAis


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


    @task()
    def cleanTempDb():
        '''Clean temporary discovery db'''
        from modules.dbExecute import exeSql

        command = 'Delete temporary db'
        sql =   """
                delete from "DW_RAW"."ref_aitoolhunt_allAis_temp"
                """
        exeSql(sql, command)

    int = initTables()
    ais = extractAllAis()
    ex = fetchExistingAis()
    ne = findNewAis(ais, ex)
    ld = loadData(df=ne)
    ct = cleanTempDb()

    int>>ais>>ex>>ne>>ld>>ct

Frn_aitoolhunt()

