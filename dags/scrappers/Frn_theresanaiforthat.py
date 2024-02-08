from pendulum import datetime
from airflow.models.dagrun import DagRun

from airflow.decorators import (
    dag,
    task,
)
# A function that sets sequential dependencies between tasks including lists of tasks


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


def Frn_theresanaiforthat():
    """
    Extract Data from tasks of Theresanaiforthat as a refrence
    """

    # @task.external_python(task_id="aitoolhunt_tabels", python=PATH_TO_PYTHON_BINARY)
    @task()
    def initTables():
        from modules.dbExecute import cretaTables


        """
        Init raw tabels if not exist
        """
        tables = {

        "ref_theresanaiforthat_allAis":

          """
          task          VARCHAR(255),
          name          VARCHAR(255),
          url_internal  VARCHAR(255),
          insert_date   timestamp,
          delete_date   timestamp

          """,
        "ref_theresanaiforthat_allAis_temp":

          """
          task          VARCHAR(255),
          name          VARCHAR(255),
          url_internal  VARCHAR(255)

          """,

        }

        cretaTables(tables)


    @task()
    def extractAllAis():
        '''Extract all Ais inside each category'''

        from modules.refTheresanaiforthat import  findAllTasks, discoverTasks
        from modules.driver import createDriver
        from modules.dbExecute import fetchData, insertData

        # Open target site and
        URL_TARGET='https://theresanaiforthat.com/tasks/'
        URL_SELENIUM="http://172.19.0.9:4444/wd/hub" # chrome-1

        driver = createDriver(URL_TARGET, URL_SELENIUM, enableCookies=True)

        tasks = findAllTasks(driver)

        # Fetch all tasks that already exist temporarily
        tempTasks = fetchData(tableName='"DW_RAW"."ref_theresanaiforthat_allAis_temp"', columns =['task'])

        # Keep uniqe tasks
        tempTasks = list(set(tempTasks['task']))
        if len(tempTasks)>0:
            tasks = [x for x in tasks if x not in tempTasks]
        print(f"{len(tasks)} new tasks")

        # loop over all tasks
        for tsk in tasks:
            ais = discoverTasks(driver, task=tsk,)
            insertData(ais, table='"DW_RAW"."ref_theresanaiforthat_allAis_temp"',)
        driver.quit()

        finalAis = fetchData(tableName='"DW_RAW"."ref_theresanaiforthat_allAis_temp"', columns =['task', 'name', 'url_internal'])
        return finalAis


    @task()
    def fetchExistingAis():
        '''Fetch existing stored Ais'''
        from modules.dbExecute import fetchData

        result = fetchData(tableName='"DW_RAW"."ref_theresanaiforthat_allAis"', columns =['task', 'name' , 'url_internal'])

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
    def loadData (df, table='"DW_RAW"."ref_theresanaiforthat_allAis"' ,dag_run: DagRun | None = None ):
        from modules.dbExecute import insertData

        print(f"{len(df)} rows ready to import at {dag_run.queued_at}")

        df['insert_date'] = dag_run.queued_at
        insertData(df, table,)


    @task()
    def cleanTempDb():
        '''Clean temporary discovery db'''
        from modules.dbExecute import exeSql

        command = 'Delete temporary db'
        sql =   """
                delete from "DW_RAW"."ref_theresanaiforthat_allAis_temp"
                """
        exeSql(sql, command)


    int = initTables()
    Ais = extractAllAis()
    ex = fetchExistingAis()
    ne = findNewAis(Ais, ex)
    ld = loadData(df= ne)
    ct = cleanTempDb()

    int>>Ais>>ex>>ne>>ld>>ct

Frn_theresanaiforthat()

