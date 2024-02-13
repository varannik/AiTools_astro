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


def Trg_futuretools():
    """
    Extract Data from futuretools as a refrence
    """


    # @task.external_python(task_id="futuretools_tabels", python=PATH_TO_PYTHON_BINARY)
    @task()
    def initTables():
        from modules.dbExecute import cretaTables
        """
        Init raw tabels if not exist
        """
        tables = {

          "ref_futuretools_target_features":

          """
          url_ai             VARCHAR(300),
          url_stb            VARCHAR(300),
          url_fav            VARCHAR(300),
          url_log            VARCHAR(300),
          path_screen_shot   VARCHAR(300),
          insert_date        timestamp,
          delete_date        timestamp

          """,

          "ref_futuretools_target_features_temp":

          """
          url_ai             VARCHAR(300),
          status             VARCHAR(1),
          insert_date        timestamp
          """,

        }

        cretaTables(tables)

    @task()
    def fetchExistingAiFeatures():
        """Fetch existing stored Ais"""
        from modules.dbExecute import fetchData

        result = fetchData(tableName='"DW_RAW"."ref_futuretools_features"', columns =['url_ai'])

        return result


    @task()
    def checkTempAis(ais):
        '''Extract scrapped ais'''
        from modules.dbExecute import fetchData

        result = fetchData(tableName='"DW_RAW"."ref_futuretools_target_features_temp"', columns =['url_ai'])

        if len(result)>0:
            ais = ais[~ais['url_ai'].isin(result['url_ai'] )]

        print(f"{len(ais)} new ais to discover ")
        return ais


    @task()
    def extractTargetFeatures(ais, dag_run: DagRun | None = None):
        """
        Extract all features on target Ai website(Logo, screen shot, ...)
        """
        from modules.refFuturetools import targetPageFeatures
        from modules.driver import createDriver
        from modules.dbExecute import insertData

        # Open target site and
        URL_TARGET='https://www.futuretools.io'
        URL_SELENIUM="http://172.19.0.5:4444/wd/hub" #chrome-2

        driver = createDriver(URL_TARGET, URL_SELENIUM)



        for index, row in ais.iterrows():
            try:
                df = targetPageFeatures(driver, row)
                df['insert_date'] = dag_run.queued_at
                insertData(df, table='"DW_RAW"."ref_futuretools_target_features"')
                status = "S"


            except:
                status = "F"
                driver.quit()
                driver = createDriver(URL_TARGET, URL_SELENIUM)

            # Update temp table ----------
            dicTemp =dict()
            dfTemp = pd.DataFrame(columns=['url_ai', 'status', 'insert_date'])
            dicTemp.update({
            'url_ai'        :row['url_ai'],
            'status'        :status,
            'insert_date'   :dag_run.queued_at
            })
            dfTemp.loc[0]=dicTemp
            insertData(dfTemp, table='"DW_RAW"."ref_futuretools_target_features_temp"')
            # ----------------------------

        driver.quit()

    @task()
    def deleteUnchangedFeatures():
        '''Delete new duplicated rows without any changes in features'''
        from modules.dbExecute import exeSql

        command = 'Delete duplicated rows in ref_futuretools_target_features'
        sql =   """
                    WITH prop AS (
                    SELECT *,RANK() OVER(PARTITION BY   url_ai,
                                                        url_stb            ,
                                                        url_fav            ,
                                                        url_log            ,
                                                        path_screen_shot

                                                        Order by insert_date) rn
                    FROM "DW_RAW".ref_futuretools_target_features
                    )

                    delete from "DW_RAW".ref_futuretools_target_features
                    where (url_ai, insert_date)
                    in (
                        with dup as (
                                    SELECT url_ai, insert_date
                                    FROM prop
                                    where rn >1
                                    )
                        select * from dup
                        );
                """
        exeSql(sql, command)

    @task()
    def cleanTempDb():
        '''Clean temporary descovery db'''
        from modules.dbExecute import exeSql

        command = 'Delete temporary db'
        sql =   """
                delete from "DW_RAW".ref_futuretools_target_features_temp
                """
        exeSql(sql, command)


    init = initTables()
    ais = fetchExistingAiFeatures()
    unai = checkTempAis(ais)
    exta = extractTargetFeatures(unai)
    cleaT= deleteUnchangedFeatures()
    cleat= cleanTempDb()

    init>>ais>>unai>>exta>>cleaT>>cleat

Trg_futuretools()