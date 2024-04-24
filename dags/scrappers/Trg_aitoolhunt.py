from pendulum import datetime
from airflow.models.dagrun import DagRun

from airflow.decorators import (
    dag,
    task,
)

import pandas as pd
from datetime import timedelta


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
        "retries": 200,
        'retry_delay': timedelta(seconds=10),
    },
    tags=["scrapper"],
)  # If set, this tag is shown in the DAG view of the Airflow UI


def Trg_aitoolhunt():
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

          "ref_aitoolhunt_target_features":

          """
          url_ai             VARCHAR(300),
          url_stb            VARCHAR(300),
          url_fav            VARCHAR(300),
          url_log            VARCHAR(300),
          path_screen_shot   VARCHAR(300),
          insert_date        timestamp,
          delete_date        timestamp

          """,

          "ref_aitoolhunt_target_features_temp":

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

        result = fetchData(tableName='"DW_RAW"."ref_aitoolhunt_features"', columns =['url_ai'])
        print(f"{len(result)} last existing Ais with duplicates")
        result = result.drop_duplicates(subset=['url_ai'], keep='first')
        print(f"{len(result)} last existing Ais unique" )

        return result


    @task()
    def extractTargetFeatures(ais, dag_run: DagRun | None = None):
        """
        Extract all features on target Ai website(Logo, screen shot, ...)
        """
        from modules.globalElements import targetPageFeatures
        from modules.driver import createDriver
        from modules.dbExecute import insertData, fetchData
        from selenium.common.exceptions import TimeoutException


        exda = fetchData(tableName='"DW_RAW"."ref_aitoolhunt_target_features_temp"', columns =['url_ai'])

        if len(exda)>0:
            ais = ais[~ais['url_ai'].isin(exda['url_ai'] )]

        print(f"{len(ais)} new ais to discover ")


        try :
            # Open target site 
            URL_TARGET='https://www.aitoolhunt.com'
            URL_SELENIUM="http://172.19.0.12:4444/wd/hub"  # chrome-2
            driver = createDriver(URL_TARGET, URL_SELENIUM)
            driver.set_page_load_timeout(5)

            for index, row in ais.iterrows():
                try:
                    df = targetPageFeatures(driver, row)
                    df['insert_date'] = dag_run.queued_at
                    insertData(df, table='"DW_RAW"."ref_aitoolhunt_target_features"')
                    status = "S"
                    print("URL successfully Accessed")

                except:
                    status = "F"
                    driver.quit()
                    driver = createDriver(URL_TARGET, URL_SELENIUM)
                    driver.set_page_load_timeout(5)

                # Update temp table ----------
                dicTemp =dict()
                dfTemp = pd.DataFrame(columns=['url_ai', 'status', 'insert_date'])
                dicTemp.update({
                'url_ai'        :row['url_ai'],
                'status'        :status,
                'insert_date'   :dag_run.queued_at
                })
                dfTemp.loc[0]=dicTemp
                insertData(dfTemp, table='"DW_RAW"."ref_aitoolhunt_target_features_temp"')
            # ----------------------------

        except TimeoutException as e:
            print("Page load Timeout Occurred. Quitting !!!")
                            # Update temp table ----------
            dicTemp =dict()
            dfTemp = pd.DataFrame(columns=['url_ai', 'status', 'insert_date'])
            dicTemp.update({
            'url_ai'        :row['url_ai'],
            'status'        :"F",
            'insert_date'   :dag_run.queued_at
            })
            dfTemp.loc[0]=dicTemp
            insertData(dfTemp, table='"DW_RAW"."ref_aitoolhunt_target_features_temp"')
            driver.quit()


        # ----------------------------

        driver.quit()

    @task()
    def deleteUnchangedFeatures():
        '''Delete new duplicated rows without any changes in features'''
        from modules.dbExecute import exeSql

        command = 'Delete duplicated rows in ref_aitoolhunt_target_features'
        sql =   """
                    WITH prop AS (
                    SELECT *,RANK() OVER(PARTITION BY url_ai,
                                                        url_stb            ,
                                                        url_fav            ,
                                                        url_log            ,
                                                        path_screen_shot

                                                        Order by insert_date) rn
                    FROM "DW_RAW".ref_aitoolhunt_target_features
                    )

                    delete from "DW_RAW".ref_aitoolhunt_target_features
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
                delete from "DW_RAW".ref_aitoolhunt_target_features_temp
                """
        exeSql(sql, command)


    init = initTables()
    ais = fetchExistingAiFeatures()
    exta = extractTargetFeatures(ais)
    cleaT= deleteUnchangedFeatures()
    cleat= cleanTempDb()

    init>>ais>>exta>>cleaT>>cleat

Trg_aitoolhunt()