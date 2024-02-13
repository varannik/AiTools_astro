
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

    start_date=datetime(2023, 1, 1),
    schedule_interval='20 6 * * *',

    catchup=False,
    default_args={
        "retries": 200,
        'retry_delay': timedelta(seconds=10),
    },
    tags=["scrapper"],
)


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
          status             VARCHAR(1),
          insert_date        timestamp

          """,

        }

        cretaTables(tables)

    @task()
    def fetchExistingAiFeatures():
        """Fetch existing stored Ais"""
        from modules.dbExecute import fetchData

        result = fetchData(tableName='"DW_RAW"."ref_theresanaiforthat_features"', columns =['url_ai'])

        return result


    @task()
    def extractTargetFeatures(exea, dag_run: DagRun | None = None):
        """
        Extract all features on target Ai website(Logo, screen shot, ...)
        """
        from modules.globalElements import targetPageFeatures
        from modules.driver import createDriver
        from modules.dbExecute import insertData, fetchData
        from selenium.common.exceptions import TimeoutException


        exda = fetchData(tableName='"DW_RAW"."ref_theresanaiforthat_target_features_temp"', columns =['url_ai'])

        if len(exda)==0:
            pass
        else :
            exea = exea[~exea['url_ai'].isin(exda['url_ai'] )]
            print(f"{len(exea)} New Ais to descover ")

        try :
            # Open target site and
            URL_TARGET='https://www.theresanaiforthat.com'
            URL_SELENIUM="http://172.19.0.6:4444/wd/hub"  # chrome-1
            driver = createDriver(URL_TARGET, URL_SELENIUM)
            driver.set_page_load_timeout(5)

            for index, row in exea.iterrows():

                try:
                    df = targetPageFeatures (driver, row)
                    df['insert_date'] = dag_run.queued_at
                    insertData(df, table='"DW_RAW"."ref_theresanaiforthat_target_features"' ,)
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
                insertData(dfTemp, table='"DW_RAW"."ref_theresanaiforthat_target_features_temp"')



        except TimeoutException as e:
            print("Page load Timeout Occurred. Quitting !!!")
            driver.quit()


        # ----------------------------

        driver.quit()

    @task()
    def deleteUnchangedAis():
        '''Delete new duplicated rows without any changes in features'''
        from modules.dbExecute import exeSql

        command = 'Delete duplicated rows in ref_theresanaiforthat_target_features'
        sql =   """
                    WITH prop AS (
                    SELECT *,RANK() OVER(PARTITION BY url_ai,
                                                        url_stb            ,
                                                        url_fav            ,
                                                        url_log            ,
                                                        path_screen_shot

                                                        Order by insert_date) rn
                    FROM "DW_RAW".ref_theresanaiforthat_target_features
                    )

                    delete from "DW_RAW".ref_theresanaiforthat_target_features
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
                delete from "DW_RAW".ref_theresanaiforthat_target_features_temp
                """
        exeSql(sql, command)


    init = initTables()
    exea = fetchExistingAiFeatures()
    etfa = extractTargetFeatures(exea)
    unch = deleteUnchangedAis()
    cltm = cleanTempDb()

    init>>exea>>etfa>>unch>>cltm


Trg_theresanaiforthat()

