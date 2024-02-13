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
    schedule_interval='30 3 * * *',
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


def Int_futuretools():
    '''Extract Data from AiToolHunt as a refrence'''

    @task()
    def initTables():
        from modules.dbExecute import cretaTables
        """
        Init raw tabels if not exist
        """
        tables = {

        "ref_futuretools_features":

        """
        name	      VARCHAR(255),
        description   VARCHAR(3000),
        url_internal  VARCHAR(255),
        pricing       VARCHAR(100),
        url_ai        VARCHAR(255),
        up_vote       int4,
        tags	      VARCHAR(500),
        url_screen_shot VARCHAR(500),
        insert_date   timestamp,
        delete_date   timestamp

        """,

        "ref_futuretools_features_temp":

        """
        url_internal	VARCHAR(255),
        insert_date     timestamp
        """

        }

        cretaTables(tables)

    @task()
    def fetchExistingAis():
        """Fetch existing stored Ais"""
        from modules.dbExecute import fetchData

        result = fetchData(tableName='"DW_RAW"."ref_futuretools_allAis"', columns =['url_internal'])
        print(f"{len(result)} last existing Ais with duplicates")
        result = result.drop_duplicates(subset=['url_internal'], keep='first')
        print(f"{len(result)} last existing Ais unique" )
        return result


    @task()
    def extractFullfeatures(ais, dag_run: DagRun | None = None):
        '''Extract all Ais inside each category'''
        from modules.refFuturetools import aiFeatures
        from modules.driver import createDriver
        from modules.dbExecute import fetchData, insertData

        # Open target site and
        URL_TARGET='https://www.futuretools.io'
        URL_SELENIUM="http://172.19.0.5:4444/wd/hub" #chrome-3
        driver = createDriver(URL_TARGET, URL_SELENIUM)

        tempFeat = fetchData(tableName='"DW_RAW"."ref_futuretools_features_temp"', columns =['url_internal'])

        # Removed temperory discoverd ais
        if len(tempFeat)>0:
            ais = ais[~ais['url_internal'].isin(tempFeat['url_internal'] )]
        print(f"{len(ais)} new Ais to discover their features")

        for index, row in ais.iterrows():
            print(row['url_internal'])
            features = aiFeatures(driver, URL_TARGET, ai = row)

            try:
                features['insert_date'] = dag_run.queued_at
                insertData(features, table='"DW_RAW"."ref_futuretools_features"')
            except:
                pass

            # add temp discoverd ai
            tm = {
                'url_internal': row['url_internal'],
                'insert_date' : dag_run.queued_at
                }
            tm =  pd.DataFrame(tm, index=[0])
            insertData(tm, table='"DW_RAW"."ref_futuretools_features_temp"')

        driver.quit()


    @task()
    def deleteUnchangedFeatures():
        '''Delete new duplicated rows without any changes in features'''
        from modules.dbExecute import exeSql

        command = 'Delete duplicated rows in ref_futuretools_features'
        sql =   """
                    WITH prop AS (
                    SELECT *,RANK() OVER(PARTITION BY   url_internal,
                                                        name,
                                                        description,
                                                        pricing,
                                                        url_ai,
                                                        up_vote,
                                                        tags,
                                                        url_screen_shot

                                                        Order by insert_date) rn
                    FROM "DW_RAW".ref_futuretools_features
                    )

                    delete from "DW_RAW".ref_futuretools_features
                    where (url_internal, insert_date)
                    in (
                        with dup as (
                                    SELECT url_internal, insert_date
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
                delete from "DW_RAW".ref_futuretools_features_temp
                """
        exeSql(sql, command)

    init = initTables()
    ais = fetchExistingAis()
    feat = extractFullfeatures(ais)
    deUn = deleteUnchangedFeatures()
    clTm = cleanTempDb()

    init>>ais>>feat>>deUn>>clTm

Int_futuretools()

