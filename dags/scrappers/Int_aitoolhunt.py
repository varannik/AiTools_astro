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
        "retries": 200,
        'retry_delay': timedelta(seconds=10),
    },
    tags=["scrapper"],
)  # If set, this tag is shown in the DAG view of the Airflow UI


def Int_aitoolhunt():
    '''Extract Data from AiToolHunt as a refrence'''

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
        tech_used 	  VARCHAR(255),
        features	  VARCHAR(10000),
        use_cases	  VARCHAR(10000),
        cat	          VARCHAR(100),
        url_internal  VARCHAR(255),
        url_ai	      VARCHAR(255),
        description   VARCHAR(2000),
        year	      varchar(6),
        likes	      int4,
        name	      VARCHAR(255),
        tags	      VARCHAR(100),
        url_screen_shot VARCHAR(255),
        insert_date   timestamp,
        delete_date   timestamp

        """,

        "ref_aitoolhunt_features_temp":

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

        result = fetchData(tableName='"DW_RAW"."ref_aitoolhunt_allAis"', columns =['cat', 'url_internal'])
        print(f"{len(result)} last existing Ais with duplicates")
        result = result.drop_duplicates(subset=['url_internal'], keep='first')
        print(f"{len(result)} last existing Ais unique" )
        return result


    @task()
    def extractFullfeatures(ais, dag_run: DagRun | None = None):
        '''Extract all Ais inside each category'''
        from modules.refAitoolhunt import aiFeatures
        from modules.driver import createDriver
        from modules.dbExecute import fetchData, insertData
        from selenium.common.exceptions import TimeoutException


        exda = fetchData(tableName='"DW_RAW"."ref_aitoolhunt_features_temp"', columns =['url_internal'])

        # Removed temperory discoverd ais
        if len(exda)>0:
            ais = ais[~ais['url_internal'].isin(exda['url_internal'] )]
        print(f"{len(ais)} new Ais to discover their features")


        try :
            # Open target site and
            URL_TARGET='https://www.aitoolhunt.com'
            URL_SELENIUM="http://172.19.0.2:4444/wd/hub"  # chrome-2
            driver = createDriver(URL_TARGET, URL_SELENIUM)
            driver.set_page_load_timeout(20)


            for index, row in ais.iterrows():
                print(row['url_internal'])
                features = aiFeatures(driver, URL_TARGET, ai = row)

                try:
                    features['insert_date'] = dag_run.queued_at
                    insertData(features, table='"DW_RAW"."ref_aitoolhunt_features"')
                    print("URL successfully Accessed")
                except:
                    driver.quit()
                    driver = createDriver(URL_TARGET, URL_SELENIUM)
                    driver.set_page_load_timeout(20)

                # add temp discoverd ai
                tm = {
                    'url_internal': row['url_internal'],
                    'insert_date' : dag_run.queued_at
                    }
                tm =  pd.DataFrame(tm, index=[0])
                insertData(tm, table='"DW_RAW"."ref_aitoolhunt_features_temp"')

        except TimeoutException as e:
            print("Page load Timeout Occurred. Quitting !!!")

            # add temp discoverd ai
            tm = {
                'url_internal': row['url_internal'],
                'insert_date' : dag_run.queued_at
                }
            tm =  pd.DataFrame(tm, index=[0])
            insertData(tm, table='"DW_RAW"."ref_aitoolhunt_features_temp"')
            driver.quit()

        # ----------------------------
        try:
            driver.quit()
        except:
            pass


    @task()
    def deleteUnchangedFeatures():
        '''Delete new duplicated rows without any changes in features'''
        from modules.dbExecute import exeSql

        command = 'Delete duplicated rows in ref_aitoolhunt_features'
        sql =   """
                    WITH prop AS (
                    SELECT *,RANK() OVER(PARTITION BY url_internal,
                                                        cat,
                                                        pricing,
                                                        semrush_rank,
                                                        "location",
                                                        tech_used,
                                                        features,
                                                        use_cases,
                                                        url_ai,
                                                        description,
                                                        "year",
                                                        likes,
                                                        "name",
                                                        tags,
                                                        url_screen_shot

                                                        Order by insert_date) rn
                    FROM "DW_RAW".ref_aitoolhunt_features
                    )

                    delete from "DW_RAW".ref_aitoolhunt_features
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
                delete from "DW_RAW".ref_aitoolhunt_features_temp
                """
        exeSql(sql, command)

    init = initTables()
    ais = fetchExistingAis()
    feat = extractFullfeatures(ais)
    deUn = deleteUnchangedFeatures()
    clTm = cleanTempDb()

    init>>ais>>feat>>deUn>>clTm

Int_aitoolhunt()

