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
    # schedule="@daily",
    # This DAG is set to run for the first time on January 1, 2023. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on the schedule
    start_date=datetime(2023, 1, 1),
    schedule_interval='30 3 * * *',
    # When catchup=False, your DAG will only run the latest run that would have been scheduled. In this case, this means
    # that tasks will not be run between January 1, 2023 and 30 mins ago. When turned on, this DAG's first
    # run will be for the next 30 mins, per the its schedule
    catchup=False,
    default_args={
        "retries": 2,  # If a task fails, it will retry 2 times.
    },
    tags=["scrapper"],
)  # If set, this tag is shown in the DAG view of the Airflow UI


def Int_theresanaiforthat():
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

                    "ref_theresanaiforthat_features_temp":

                    """
                    url_internal    VARCHAR(255),
                    insert_date     timestamp
                    """
                    ,

                    "ref_theresanaiforthat_features":

                    """
                    url_internal    VARCHAR(255),
                    rate            float4,
                    count_rate      int4,
                    count_save      int4,
                    count_comments  int4,
                    usecase         VARCHAR(300),
                    tags            VARCHAR(1000),
                    price           VARCHAR(300),
                    url_screen_shot VARCHAR(300),
                    url_ai          VARCHAR(300),
                    insert_date     timestamp
                    """

                    ,

                    "ref_theresanaiforthat_description":

                    """
                    url_internal  VARCHAR(255),
                    descrption    VARCHAR(3000),
                    insert_date   timestamp
                    """
                    ,

                    "ref_theresanaiforthat_impacted_jobs":

                    """
                    url_internal  VARCHAR(255),
                    title         VARCHAR(300),
                    impact        int4,
                    count_tasks   int4,
                    count_ais     int4,
                    insert_date   timestamp
                    """

                    ,

                    "ref_theresanaiforthat_attributes":

                    """
                    url_internal  VARCHAR(255),
                    attribute     VARCHAR(300),
                    description   VARCHAR(300),
                    insert_date   timestamp

                    """
        }

        cretaTables(tables)

    @task()
    def fetchExistingAis():
        """Fetch existing stored Ais"""
        from modules.dbExecute import fetchData

        result = fetchData(tableName='"DW_RAW"."ref_theresanaiforthat_allAis"', columns =['url_internal'])
        print(f"{len(result)} last existing Ais with duplicates")
        uniqueAis = result.drop_duplicates(subset=['url_internal'], keep='first')
        print(f"{len(uniqueAis)} last existing Ais unique" )
        return uniqueAis


    @task()
    def checkExistingAis(ais):
        '''Extract scrapped ais'''
        from modules.dbExecute import fetchData

        result = fetchData(tableName='"DW_RAW"."ref_theresanaiforthat_features_temp"', columns =['url_internal'])

        if len(result)>0:
            ais = ais[~ais['url_internal'].isin(result['url_internal'] )]

        print(f"{len(ais)} new ais to descover ")
        return ais




    @task()
    def extractFullfeatures(ais ,dag_run: DagRun | None = None):
        """
        Extract all Ais inside each category
        """
        from modules.refTheresanaiforthat import soupParser, extFeatures, extMostImpacedJobs, extdesc, extAtts
        from modules.driver import createDriver
        from modules.dbExecute import insertData

        # Open target site and
        URL_TARGET='https://theresanaiforthat.com'
        URL_SELENIUM="http://172.19.0.6:4444/wd/hub" # chrome-2

        driver = createDriver(URL_TARGET, URL_SELENIUM, enableCookies=True)

        for index, row in ais.iterrows():
            intUrl = row['url_internal']
            print(intUrl)

            if 'http' in intUrl:
                pass
            else:
                # try:
                url_ai = [URL_TARGET, intUrl]
                url_ai = ''.join(url_ai)
                driver.get(url_ai)
                soup = soupParser(driver)

                feat = extFeatures(soup, intUrl)
                jobs = extMostImpacedJobs(soup, intUrl)
                desc = extdesc(soup, intUrl)
                atts = extAtts(soup, intUrl)


                try:
                    feat['insert_date'] = dag_run.queued_at
                    insertData(feat, table= '"DW_RAW"."ref_theresanaiforthat_features"')
                except:
                    pass

                try:
                    jobs['insert_date'] = dag_run.queued_at
                    insertData(jobs, table= '"DW_RAW"."ref_theresanaiforthat_impacted_jobs"')
                except:
                    pass

                try:
                    desc['insert_date'] = dag_run.queued_at
                    insertData(desc, table= '"DW_RAW"."ref_theresanaiforthat_description"')
                except:
                    pass

                try:
                    atts['insert_date'] = dag_run.queued_at
                    insertData(atts, table= '"DW_RAW"."ref_theresanaiforthat_attributes"')
                except:
                    pass

                # add to temp discoverd db
                tm = {
                    'url_internal': intUrl,
                    'insert_date' : dag_run.queued_at
                    }
                tm =  pd.DataFrame(tm, index=[0])
                insertData(tm, table= '"DW_RAW"."ref_theresanaiforthat_features_temp"')


        driver.quit()


    @task()
    def deleteUnchangedProperties():
        '''Delete new duplicated rows without any changes in properties'''
        from modules.dbExecute import exeSql

        command = 'Delete duplicated rows in ref_theresanaiforthat_features'
        sql =   """
                WITH prop AS (
                SELECT *,RANK() OVER(PARTITION BY url_internal,
                                                    rate,
                                                    count_rate,
                                                    count_save,
                                                    count_comments,
                                                    usecase,
                                                    tags,
                                                    price,
                                                    url_screen_shot,
                                                    url_ai
                                                    Order by insert_date) rn
                FROM "DW_RAW".ref_theresanaiforthat_features
                )

                delete from "DW_RAW".ref_theresanaiforthat_features
                where (url_internal, insert_date)
                in (
                    with dup as (
                                SELECT url_internal as dupUrl, insert_date as dupDate
                                FROM prop
                                where rn >1
                                )
                    select * from dup
                    );
                """
        exeSql(sql, command)

    @task()
    def deleteUnchangedAttributes():
        '''Delete new duplicated rows without any changes in attributes'''
        from modules.dbExecute import exeSql

        command = 'Delete duplicated rows in ref_theresanaiforthat_attributes'
        sql =   """
                WITH atts AS (
                SELECT *,RANK() OVER(PARTITION BY url_internal,
                                                 "attribute",
                                                 description  Order by insert_date) rn
                FROM "DW_RAW".ref_theresanaiforthat_attributes
                )

                delete from "DW_RAW".ref_theresanaiforthat_attributes
                where (url_internal, insert_date)
                in (
                    with dup as (
                                SELECT url_internal as dupUrl, insert_date as dupDate
                                FROM atts
                                where rn >1
                                )
                    select * from dup
                    );
                """
        exeSql(sql, command)


    @task()
    def deleteUnchangedDescrption():
        '''Delete new duplicated rows without any changes in descrptions'''
        from modules.dbExecute import exeSql

        command = 'Delete duplicated rows in ref_theresanaiforthat_description'
        sql =   """
                WITH dec AS (
                SELECT *,RANK() OVER(PARTITION BY url_internal,
                                                descrption   Order by insert_date) rn
                FROM "DW_RAW".ref_theresanaiforthat_description
                )

                delete from "DW_RAW".ref_theresanaiforthat_description
                where (url_internal, insert_date)
                in (
                    with dup as (
                                SELECT url_internal as dupUrl, insert_date as dupDate
                                FROM dec
                                where rn >1
                                )
                    select * from dup
                    );
                """
        exeSql(sql, command)

    @task()
    def deleteUnchangedImpactedJobs():
        '''Delete new duplicated rows without any changes in Impacted Jobs'''
        from modules.dbExecute import exeSql

        command = 'Delete duplicated rows in ref_theresanaiforthat_impacted_jobs'
        sql =   """
                WITH dec AS (
                SELECT *,RANK() OVER(PARTITION BY url_internal,title, impact, count_tasks, count_ais    Order by insert_date) rn
                FROM "DW_RAW".ref_theresanaiforthat_impacted_jobs
                )

                delete from "DW_RAW".ref_theresanaiforthat_impacted_jobs
                where (url_internal, insert_date)
                in (
                    with dup as (
                                SELECT url_internal, insert_date
                                FROM dec
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
                delete from "DW_RAW".ref_theresanaiforthat_features_temp
                """
        exeSql(sql, command)


    init = initTables()
    ais = fetchExistingAis()
    newA = checkExistingAis(ais)
    newF = extractFullfeatures(newA)
    dp = deleteUnchangedProperties()
    da = deleteUnchangedAttributes()
    dd = deleteUnchangedDescrption()
    dj = deleteUnchangedImpactedJobs()
    cd = cleanTempDb()

    init>>ais>>newA>>newF>>[dp,da,dd,dj]>>cd

Int_theresanaiforthat()

