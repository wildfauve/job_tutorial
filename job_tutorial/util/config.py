from job_tutorial.util import env

JOB_URN_BASE = "urn:sparkjob:domainName:JobName"
TABLE_FORMAT = 'delta'
DATABASE_NAME = 'tutorialDomain'
DATABASE_PATH = "dbfs:/user/hive/warehouse/tutorialDomain.db"
TABLE1 = "tutorialTable1"
TABLE2 = "tutorialTable2"

config = {
    'env': env.Env().env,
    'table_format': 'delta',
    'database_name': DATABASE_NAME,
    'db_path': DATABASE_PATH,
    'tutorialTable1': {
        'table': TABLE1,
        'fully_qualified': f"{DATABASE_NAME}.{TABLE1}"
    },
    'tutorialTable2': {
        'table': TABLE2,
        'fully_qualified': f"{DATABASE_NAME}.{TABLE2}"
    }
}