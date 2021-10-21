import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

ctx = snowflake.connector.connect(
    user=os.getenv('USER'),  # Your Email ID
    account=os.getenv('ACCOUNT'),
    password=os.getenv('PASSWORD'),
    region=os.getenv('REGION'),  # This could vary based on location

)

cur = ctx.cursor()  # Query data
cnx = ctx

try:
    cnx.cursor().execute("USE SNOWFLAKE_SAMPLE_DATA")  # Select the database to query in
    cur.execute("SELECT * FROM WEATHER.DAILY_14_TOTAL limit 1")
    for (col1, col2) in cur:
        print('{0}, {1}'.format(col1, col2))
finally:
    cnx.close()
