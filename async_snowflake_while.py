from datetime import datetime
import snowflake.connector
import os
from snowflake.connector import ProgrammingError
from time import sleep

from dotenv import load_dotenv

load_dotenv()

MY_ENV_VAR = os.getenv('MY_ENV_VAR')
print(f' results1 {MY_ENV_VAR}')

ctx = snowflake.connector.connect(
    user=os.getenv('USER'),  # Your Email ID
    account=os.getenv('ACCOUNT'),
    password=os.getenv('PASSWORD'),
    region=os.getenv('REGION'),  # This could vary based on location

)
cur1 = ctx.cursor()  # Query data


# Execute a long-running query asynchronously.
cur1.execute_async('select count(*) from table(generator(timeLimit => 35))')
query_id = cur1.sfqid
cur1.get_results_from_sfqid(query_id)



try:
    while ctx.is_still_running(ctx.get_query_status(query_id)):
        t = datetime.now().strftime('%#I:%M:%S%p')
        print(f"{t}: {ctx.get_query_status(query_id).name}")
        sleep(2)
except ProgrammingError as err:
    print(f"Programming Error: {err}")
finally:
    t = datetime.now().strftime('%#I:%M:%S%p')
    print(f"{t}: {ctx.get_query_status(query_id).name}")