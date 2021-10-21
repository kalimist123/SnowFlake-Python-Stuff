import snowflake.connector
import os
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
cur2 = ctx.cursor()  # Query data
cur3 = ctx.cursor()  # Query data
cnx = ctx

try:

    cur1.execute_async('select count(*) from table(generator(timeLimit => 15))')

    query1_id = cur1.sfqid
    cur2.execute_async('select count(*) from table(generator(timeLimit => 10))')

    query2_id = cur2.sfqid
    cur3.execute_async('select count(*) from table(generator(timeLimit => 5))')
    query3_id = cur3.sfqid


    cur1.get_results_from_sfqid(query1_id)
    results1 = cur1.fetchall()

    cur2.get_results_from_sfqid(query2_id)
    results2 = cur2.fetchall()
    cur3.get_results_from_sfqid(query3_id)
    results3 = cur3.fetchall()

    print(f' results1 {results1[0]}')
    print(f' results2 {results2[0]}')
    print(f' results3 {results3[0]}')
finally:
    cnx.close()
