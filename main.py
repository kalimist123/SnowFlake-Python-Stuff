import snowflake.connector

#Connection Details : SSO

ctx = snowflake.connector.connect(
	user='mlennon',#Your Email ID
	account='sd76050',
    password='Dundalk09!',
   region='eu-west-1', # This could vary based on location

)

cur = ctx.cursor()#Query data
cnx=ctx

try:
    cnx.cursor().execute("USE SNOWFLAKE_SAMPLE_DATA")  # Select the database to query in
    cur.execute("SELECT * FROM WEATHER.DAILY_14_TOTAL limit 1")
    for (col1, col2) in cur:
        print('{0}, {1}'.format(col1, col2))
finally:
    cnx.close()