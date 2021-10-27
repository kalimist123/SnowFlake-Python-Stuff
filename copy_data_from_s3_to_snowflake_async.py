import os
import snowflake.connector
import boto3
import math
import asyncio
import concurrent.futures
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from timeit import default_timer as timer
import time

load_dotenv()


# fetched file names from sub-folder in s3 bucket
def fetch_file_paths_from_subfolder(bucket_name, subfolder_name, profile_name):
    # initialize session for the specified aws profile
    session = boto3.Session(profile_name=profile_name)
    # initialize s3 resource
    s3 = session.resource('s3')
    # get s3 bucket for specified bucket name
    bucket = s3.Bucket(bucket_name)

    # check if bucket exists
    try:
        s3.meta.client.head_bucket(Bucket=bucket.name)
    except ClientError as err:
        print("bucket '" + bucket_name + "' does not exist or access denied.")
        print(err)

    # fetch all file paths from specified subfolder
    file_names = []
    obj_count = 0
    for obj in bucket.objects.filter(Prefix=subfolder_name + "/"):
        obj_count = obj_count + 1
        path, filename = os.path.split(obj.key)
        if filename:
            file_names.append(filename)

    # check if subfolder exists
    if obj_count == 0:
        raise ValueError("subfolder '" + subfolder_name + "' does not exist.")

    # check if any files exist within the subfolder
    if len(file_names) == 0:
        raise ValueError("subfolder '" + subfolder_name + "' does not have any files.")

    return file_names


# divides file names into batches based on the number of threads
def divide_file_names_into_batches(file_names, num_threads):
    # calculate batch size
    batch_size = math.ceil(len(file_names) / num_threads)

    # divide file names into batches
    file_names_batches = []
    file_name_index_counter = 0
    for _ in range(num_threads):
        temp_file_names = []
        loop_range = file_name_index_counter + batch_size
        if loop_range > len(file_names):
            loop_range = len(file_names)

        for x in range(file_name_index_counter, loop_range):
            temp_file_names.append(file_names[x])
            file_name_index_counter = file_name_index_counter + 1

        file_names_batches.append(temp_file_names)

    return file_names_batches


# builds SQL copy command for each batch of file names
def build_sql_copy_command_for_each_bach_of_file_names(file_names_batches, table, stage):
    sql_commands = []
    for file_names in file_names_batches:
        file_names_delimited = "', '".join(file_names)
        sql = (f"copy into {table} "
               f"from {stage} "
               "file_format = (type = json) "
               f"files = ('{file_names_delimited}');")

        sql_commands.append(sql)

    return sql_commands


# ***
# *** main ***
bucket_name = 'sisense-mvp'
subfolder_name = 'test-folder'
aws_profile = 'gmail-profile'
num_threads = 3
destination_table = 'json_table'
database = 'DEMO_DB'
stage = '@rocketship_external_stage_json'

print('S3 bucket name: ' + bucket_name)
print('S3 sub-folder name: ' + subfolder_name)
print('AWS profile: ' + aws_profile)
print('Number of async threads: ' + str(num_threads))
print('Snowflake destination table: ' + destination_table)
print('Snowflake stage: ' + stage)
print()

# fetch file names from sub-folder in s3 bucket
file_names = fetch_file_paths_from_subfolder(bucket_name, subfolder_name, aws_profile)

print('List of file names fetched from sub-folder in s3 bucket')
for file_name in file_names:
    print(file_name)
print()

# divide file names into batches based on the number of threads
file_names_batches = divide_file_names_into_batches(file_names, num_threads)

print('List of file names divided into batches')
print(file_names_batches)
print()

# build SQL copy command for each batch of file names
sql_copy_commands = build_sql_copy_command_for_each_bach_of_file_names(file_names_batches, destination_table, stage)

print('List of sql copy commands for each batch of file names')
print("\n".join(sql_copy_commands))
print()

# *** copy data from s3 to snowflake

# initialise snowflake connection
ctx = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),  # Your Email ID
    account=os.getenv('ACCOUNT'),
    password=os.getenv('PASSWORD'),
    region=os.getenv('REGION'),  # This could vary based on location
)

cur = ctx.cursor()  # Query data
cnx = ctx

try:
    cnx.cursor().execute("USE " + database)  # Select the database to query in
    cnx.cursor().execute("USE ROLE \"" + os.getenv('ROLE') + "\"")  # Select role to query in

    print('Connection details')
    print('(User, Role, Database, Warehouse, Schema)')
    cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE() ,CURRENT_DATABASE(),CURRENT_WAREHOUSE(),CURRENT_SCHEMA();")
    for (col1) in cur:
        print('{0}'.format(col1))
    print()

    # Truncate destination table if it exists
    print('Truncate destination table if it exists...')
    cnx.cursor().execute("truncate table if exists " + destination_table + ";")
    print('Truncate done')
    print()

    # Execute copy commands asynchronously
    print('Copy data in parallel from s3 to snowflake destination table...')
    start = timer()
    queries = dict()
    for sql_command in sql_copy_commands:
        cur = ctx.cursor()
        cur.execute_async(sql_command)
        queries[cur.sfqid] = sql_command

    # Wait until all commands have executed and print each command when it completes
    completed_query_ids = []
    while len(completed_query_ids) < len(queries):
        for key, value in queries.items():
            if not ctx.is_still_running(ctx.get_query_status(key)) and key not in completed_query_ids:
                completed_query_ids.append(key)
                print("The following command has successfully completed: \n", value)
            time.sleep(1)
    print()

    elapsed = (timer() - start)
    print("Copy data done and took: {} seconds".format(elapsed))
    print()

    print("Number of rows copied:")
    cur.execute("select count(1) from " + destination_table + ";")
    for (col1) in cur:
        print(col1[0])
    print()

finally:
    cnx.close()
