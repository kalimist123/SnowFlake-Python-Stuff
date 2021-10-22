import os
import boto3
import math
from botocore.exceptions import ClientError


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


# *** main
bucket_name = 'sisense-mvp'
subfolder_name = 'test-folder'
aws_profile = 'gmail-profile'
num_threads = 3

print('S3 bucket name: ' + bucket_name)
print('S3 sub-folder name: ' + subfolder_name)
print('AWS profile: ' + aws_profile)
print('Number of async threads: ' + str(num_threads))
print()

# fetch file names from sub-folder in s3 bucket
file_names = fetch_file_paths_from_subfolder(bucket_name, subfolder_name, aws_profile)

print('List of file names fetched from sub-folder in s3 bucket')
for file_name in file_names:
    print(file_name)
print()

# divide file names into batches based on the number of threads
file_names_batches = divide_file_names_into_batches(file_names, num_threads)

print(file_names_batches)
