import os
import boto3
from botocore.exceptions import ClientError

def fetch_file_paths_from_subfolder(bucket_name, subfolder_name,  profile_name):
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
        print("bucket '"+bucket_name+"' does not exist or access denied.")
        print(err)

    # fetch all file paths from specified subfolder
    file_paths = []
    objCount=0
    for obj in bucket.objects.filter(Prefix=subfolder_name + "/"):
        objCount=objCount+1
        path, filename = os.path.split(obj.key)
        if filename:
            file_paths.append(obj.key)

    # check if subfolder exists
    if objCount == 0:
        raise ValueError("subfolder '" + subfolder_name + "' does not exist.")

    # check if any files exist within the subfolder
    if len(file_paths) == 0:
        raise ValueError("subfolder '" + subfolder_name + "' does not have any files.")

    return file_paths


# execute function
file_paths = fetch_file_paths_from_subfolder('sisense-mvp', 'test-folder', 'gmail-profile')
for file_path in file_paths:
    print(file_path)

