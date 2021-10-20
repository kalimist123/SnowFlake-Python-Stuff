import boto3
import pandas


def delete_all_objects(bucket_name_to_delete):
    res = []
    bucket_to_delete = boto3.resource('s3', region_name='eu-west-1').Bucket(bucket_name_to_delete)
    if bucket_to_delete.creation_date:
        for obj_version in bucket_to_delete.object_versions.all():
            res.append({'Key': obj_version.object_key,
                        'VersionId': obj_version.id})
        if res:
            print(res)
            bucket_to_delete.delete_objects(Delete={'Objects': res})
        else:
            print('no files in bucket')



session = boto3.Session(profile_name='default')
client = session.client('s3', region_name='eu-west-1')
bucketname = 'python-s3-test-e10928b5'
# Fetch the list of existing buckets
clientResponse = client.list_buckets()

# Print the bucket names one by one
print('Printing bucket names before...')
for bucket in clientResponse['Buckets']:
    print(f'Bucket Name: {bucket["Name"]}')

# delete the bucket
delete_all_objects(bucketname)

# Creating a bucket in AWS S3
location = {'LocationConstraint': 'eu-west-1'}

bucket_to_delete = boto3.resource('s3', region_name='eu-west-1').Bucket(bucketname)
if not bucket_to_delete.creation_date:
    client.create_bucket(
        Bucket=bucketname,
        CreateBucketConfiguration=location
    )

clientResponse = client.list_buckets()

# Print the bucket names one by one
print('Printing bucket names after...')
for bucket in clientResponse['Buckets']:
    print(f'Bucket Name: {bucket["Name"]}')

res = []
boto3.resource('s3', region_name='eu-west-1').meta.client.upload_file( 'test1.csv', bucketname, 'test1.csv')
for obj_version in bucket_to_delete.object_versions.all():
    res.append({'Key': obj_version.object_key,
                'VersionId': obj_version.id})
if res:
    print(res)
else:
    print('no files in bucket')