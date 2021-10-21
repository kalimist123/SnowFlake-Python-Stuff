import boto3
import os


session = boto3.Session(profile_name='default')
client = session.client('s3', region_name='eu-west-1')
bucketname = ''
# Fetch the list of existing buckets
clientResponse = client.list_buckets()

# Print the bucket names one by one
print('Printing bucket names before...')
for bucket in clientResponse['Buckets']:
    print(f'Bucket Name: {bucket["Name"]}')

s3 = session.resource('s3')
bucket_better = s3.Bucket(bucketname)

res = []
for obj_version in bucket_better.object_versions.all():
    res.append({'Key': obj_version.object_key,
                'VersionId': obj_version.id})
if res:
    print(res)
else:
    print('no files in bucket')

for object_summary in bucket_better.objects.filter(Prefix="files-for-processing/"):
    print(object_summary.key)




