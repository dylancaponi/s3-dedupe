# create csv with all bucket objects key, etag, size
# create csv with bucket objects deduped by etag, size, filename

import os
from datetime import datetime

import boto3
import pandas as pd


def extract_data():

    client = boto3.client('s3', region_name=REGION)

    for bucket in BUCKETS:
        print(bucket)

        # create paginator
        paginator = client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket)

        # pull data
        df = pd.DataFrame()
        for page in page_iterator:
            df = df.append(page['Contents'], ignore_index=True)
            print(df.shape[0])

        # save data
        df.to_csv(f's3-list-{bucket}.csv')


def find_dupes():
    # load data
    df = pd.DataFrame()
    for bucket in BUCKETS:
        df = df.append(pd.read_csv(f's3-list-{bucket}.csv'))

    # extract key
    df['Filename'] = df['Key'].apply(lambda x: x.split('/')[-1:][0])
    print(df.shape)
    group_df = df.groupby(['ETag','Size','Filename'], as_index=False).size().sort_values(ascending=False)
    group_df['Bucket'] = bucket
    size = group_df.shape[0]
    print(size)

    date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    group_df.to_csv(f'{date}-dupes-{size}rows.csv', header=True)


REGION = 'us-east-1'
BUCKETS = ['bucket-name']

extract_data()
find_dupes()