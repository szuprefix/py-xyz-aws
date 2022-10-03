# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals
import boto3
from botocore.config import Config

from .utils import get_setting

A = lambda c: get_setting('S3', c)
SECRET_ID = A('SECRET_ID')
SECRET_KEY = A('SECRET_KEY')
REGION = A('REGION') or "us-west-2"
BUCKET = A('BUCKET')


def gen_signature(key, secret_id=SECRET_ID, secret_key=SECRET_KEY, expire=300,
                  bucket=BUCKET):
        s3 = boto3.client('s3',
                                 aws_access_key_id=secret_id,
                                 aws_secret_access_key=secret_key,
                                 config=Config(signature_version='s3v4'),
                                 region_name=REGION)
        url = s3.generate_presigned_url(
            ClientMethod='put_object',
            Params={
                'Bucket': bucket,
                'Key': key,
                'ACL': 'public-read'
            },
            ExpiresIn=expire
        )
        return dict(bucket= bucket, region=REGION, url = url)
