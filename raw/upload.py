import logging
import boto3
from botocore.exceptions import ClientError
import os
import datetime

source_diretory = "*****"
s3_bucket = "*******"
s3_prefix = "*******"

s3 = boto3.client("s3")

today = datetime.date.today()
year = today.strftime('%Y')
month = today.strftime('%m')
day = today.strftime('%d')

files_to_upload = ["movies.csv", "series.csv"]
folders = ["Movies", "Series"]

for file_name in files_to_upload:
    folder_name = os.path.splitext(file_name)[0].capitalize()
    s3_key = f"{s3_prefix}/{folders}/{datetime.now().strftime('%Y/%m/%d')}/{file_name}"

    s3.upload_file(os.path.join(source_diretory,file_name), s3_bucket, s3_key)

print(f"Arquivo {file_name} carregado com sucesso para o S3.")