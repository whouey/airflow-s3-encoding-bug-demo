import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator
import logging

def _s3hook_readkey(aws_conn_id, bucket, key):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    s3 = S3Hook(aws_conn_id = aws_conn_id)
    file_content = s3.read_key(key, bucket)

    logging.info(file_content)

def _s3hook_getkey(aws_conn_id, bucket, key):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    s3 = S3Hook(aws_conn_id = aws_conn_id)
    file_content = s3.get_key(key, bucket).get()['Body'].read().decode('big5')

    logging.info(file_content)
    
def _s3hook_selectkey(aws_conn_id, bucket, key):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    s3 = S3Hook(aws_conn_id = aws_conn_id)
    file_content = s3.select_key(key, bucket)

    logging.info(file_content)

with DAG(
    "s3_big5",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval=None
) as dag:

    s3hook_readkey = PythonOperator(
        task_id = 's3hook_readkey',
        python_callable = _s3hook_readkey,
        op_kwargs = {
            'aws_conn_id': 'aws_default',
            'bucket': 'testbig5src',
            'key': 'big5_file.txt',
        }
    )

    s3hook_getkey = PythonOperator(
        task_id = 's3hook_getkey',
        python_callable = _s3hook_getkey,
        op_kwargs = {
            'aws_conn_id': 'aws_default',
            'bucket': 'testbig5src',
            'key': 'big5_file.txt',
        }
    )

    s3hook_selectkey = PythonOperator(
        task_id = 's3hook_selectkey',
        python_callable = _s3hook_selectkey,
        op_kwargs = {
            'aws_conn_id': 'aws_default',
            'bucket': 'testbig5src',
            'key': 'big5_csv.csv',
        }
    )

    # transfer without transform
    s3filetransformoperator = S3FileTransformOperator(
        task_id = 's3filetransformoperator',
        source_s3_key = 's3://testbig5src/big5_file.txt',
        dest_s3_key = 's3://testbig5dst/big5_file.txt',
        select_expression = 'SELECT * FROM S3Object',
        replace = True,
    )
