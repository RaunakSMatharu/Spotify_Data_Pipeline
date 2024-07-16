from airflow import DAG

from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
#from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

from datetime import datetime, timedelta



#define args
default_args = {
    "owner": "raunak",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 13),
}

dag=DAG(
    dag_id="spotify_trigger_external",
    default_args=default_args,
    description="DAG to Trigger Lambda Function and Check S3 Upload",
    schedule_interval=timedelta(days=1),
    catchup=False
)


trigger_extract_lambda= LambdaInvokeFunctionOperator(
    task_id="trigger_extract_lambda",
    function_name="spotify_api_data_extract",
    aws_conn_id="aws_spotify_conn",
    region_name="us-east-1",
    dag=dag,
)

check_s3_upload =S3KeySensor(
    task_id ='check_s3_upload',
    bucket_key='s3://spotify-etl-project-raunak/raw_data/to_processed/*.json',
    wildcard_match=True ,
    aws_conn_id='aws_spotify_conn',
    timeout=60*60,#'wait for 1 hour',
    poke_interval=60,
    dag=dag,

)

trigger_transform_glue = GlueJobOperator(
    task_id="trigger_transform_load_Glue",
    job_name="Spotify_Spark_transformation",
    script_location="s3://aws-glue-assets-044928725652-us-east-1/scripts/",
    aws_conn_id="aws_spotify_conn",
    region_name="us-east-1",
    iam_role_name='Glue_spotify_IAM_Role',  # Correct argument name
    s3_bucket="aws-glue-assets-044928725652-us-east-1",
    dag=dag 
)



trigger_extract_lambda>>check_s3_upload>>trigger_transform_glue



# trigger_transform_lambda= LambdaInvokeFunctionOperator(
#     task_id="trigger_transform_load_lambda",
#     function_name="spotify_transformation_load_function",
#     aws_conn_id="aws_spotify_conn",
#     region_name="us-east-1",
#     dag=dag,
# )
# trigger_extract_lambda>>check_s3_upload>>trigger_transform_lambda

