import boto3, json , pprint, requests, textwrap, time, logging
import os, os.path
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
#from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta, date
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
import pyarrow.parquet as pq
import numpy as np
import pandas as pd 
from airflow.exceptions import AirflowException
from airflow.models import DagRun
import s3fs,decimal,time
from s3path import S3Path
import pyarrow as pa

region_name = 'ap-southeast-2'

def client(region_name):
    global emr
    emr = boto3.client('emr', region_name='ap-southeast-2')
    return emr

def get_security_group_id(group_name, region_name):
    ec2 = boto3.client('ec2', region_name=region_name)
    response = ec2.describe_security_groups(GroupNames=[group_name])
    return response['SecurityGroups'][0]['GroupId']

s3 = boto3.resource('s3')

def reads_config(key = 'new_config.json'):
    response = s3.Object('atul-landingbucket-batch08',key)
    data=response.get()['Body'].read().decode('utf-8')
    appdata=json.loads(data)
    return appdata

def ec2_client(region_name):
    global ec2
    ec2 = boto3.client('ec2', region_name='ap-southeast-2')
    return ec2

def create_cluster(region_name, cluster_name='Atul_Cluster_ec' + str(datetime.now()), release_label='emr-6.10.0',master_instance_type='m5.xlarge',**kwargs):
    emr_master_security_group_id = get_security_group_id('ElasticMapReduce-master', region_name=region_name)
    emr_slave_security_group_id= emr_master_security_group_id
    emr  = client('ap-southeast-2')
    ti =kwargs['ti']
    ec2 = ec2_client('ap-southeast-2')
    response = ec2.describe_subnets()
    subnet_val = response['Subnets'][1]['SubnetId']
    data = reads_config('new_config.json')
    cluster_response = emr.run_job_flow(
    Name=cluster_name,
    ReleaseLabel=release_label,
    Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': master_instance_type,
                    'InstanceCount': 1,
                }

            ],
            'Ec2SubnetId': subnet_val,
            'KeepJobFlowAliveWhenNoSteps': True,
            'Ec2KeyName' : 'at_new_key_pair',
            'EmrManagedMasterSecurityGroup': emr_master_security_group_id,
            'EmrManagedSlaveSecurityGroup': emr_slave_security_group_id
        },
		
        AutoTerminationPolicy = {'IdleTimeout': 3600},	
        VisibleToAllUsers=True,
        JobFlowRole= "EMR_EC2_DefaultRole",
        ServiceRole= "EMR_DefaultRole",
        LogUri='s3://atul-landingbucket-batch08/',
        #Configurations=['s3://atul-landingbucket-batch08/new_config.json'],
        Configurations=  data,
        Applications = [
            { 'Name': 'Hadoop' },
            { 'Name': 'Spark' },
            { 'Name': 'Hive' },
            { 'Name': 'Livy' },
            { 'Name': 'JupyterEnterpriseGateway'},
            { 'Name': 'JupyterHub'},
            { 'Name': 'Pig'},
	        { 'Name': 'Hue'},
	    ],
         			)
    return cluster_response['JobFlowId']


def get_cluster_dns(cluster_id):
    emr  = client('ap-southeast-2')
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['MasterPublicDnsName']


def wait_for_cluster_creation(cluster_id):
    emr  = client('ap-southeast-2')
    emr.get_waiter('cluster_running').wait(ClusterId=cluster_id)

'''

def livy_task(master_dns,emr_spark_job,dataset):
    host = 'http://' + master_dns + ':8998'
    data = {"file":emr_spark_job, "className":"com.example.SparkApp","args":[dataset]}
    headers = {'Content-Type': 'application/json'}
    response =requests.post(host+ '/batches', data=json.dumps(data), headers=headers)
    logging.info(response.json())
    return response.json()
'''

def livy_task(master_dns,emr_spark_job,dataset,spark_config,dataset_path):
    host = 'http://' + master_dns + ':8998'
    data = {"file":emr_spark_job, "className":"com.example.SparkApp","args":[dataset,spark_config,dataset_path]}
    headers = {'Content-Type': 'application/json'}
    response =requests.post(host+ '/batches', data=json.dumps(data), headers=headers)
    logging.info(response.json())
    return response.json()

def terminate_cluster(**kwargs):
    emr  = client('ap-southeast-2')
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.terminate_job_flows(JobFlowIds=[cluster_id])
    




default_args = {'owner': 'administer',
		'start_date': datetime(2022, 3, 4),
		'retries': 0,
		'retry_delay': timedelta(minutes=5)
}
# Initialize the DAG
dag = DAG('Test_jobs', default_args=default_args,description='Spark_job',schedule_interval=None, catchup=False)
#client(region_name='ap-southeast-2')

s4 = boto3.resource('s3')


def read_config(app_config):
    response = s4.Object('atul-landingbucket-batch08',app_config)
    data=response.get()['Body'].read().decode('utf-8')
    appdata=json.loads(data)
       
    if appdata:
        return appdata
    else:
        raise ValueError("Config file not present in S3 location")


s3 = boto3.resource('s3')
s5 = boto3.resource('s3')

'''def copy_data(**context):
    ti =context['ti']
    va= context['dag_run'].conf.get('dataset')
    va = va.split("/")[1]
    data = ti.xcom_pull(task_ids='read_config')
    if(va == 'actives.parquet'):
        copy_source = {
                'Bucket': data['ingest-dataset']['source']['data-location'].split("/")[2],
                'Key': "dataset/"+"actives.parquet"
            }

        dest = data['ingest-dataset']['destination']['data-location'].split("/")[2]
        bucket = s3.Bucket(dest)
        data_p = "Dataset/"+"actives.parquet"
        bucket.copy(copy_source, data_p)
    else:
        copy_source_v = {
                     'Bucket': data['ingest-dataset']['source']['data-location'].split("/")[2],
                    'Key': "dataset/"+"viewership.parquet"
                }

        dest_v = data['ingest-dataset']['destination']['data-location'].split("/")[2]
        bucket_v = s5.Bucket(dest_v)
        data_p_v = "Dataset/"+"viewership.parquet"
        bucket_v.copy(copy_source_v, data_p_v)
'''
def copy_data(**context):
    ti =context['ti']
    va= context['dag_run'].conf.get('dataset')
    va = va.split("/")[1]
    data = ti.xcom_pull(task_ids='read_config')
    if(va == 'actives.parquet' or va == 'viewership.parquet'):
        if(va == 'actives.parquet'):
        copy_source = {
                'Bucket': data['ingest-dataset']['source']['data-location'].split("/")[2],
                'Key': "new_dataset/"+va
            }

        dest = data['ingest-dataset']['destination']['data-location'].split("/")[2]
        bucket = s3.Bucket(dest)
        data_p = "new_raw_file/"+va
        bucket.copy(copy_source, data_p)
        else:
            copy_source_v = {
                     'Bucket': data['ingest-dataset']['source']['data-location'].split("/")[2],
                     'Key': "new_dataset/"+va
                }

            dest_v = data['ingest-dataset']['destination']['data-location'].split("/")[2]
            bucket_v = s5.Bucket(dest_v)
            data_p_v = "new_raw_file/"+va
            bucket_v.copy(copy_source_v, data_p_v)
    else:
        raise ValueError("Data is  not copied to raw zone")



def pre_validation(**context):
    ti =context['ti']
    va= context['dag_run'].conf.get('dataset')
    va = va.split("/")[1]

    data = ti.xcom_pull(task_ids='read_config')
    
    s3 = s3fs.S3FileSystem()
    
    src_bucket = data['ingest-dataset']['source']['data-location'].split("/")[2]
    dest_bucket =  data['ingest-dataset']['destination']['data-location'].split("/")[2]
    
    landing_path = 's3://'+src_bucket+'/'+"new_dataset/"+va
    raw_path = 's3://'+dest_bucket+'/'+"new_raw_file/"+va
    
    df_landingzone=pq.ParquetDataset(landing_path, filesystem=s3).read_pandas().to_pandas()
    df_rawzone=pq.ParquetDataset(raw_path, filesystem=s3).read_pandas().to_pandas()
   #print(df_landingzone)
   #print("\n")
   #print("\n")
   #print(df_rawzone)
    if df_rawzone[df_rawzone.columns[0]].count()!= 0:
        for raw_columnname in df_rawzone.columns:
            if df_rawzone[raw_columnname].count() == df_landingzone[raw_columnname].count():
                print('Count satisfied ',str(raw_columnname))
            else:
                print("Count different")
                return ValueError("Count ", str(raw_columnname))
    else:
        raise ValueError("No Data Available")
    return va        



'''
def livy_spark_submit(emr_spark_job,**context):
    # ti is the Task Instance
    ti = context['ti']
    va= context['dag_run'].conf.get('dataset')
    dataset = va.split("/")[1]
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    wait_for_cluster_creation(cluster_id)
    cluster_dns = get_cluster_dns(cluster_id)
    host = 'http://' + cluster_dns + ':8998'
    headers = {'Content-Type': 'application/json'}
    response = livy_task(cluster_dns,emr_spark_job,dataset)
    batch_id = response['id']
    batch_status = response['state']
    if batch_status == 'starting':
        print("Livy started and submitted using livy")
    else:
        print("Livy not implemented")
    while batch_status not in ['success', 'dead', 'killed', 'failed']:
        response = requests.get(host + '/batches/' + str(batch_id), headers=headers)
        batch_status = response.json()['state']
        #print(batch_status)
        time.sleep(10)

    if batch_status == 'success':
        print("Livy job completed successfully")
    else:
        print("Livy job failed or was killed")
'''

def livy_spark_submit(emr_spark_job,dataset,spark_config,dataset_path,**context):
    # ti is the Task Instance
    ti = context['ti']
    va= context['dag_run'].conf.get('dataset')
    dataset = va.split("/")[1]
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    wait_for_cluster_creation(cluster_id)
    cluster_dns = get_cluster_dns(cluster_id)
    host = 'http://' + cluster_dns + ':8998'
    headers = {'Content-Type': 'application/json'}
    response = livy_task(cluster_dns,emr_spark_job,dataset,spark_config,dataset_path)
    batch_id = response['id']
    batch_status = response['state']
    #print(response)
    #print("\n")
    if batch_status == 'starting':
        print("Livy started and submitted using livy")
    else:
        print("Livy not implemented")
    while batch_status not in ['success', 'dead', 'killed', 'failed']:
        response = requests.get(host + '/batches/' + str(batch_id), headers=headers)
        batch_status = response.json()['state']
        time.sleep(10)

    if batch_status == 'success':
        print("Livy job completed successfully")
    else:
        print("Livy job failed or was killed")


def terminate_emr(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    terminate_cluster(cluster_id)


def post_validation(**context):
    ti=context['ti']
    data = ti.xcom_pull(task_ids='read_config')
    va= context['dag_run'].conf.get('dataset')
    va = va.split("/")[1]
    s3f = s3fs.S3FileSystem()


    srci_bucket =  data['ingest-dataset']['destination']['data-location'].split("/")[2]
    desti_bucket = data['transformation-dataset']['destination']['data-location'].split("/")[2]

    raws_path = 's3://'+srci_bucket+'/'+"new_raw_file/"+va
    stagings_path ='s3://'+desti_bucket+'/'+'Transformed_Datasets/'+va

    df_rawzone=pq.ParquetDataset(raws_path,filesystem=s3f).read_pandas().to_pandas()
    df_stagingzone = pq.ParquetDataset(stagings_path,use_legacy_dataset=False).read_pandas().to_pandas()

    if df_stagingzone[df_stagingzone.columns[0]].count()!= 0:
        for raw_columnname in df_rawzone.columns:
            if (df_stagingzone[raw_columnname].count() == df_rawzone[raw_columnname].count()):
                print('Count matched',str(raw_columnname))
            else:
                print("count not matched")
                return ValueError("Count ", str(raw_columnname))
    else:
        raise ValueError("No Data Available")
    print("\n")
    print("\n")
    s3_1=s3fs.S3FileSystem()
    s3_2=s3fs.S3FileSystem()

    source_path = data["ingest-dataset"]["destination"]["data-location"]
    destination_path=data["transformation-dataset"]["destination"]["data-location"]

    dataset_obtained_path = va
    raw_path=source_path+dataset_obtained_path

    raw_data = pq.ParquetDataset(raw_path,filesystem=s3_1).read_pandas().to_pandas()
    staging_path=destination_path+dataset_obtained_path+"/"
    staging_data1=pq.ParquetDataset(staging_path)


    schema = staging_data1.schema
    for field in schema:
        if field.name=="location_source":
            if field.type=="string":
                print(f"the transformation for {field.name} is successfull")
            else:
                print(f"the transformation for {field.name} is not successfull")
        if field.name=="user_lattitude" or field.name=="user_longitude":
            if isinstance(field.type, pa.Decimal128Type):
                precision=field.type.scale
                if precision==7:
                    print(f"the {field.name} column is of DecimalType(7) data type")
                else:
                    print(f"The {field.name}  column is not of DecimalType(7) data type")


read_config=PythonOperator(
task_id='read_config',
python_callable=read_config,
op_kwargs={'app_config': 'updated_app config file.json'},
dag=dag)
    


copy_data=PythonOperator(
    task_id='copy_data',
    python_callable=copy_data,
    dag=dag,provide_context = True)

pre_validation=PythonOperator(
    task_id='pre_validation',
    python_callable=pre_validation,
    dag=dag,provide_context = True)


create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_cluster,
    op_kwargs={'region_name':'ap-southeast-2', 'cluster_name':'Atul_Cluster_ec'},
    dag=dag)
'''
spark_job_submit = PythonOperator(
    task_id='spark_job_submit',
    python_callable=livy_spark_submit,
    op_kwargs={'emr_spark_job':'s3://atul-landingbucket-batch08/pyspark_job.py'},
    dag=dag,provide_context = True)
'''
spark_job_submit = PythonOperator(
    task_id='spark_job_submit',
    python_callable=livy_spark_submit,
    op_kwargs={'emr_spark_job':'s3://atul-landingbucket-batch08/pyspark_job.py', 'dataset':'actives/viewership', 'spark_config':'new_config.json','dataset_path':'s3://atul-landingbucket-batch08/new_dataset/'},
    dag=dag,provide_context = True)

terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_cluster,
    trigger_rule='all_success',
    dag=dag)


post_validation=PythonOperator(
    task_id='post_validation',
    python_callable=post_validation,
    dag=dag,provide_context = True)

read_config>>copy_data>>pre_validation>>create_cluster>>spark_job_submit>>terminate_cluster>>post_validation
