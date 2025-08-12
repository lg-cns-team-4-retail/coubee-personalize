import os
import time
import datetime
from common.utils import get_personalize_client

def lambda_handler(event, context):
    INTERACTION_S3_PATH = os.environ.get('INTERACTION_S3_PATH')
    USER_S3_PATH = os.environ.get('USER_S3_PATH')
    ROLE_ARN = os.environ.get('ROLE_ARN')

    current_dir = os.path.dirname(os.path.realpath(__file__))

    # 2. 현재 디렉터리 경로와 'schema' 폴더 이름을 합쳐 schema 폴더의 전체 경로를 만듭니다.
    #    결과: /var/task/schema
    schema_dir_path = os.path.join(current_dir, 'schema')

    dataset_import_list = create_dataset(schema_dir_path, INTERACTION_S3_PATH, USER_S3_PATH, ROLE_ARN)

    return {
        "status": 200,
        "body":{
            "message": "dataset import 작업이 완료되었습니다",
            "data": dataset_import_list
        }
    }


def create_dataset(schema_dir, INTERACTION_S3_PATH, USER_S3_PATH, ROLE_ARN) -> list:

    personalize_client = get_personalize_client()

    dtime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    dataset_import_list = []

    #Dataset 생성
    response = personalize_client.create_dataset_group(name = f'{dtime}coubee-dataset', domain = "ECOMMERCE")
    dsg_arn = response['datasetGroupArn'] 
    description = personalize_client.describe_dataset_group(datasetGroupArn=dsg_arn)['datasetGroup']
    print('1. Name: ' + description['name']) 
    print('1. ARN: ' + description['datasetGroupArn']) 
    print('1. Status: ' + description['status'])
    time.sleep(5) 

    #interaction_schema 생성
    interaction_schema_path = os.path.join(schema_dir,'interaction_schema.json')
    with open(interaction_schema_path) as f: 
      dtime = dtime
      createSchemaResponse = personalize_client.create_schema(
        name=f'coubee-interaction-{dtime}-schema', 
        schema=f.read(),
        domain='ECOMMERCE'
         )
    interaction_schema_arn = createSchemaResponse['schemaArn'] 
    print("1-1. INTERACTION SCHEMA :: ", interaction_schema_arn)
    
    #user_schema 생성
    user_schema_path = os.path.join(schema_dir,'user_schema.json')
    with open(user_schema_path) as f: 
      dtime = dtime
      createSchemaResponse = personalize_client.create_schema( 
        name=f'coubee-user-{dtime}-schema', 
        schema=f.read(),
        domain='ECOMMERCE')
    user_schema_arn = createSchemaResponse['schemaArn'] 
    print("1-1. USER SCHEMA :: ", user_schema_arn)

    #Dataset 생성
    DSG_STATUS = '' 
    while DSG_STATUS != 'ACTIVE': 
      print("wait DSG to created... ::", DSG_STATUS) 
      time.sleep(30) 
      DSG_STATUS = personalize_client.describe_dataset_group( datasetGroupArn=dsg_arn )['datasetGroup']['status']
    
    #interatcion_dataset 생성
    i_response = personalize_client.create_dataset( 
      name=f'interaction-{int(time.time())}', 
      schemaArn=interaction_schema_arn, 
      datasetGroupArn=dsg_arn, 
      datasetType='interactions'
    ) 
    print('2. Interaction Dataset Arn: ' + i_response['datasetArn']) 
    interaction_ds_arn = i_response['datasetArn']

    #User_dataset생성 
    u_response = personalize_client.create_dataset( 
      name=f'user-{int(time.time())}', 
      schemaArn=user_schema_arn, 
      datasetGroupArn=dsg_arn, 
      datasetType='users'
    )
    print('2. User Dataset Arn: ' + u_response['datasetArn'])
    user_ds_arn = u_response['datasetArn']

    while True:
      interaction_response = personalize_client.describe_dataset(datasetArn=interaction_ds_arn)
      user_response = personalize_client.describe_dataset(datasetArn=user_ds_arn)
      interaction_status = interaction_response['dataset']['status']
      user_status = user_response['dataset']['status']
      print(f"Interactions dataset status: {interaction_status}{user_status}")
      if interaction_status == 'ACTIVE' and user_status == 'ACTIVE':
          break
      elif interaction_status == 'CREATE FAILED' or user_status == 'CREATE FAILED':
          print(f"Dataset creation failed with reason: {interaction_response['dataset'].get('failureReason', 'Unknown')}")
          raise Exception('Interactions dataset creation failed')
      time.sleep(20)
    
    #interaction-import-job 생성
    response = personalize_client.create_dataset_import_job( 
      jobName=f'interaction-import-{int(time.time())}', 
      datasetArn=interaction_ds_arn, 
      dataSource={'dataLocation': INTERACTION_S3_PATH}, 
      # TODO S3권한 가진 ROLE생성 필요
      roleArn= ROLE_ARN
    )
    interaction_dsij_arn = response['datasetImportJobArn'] 
    dataset_import_list.append(interaction_dsij_arn)
    print('3. Interaction Dataset Import Job arn: ' + interaction_dsij_arn)

    #user-import-job 생성
    response = personalize_client.create_dataset_import_job( 
      jobName=f'user-import-{int(time.time())}', 
      datasetArn=user_ds_arn, 
      dataSource={'dataLocation': USER_S3_PATH}, 
      # TODO S3권한 가진 ROLE생성 필요
      roleArn= ROLE_ARN
    ) 
    user_dsij_arn = response['datasetImportJobArn'] 
    dataset_import_list.append(user_dsij_arn)
    print('3. User Dataset Import Job arn: ' + user_dsij_arn)

    return dataset_import_list