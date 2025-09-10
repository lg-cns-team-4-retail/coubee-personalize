import os
import time
import datetime
from common.utils import get_personalize_client

def lambda_handler(event, context):
    USER_S3_PATH = event['body']['uploadFile'][0]
    INTERACTION_S3_PATH = event['body']['uploadFile'][1]
    ROLE_ARN = os.environ.get('ROLE_ARN')

    current_dir = os.path.dirname(os.path.realpath(__file__))
    schema_dir_path = os.path.join(current_dir, 'schema')

    dataset_import_list = create_dataset(schema_dir_path, INTERACTION_S3_PATH, USER_S3_PATH, ROLE_ARN)
    
    if dataset_import_list:
        return dataset_import_list
    else:
        return {
            "body": {
                "message": "데이터셋 생성 중 에러 발생했습니다.",
                "data": []
            }
        }


def create_dataset(schema_dir, INTERACTION_S3_PATH, USER_S3_PATH, ROLE_ARN) -> dict:
    personalize_client = get_personalize_client()
    dtime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    dataset_import_list = []

    DSG_NAME = 'coubee-main2'
    dsg_arn = None

    # Dataset Group 생성
    try:
        response = personalize_client.create_dataset_group(name=DSG_NAME, domain="ECOMMERCE")
        dsg_arn = response['datasetGroupArn']
        description = personalize_client.describe_dataset_group(datasetGroupArn=dsg_arn)['datasetGroup']
        print('1. Name: ' + description['name'])
        print('1. ARN: ' + description['datasetGroupArn'])
        print('1. Status: ' + description['status'])
        time.sleep(15)
    except Exception as e:
        return []

    # Interaction Schema 생성
    interaction_schema_path = os.path.join(schema_dir, 'interaction_schema.json')
    with open(interaction_schema_path) as f:
        createSchemaResponse = personalize_client.create_schema(
            name=f'coubee-interaction-{dtime}-schema',
            schema=f.read(),
            domain='ECOMMERCE'
        )
    interaction_schema_arn = createSchemaResponse['schemaArn']
    print("1-1. INTERACTION SCHEMA :: ", interaction_schema_arn)

    # User Schema 생성
    user_schema_path = os.path.join(schema_dir, 'user_schema.json')
    with open(user_schema_path) as f:
        createSchemaResponse = personalize_client.create_schema(
            name=f'coubee-user-{dtime}-schema',
            schema=f.read(),
            domain='ECOMMERCE'
        )
    user_schema_arn = createSchemaResponse['schemaArn']
    print("1-1. USER SCHEMA :: ", user_schema_arn)

    # Dataset Group 활성화 대기
    DSG_STATUS = ''
    while DSG_STATUS != 'ACTIVE':
        print("wait DSG to created... ::", DSG_STATUS)
        time.sleep(10)
        DSG_STATUS = personalize_client.describe_dataset_group(datasetGroupArn=dsg_arn)['datasetGroup']['status']

    # Interaction Dataset 생성
    i_response = personalize_client.create_dataset(
        name=f'interaction-{int(time.time())}',
        schemaArn=interaction_schema_arn,
        datasetGroupArn=dsg_arn,
        datasetType='interactions'
    )
    interaction_ds_arn = i_response['datasetArn']
    print('2. Interaction Dataset Arn: ' + interaction_ds_arn)

    # User Dataset 생성
    u_response = personalize_client.create_dataset(
        name=f'user-{int(time.time())}',
        schemaArn=user_schema_arn,
        datasetGroupArn=dsg_arn,
        datasetType='users'
    )
    user_ds_arn = u_response['datasetArn']
    print('2. User Dataset Arn: ' + user_ds_arn)

    while True:
        interaction_status = personalize_client.describe_dataset(datasetArn=interaction_ds_arn)['dataset']['status']
        user_status = personalize_client.describe_dataset(datasetArn=user_ds_arn)['dataset']['status']
        print(f"Interactions dataset status: {interaction_status}, User dataset status: {user_status}")

        if interaction_status == 'ACTIVE' and user_status == 'ACTIVE':
            break
        elif interaction_status == 'CREATE FAILED' or user_status == 'CREATE FAILED':
            print(f"Dataset creation failed with reason: {interaction_status}, {user_status}")
            raise Exception('Dataset creation failed')
        time.sleep(10)
    

    # Interaction Import Job 생성
    response = personalize_client.create_dataset_import_job(
        jobName=f'interaction-import-{int(time.time())}',
        datasetArn=interaction_ds_arn,
        dataSource={'dataLocation': INTERACTION_S3_PATH},
        roleArn=ROLE_ARN
    )
    interaction_dsij_arn = response['datasetImportJobArn']
    print('3. Interaction Dataset Import Job arn: ' + interaction_dsij_arn)

    # User Import Job 생성
    response = personalize_client.create_dataset_import_job(
        jobName=f'user-import-{int(time.time())}',
        datasetArn=user_ds_arn,
        dataSource={'dataLocation': USER_S3_PATH},
        roleArn=ROLE_ARN
    )
    user_dsij_arn = response['datasetImportJobArn']
    print('3. User Dataset Import Job arn: ' + user_dsij_arn)

    return {
        "datasetGroupArn": dsg_arn,
        "interactionDatasetImportJobArn": interaction_dsij_arn,
        "userDatasetImportJobArn": user_dsij_arn
    }