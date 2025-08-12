import time
from common.utils import get_personalize_client

def lambda_handler(event,context) -> dict:
    personalize_client = get_personalize_client()
    DSG_ARN = personalize_client.list_dataset_groups()['datasetGroups'][0]['datasetGroupArn']
    
    solution_obj = personalize_client.create_solution(
        name=f"coubee-solution-{int(time.time())}",
        recipeArn= "arn:aws:personalize:::recipe/aws-user-personalization-v2",
        datasetGroupArn=DSG_ARN,
    )
    solution_arn = solution_obj['solutionArn']
    while True:
        status = personalize_client.describe_solution(solutionArn=solution_arn)['solution']['status']
        print(f"Solution status: {status}")
        if status in ('ACTIVE', 'CREATE FAILED'):
            break
        time.sleep(30)  # 30초 간격으로 체크

    if status != 'ACTIVE':
        raise RuntimeError("Solution 생성 실패")
    
    sv_obj = personalize_client.create_solution_version(
        solutionArn=solution_arn,
        trainingMode='FULL'
    )
    return{
        "statusCode": 200,
        "body": "solution version 생성이 완료되었습니다."
    }