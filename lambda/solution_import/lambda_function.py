import time
from common.utils import get_personalize_client

def lambda_handler(event,context) -> dict:
    personalize_client = get_personalize_client()
    # Step Functions로부터 데이터셋 그룹 ARN을 전달받음
    dsg_arn = event['datasetGroupArn']
    
    #Solution 생성 시작
    solution_response = personalize_client.create_solution(
        name="coubee-solution",
        recipeArn="arn:aws:personalize:::recipe/aws-user-personalization-v2",
        datasetGroupArn=dsg_arn,
    )
    solution_arn = solution_response['solutionArn']
    
    #Solution Version 생성 시작
    sv_response = personalize_client.create_solution_version(
        solutionArn=solution_arn,
        trainingMode='FULL'
    )
    solution_version_arn = sv_response['solutionVersionArn']

    #Step Functions에 필요한 정보 반환 후 즉시 종료
    return {
        "solutionVersionArn": solution_version_arn
    }