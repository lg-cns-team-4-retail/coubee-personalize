import os
import time
from common.utils import get_personalize_client
from common.utils import get_s3_client

def lambda_handler(event,context) -> dict:
    #TODO postgresql 테이블 연결 필요
    # df = pd.read_sql_table("테이블 이름") 
    # # 여기에서도 위의 etl_user 함수에서 해준 것처럼 동일한 type으로 전처리 해줘야합니다. 
    # # 그 결과를 json 형태로 만들어서 S3에 업로드 해주어야 합니다.

    #TODO input item data를 JSON형태로 만들어서 S3에 업로드 /batch_inference로 할 예정
    # out = user_df.to_json(orient='records')[1:-1].replace('},{', '}\n{') 
    # s3 = boto3.resource('s3') s3.Object('your destination', 'path/to/batch_input.json').put(Body=out)

    ROLE_ARN = os.environ.get('ROLE_ARN')
    USER_JSON_S3 = os.environ.get('USER_JSON_S3')
    OUT_JSON_S3 = os.environ.get('OUT_JSON_S3')

    personalize_client = get_personalize_client()

    # 가장 최근 DatasetGroup 찾기
    all_dataset_groups = personalize_client.list_dataset_groups().get('datasetGroups', [])
    if not all_dataset_groups:
        raise RuntimeError("사용 가능한 데이터세트 그룹이 없습니다.")
    

    # 생성날짜 기준 lambda사용해서 최근 1개만 가져옴
    latest_dsg = sorted(all_dataset_groups, key=lambda x: x['creationDateTime'], reverse=True)[0]
    DSG_ARN = latest_dsg['datasetGroupArn']

    # 생성된 solution 찾기
    all_solutions = personalize_client.list_solutions(datasetGroupArn=DSG_ARN).get('solutions', [])
    if not all_solutions:
        raise RuntimeError(f"데이터세트 그룹 {DSG_ARN}에 해당하는 솔루션이 없습니다.")
    
    # ACTIVE된 solution 찾기
    active_solutions = [s for s in all_solutions if s['status'] == 'ACTIVE']
    if not active_solutions:
        raise RuntimeError(f"데이터세트 그룹 {DSG_ARN}에서 ACTIVE 상태의 솔루션이 없습니다.")
    
    # ACTIVE된 Solution 중 가장 최근에 생성된 solution 찾기
    latest_solution = sorted(active_solutions, key=lambda x: x['creationDateTime'], reverse=True)[0]
    SOLUTION_ARN = latest_solution['solutionArn']


    # 가장 최근 솔루션에서 모든 솔루션 버전 찾기
    all_solution_versions = personalize_client.list_solution_versions(solutionArn=SOLUTION_ARN).get('solutionVersions', [])
    if not all_solution_versions:
        raise RuntimeError(f"솔루션 {SOLUTION_ARN}에 해당하는 버전이 없습니다.")
    
    #ACTIVE된 Solution 버전 찾기
    active_solution_versions = [sv for sv in all_solution_versions if sv['status'] == 'ACTIVE']
    if not active_solution_versions:
        raise RuntimeError(f"솔루션 {SOLUTION_ARN}에서 ACTIVE 상태의 버전이 없습니다.")
    
    # 가장 최근의 solution version 찾기
    latest_solution_version = sorted(active_solution_versions, key=lambda x: x['creationDateTime'], reverse=True)[0]
    SV_ARN = latest_solution_version['solutionVersionArn']

    try:
        response = personalize_client.create_batch_inference_job( 
            solutionVersionArn=SV_ARN, 
            jobName=f"recommendation-batch-{int(time.time())}", 
            roleArn= ROLE_ARN, 
            jobInput= {"s3DataSource": {"path": USER_JSON_S3}}, 
            jobOutput= {"s3DataDestination": {"path": OUT_JSON_S3}}, 
            numResults=10, 
        )
    except Exception as e:
        return{
            "statusCode": 500,
            "body": f"배치 추론 작업 생성 중 에러 발생: {e}"
        }

    return{
        "statusCode": 200,
        "body": "배치 추론 작업이 성공적으로 시작되었습니다."
    }