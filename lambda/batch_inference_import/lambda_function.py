import os
import time
import pandas as pd
from common.utils import get_personalize_client
from common.utils import get_s3_client
from common.utils import db_connection

def lambda_handler(event,context) -> dict:
    #S3 환경변수
    ROLE_ARN = os.environ.get('ROLE_ARN')
    USER_JSON_S3 = os.environ.get('USER_JSON_S3')
    OUT_JSON_S3 = os.environ.get('OUT_JSON_S3')
    BUCKET_NAME = os.environ.get('BUCKET_NAME')

    #DB 커넥션
    engine = db_connection()
    conn = None
    s3_client = None
    try:
        conn = engine.raw_connection()
        sql_query = "select cu.id from coubee_user.coubee_user as cu where cu.role = 'ROLE_USER'"
        df = pd.read_sql_query(sql_query, conn)
        df = df.rename(columns={'id': 'userId'})
        df['userId'] = df['userId'].astype(str)

        user_input_key = f"{USER_JSON_S3}/{int(time.time())}-user-input.json"
        user_input_url = f"s3://{BUCKET_NAME}/{user_input_key}"

        #Json lines 형식으로 변환 
        json_input = df.to_json(orient='records',lines= True)
        #S3 클라이언트 커넥션
        s3_client = get_s3_client()
        s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=user_input_key,
        Body=json_input.encode('utf-8')  # 문자열을 바이트로 인코딩
    )
    except Exception as e:
        raise Exception(f"유저 input 중 에러 발생: {str(e)}")
    finally:
        if conn:
            conn.close()
    #배치 추론
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
            numResults=10,
            batchInferenceJobConfig={
        "itemExplorationConfig": {
            "explorationWeight": "0.10",
            "explorationItemAgeCutOff": "30"
        }
    },
            jobInput= {"s3DataSource": {"path": user_input_url}}, 
            jobOutput= {"s3DataDestination": {"path": f"s3://{BUCKET_NAME}/{OUT_JSON_S3}/"}}
        )
    except Exception as e:
        raise Exception(f"배치 추론 작업 중 에러 발생: {str(e)}")

    return{
        "batchInferenceJobArn": response.get('batchInferenceJobArn')
    }