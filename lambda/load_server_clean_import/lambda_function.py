import os
import time
import json
from sqlalchemy import create_engine, text, Table, MetaData, Column, Text, BIGINT
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.schema import CreateSchema
from common.utils import get_personalize_client
from common.utils import get_s3_client
from common.utils import db_connection


def lambda_handler(event,context) -> dict:
    personalize_client = get_personalize_client()
    s3_client = get_s3_client()
    BUCKET_NAME = os.environ.get('OUTPUT_FOLDER')
    FILE_NAME = os.environ.get('FILE_KEY')
    
    try:
        etl_res = etl_recommend(s3_client, BUCKET_NAME, FILE_NAME)
        if etl_res.get("statusCode") != 200:
            print("ETL 작업 실패:", etl_res.get("body"))
            return etl_res
        clean_up(personalize_client)
    except Exception as e:
       return{
          "statusCode": 500,
          "body": f"cleanup 실패 : {str(e)}"
       }
    return{
       "statusCode": 200,
       "body": "s3 import/db 저장 완료 및 celan-up 완료"
    }

def etl_recommend(s3,bucket_name, file_key):
    recommendations_to_save = []
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    #  각 줄을 json line으로 변환시킴
    for line in response['Body'].iter_lines():
        res = json.loads(line)
        user_id = res['input']['userId']
        recommended_items = res['output']['recommendedItems']
        parsed_items = [int(item) for item in recommended_items]
        recommendations_to_save.append({
            "userId": user_id,
            "recommended_items" : parsed_items
        })
    if not recommendations_to_save:
        print("S3 파일에서 처리할 데이터가 없습니다.")
        return {"statusCode": 200, "body": "처리할 데이터 없음"}
    try:
        #DB에 저장할 형태로 가공시킴
        insert_db = [
            {
            "user_id": data["userId"],
            #   json 직렬화
            "recommend_items": json.dumps(data["recommended_items"])
            }
            for data in recommendations_to_save
        ]
        #Postgresql DB 연결 설정
        engine = db_connection()
        SCHEMA_NAME = "coubee_product"
        with engine.connect() as conn:
            #테이블 스키마 정의
            if not conn.dialect.has_schema(conn, SCHEMA_NAME):
                conn.execute(CreateSchema(SCHEMA_NAME))
                conn.commit()
            metadata = MetaData()
            recommendations_table = Table('item_recommend', metadata,
                                                Column('user_id', BIGINT, primary_key=True),
                                                Column('recommend_items', Text),
                                                schema=SCHEMA_NAME
                                            )
            metadata.create_all(conn)
            # upsert 실행
            stmt = insert(recommendations_table)

            # 2. recommend_items
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=['user_id'],  # 중복을 확인할 고유 키 컬럼
                set_=dict(recommend_items=stmt.excluded.recommend_items) # 충돌 시 업데이트할 값
            )

            conn.execute(upsert_stmt, insert_db)
            #트랜잭션 커밋
            conn.commit()
    except Exception as e:
        return{
            "statusCode": 500,
            "body": f"DB 저장 실패: {str(e)}"
        }
    return{
            "statusCode": 200,
            "body": f"DB 저장 완료"
        }

# AWS 위에 Personalize 관련 인스턴스 모두 삭제
def clean_up(personalize) -> None: 
    DSG_ARN = personalize.list_dataset_groups()['datasetGroups'][0]['datasetGroupArn'] 
    SOLUTION_LIST = personalize.list_solutions( datasetGroupArn=DSG_ARN )['solutions'] 
    DS_LIST = personalize.list_datasets( datasetGroupArn=DSG_ARN )['datasets']
    try:
        for idx, solution in enumerate(SOLUTION_LIST): 
            response = personalize.delete_solution( 
            solutionArn=solution['solutionArn'] 
            )

        for idx, dataset in enumerate(DS_LIST): 
            response = personalize.delete_dataset( 
            datasetArn=dataset['datasetArn'] 
            )
    
        DS_LENGTH = 1
        while DS_LENGTH > 0: 
            print("WAIT DS TO DELETE... :: ", DS_LENGTH) 
            time.sleep(30) 
            DS_LENGTH = len(personalize.list_datasets(datasetGroupArn=DSG_ARN)['datasets']) 
            response = personalize.delete_dataset_group( datasetGroupArn=DSG_ARN ) 
            print("DSG CLEAN UP")
    except Exception as e:
        raise f"Dataset 삭제 실패: {str(e)}"