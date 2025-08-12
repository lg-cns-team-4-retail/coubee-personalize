import boto3
import os
import traceback
import urllib
from sqlalchemy import create_engine
from botocore.exceptions import ClientError


#Personalize client 생성
def get_personalize_client():
    return boto3.client('personalize', region_name='ap-northeast-2')

#S3 클라이언트 생성
def get_s3_client():
    return boto3.client("s3")

def extract_arn_id(arn: str) -> str:
    arn_id = arn.split('/')[-1]
    return arn_id

def read_s3_json():
    return 0

#DB 커넥션
def db_connection():
    DB_USER = os.environ.get('DB_USER')
    DB_PASSWORD = os.environ.get('DB_PASSWORD')
    DB_HOST = os.environ.get('DB_HOST')
    DB_PORT = os.environ.get('DB_PORT')
    DB_NAME = os.environ.get('DB_NAME')
    # TODO: DB 커넥션
    try:
        safe_password = urllib.parse.quote_plus(DB_PASSWORD)
        db_uri = f"postgresql://{DB_USER}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_uri)
        return engine
    except Exception as e:
        print(f"오류 발생: {str(e)}")
        raise


#Personalize상태 받아오는 로직
def parse_personalize_event(event: dict) -> dict:
    try:
        detail: dict = event.get('detail',{})
        status = detail.get('status')

        event_type = None
        resource_arn = None

        if "batchInferenceJobArn" in detail:
            event_type = "BatchInference Create"
            resource_arn = detail["batchInferenceJobArn"]

        elif "solutionVersionArn" in detail:
            event_type = "SolutionCreate"
            resource_arn = detail["solutionVersionArn"]

        elif "datasetImportJobArn" in detail:
            event_type = 'DatasetImportJob'
            resource_arn = detail["datasetImportJobArn"]

        return{
            "event_type": event_type,
            "arn": resource_arn,
            "status": status
        }
    except Exception as e:
        print("파싱중 에러 발생")
        print(traceback.format_exc)
        return {
            "event_type": None,
            "arn": None,
            "status": None
        }