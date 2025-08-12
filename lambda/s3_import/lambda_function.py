import pandas as pd
import os
import urllib.parse
from datetime import datetime
from common.utils import get_s3_client
from common.utils import db_connection

#lambda entry point함수
def lambda_handler(event, context) -> dict:
    BUCKET_NAME = os.environ.get('BUCKET_NAME')
    BASE_DIR = "/tmp"

    upload_file = []

    try:
        s3_client = get_s3_client()
        local_path, s3_path = etl_user(BASE_DIR)
        s3_client.upload_file(
            local_path, BUCKET_NAME, s3_path
        )
        upload_file.append(f"s3://{s3_path}")

        local_path, s3_path = etl_interaction(BASE_DIR)
        s3_client.upload_file(
            local_path, BUCKET_NAME, s3_path
        )
        upload_file.append(f"s3://{BUCKET_NAME}/{s3_path}")

        return {
            "statusCode": 200,
            "body": {
                "message": "ETL 작업이 완료되었습니다.",
                "uploadFile": upload_file
            }
        }
    
    except Exception as e:
        print(f"업로드 중 에러 발생: {e}")
        return{
            "statusCode": 500,
            "body": {
                "message": str(e)
        }
        }

def etl_user(base_dir) -> str:
    engine = db_connection()

    # TODO read_csv말고 sql_table읽어서 csv파일로 변경 후 저장해야됨
    #df = pd.read_sql_table("user_info",engine, schema="coubee_user")
    df = pd.read_csv("C:/final/coubee-etl/airflow-docker/user_dataset.csv", encoding='utf-8')#utf-8
    df_selected = df.reset_index()[['USER_ID','AGE','GENDER']]

    time = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_path = os.path.join(base_dir, f"{time}_user_data.csv")
    df_selected.to_csv(local_path, index = False)
    
    s3_path = f"user/{time}user_data.csv"

    return local_path, s3_path

def etl_interaction(base_dir) -> str:
    engine = db_connection()

    # TODO read_csv말고 sql_table읽어서 csv파일로 변경 후 저장해야됨
    #df = pd.read_sql_table("user_info",engine, schema="coubee_user")
    df = pd.read_csv("C:/final/coubee-etl/airflow-docker/user_dataset.csv", encoding='utf-8')#utf-8
    df_selected = df.reset_index()[['USER_ID', 'ITEM_ID', 'TIMESTAMP', 'EVENT_TYPE']]

    time = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_path = os.path.join(base_dir, f"{time}_interaction_data.csv")
    df_selected.to_csv(local_path, index = False)

    s3_path = f"interaction/{time}interaction_data.csv"

    return local_path, s3_path
