import pandas as pd
import os
import urllib
from datetime import datetime
from sqlalchemy import create_engine
from common.utils import get_s3_client

#lambda entry point함수
def lambda_handler(event, context) -> dict:
    BUCKET_NAME = os.environ.get('BUCKET_NAME')
    USER_URL = os.environ.get('USER_URL')
    INTERACTION_URL = os.environ.get('INTERACTION_URL')
    BASE_DIR = "/tmp"

    upload_file = []

    try:
        s3_client = get_s3_client()
        local_path= etl_user(BASE_DIR)
        s3_client.upload_file(
            local_path, BUCKET_NAME, USER_URL
        )
        upload_file.append(f"s3://{BUCKET_NAME}/{USER_URL}")

        local_path= etl_interaction(BASE_DIR)
        s3_client.upload_file(
            local_path, BUCKET_NAME, INTERACTION_URL
        )
        upload_file.append(f"s3://{BUCKET_NAME}/{INTERACTION_URL}")

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
def db_connection(DB_NAME: str):
    DB_USER = os.environ.get('DB_USER')
    DB_PASSWORD = os.environ.get('DB_PASSWORD')
    DB_HOST = os.environ.get('DB_HOST')
    DB_PORT = os.environ.get('DB_PORT')
    try:
        safe_password = urllib.parse.quote_plus(DB_PASSWORD)
        db_uri = f"postgresql://{DB_USER}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_uri)
        return engine
    except Exception as e:
        print(f"DB 연결 중 오류 발생: {str(e)}")
        raise

def etl_user(base_dir) -> str:
    engine = db_connection("coubee_user")
    conn = engine.raw_connection()
    sql_query = 'SELECT user_id, age, gender FROM coubee_user.user_info'
    try:
        df = pd.read_sql_query(sql_query,conn)
        new_columns = ['USER_ID', 'AGE', 'GENDER']
        df.columns = new_columns
        time = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_path = os.path.join(base_dir, f"{time}_user_data.csv")
        df.to_csv(local_path, index = False)
    finally:
        conn.close()

    return local_path

def etl_interaction(base_dir) -> str:
    product_engine = db_connection("coubee_product")
    order_engine = db_connection("coubee_order")

    product_conn = product_engine.raw_connection()
    order_conn = order_engine.raw_connection()

    sql_view_query = "SELECT user_id, product_id, unix_timestamp, view as event_type FROM coubee_product.product_view_record"
    sql_product_query = "select o.user_id, oi.product_id , o.unix_timestamp, oi.event_type from coubee_order.orders  as o " \
    "inner join coubee_order.order_items as oi on o.order_id = oi.order_id where status = 'PAYED'"
    try:
        # sql 테이블 view와 product 데이터 프레임 생성
        df_view = pd.read_sql_query(sql_view_query,product_conn)
        df_purchase = pd.read_sql_query(sql_product_query, order_conn)

        # Interaction 칼럼명 수정
        df_sum = pd.concat([df_view, df_purchase], ignore_index=True)
        new_columns = ['USER_ID', 'ITEM_ID' , 'TIMESTAMP', 'EVENT_TYPE']
        df_sum.columns = new_columns

        #csv파일로 저장
        time = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_path = os.path.join(base_dir, f"{time}_interaction_data.csv")
        df_sum.to_csv(local_path, index = False)
    finally:
        product_conn.close()
        order_conn.close()
    return local_path
