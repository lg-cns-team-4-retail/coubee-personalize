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
        local_file_name = os.path.basename(local_path)
        s3_client.upload_file(
            local_path, BUCKET_NAME, f"{USER_URL}/{local_file_name}"
        )
        upload_file.append(f"s3://{BUCKET_NAME}/{USER_URL}/")

        local_path= etl_interaction(BASE_DIR)
        local_file_name = os.path.basename(local_path)
        s3_client.upload_file(
            local_path, BUCKET_NAME, f"{INTERACTION_URL}/{local_file_name}"
        )
        upload_file.append(f"s3://{BUCKET_NAME}/{INTERACTION_URL}/")

        return {
            "body": {
                "message": "ETL 작업이 완료되었습니다.",
                "uploadFile": upload_file
            }
        }
    
    except Exception as e:
        raise Exception(f"업로드 중 에러 발생: {e}")
    
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
    conn = None
    try:
        conn = engine.raw_connection()
        sql_query = 'SELECT user_id, age, gender FROM coubee_user.user_info'
        df = pd.read_sql_query(sql_query,conn)
        new_columns = ['USER_ID', 'AGE', 'GENDER']
        df.columns = new_columns
        time = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_path = os.path.join(base_dir, f"{time}_user_data.csv")
        df.to_csv(local_path, index = False)
    except Exception as e:
        print(f"DB 작업 중 에러 발생: {str(e)}")
    finally:
        if conn:
            conn.close()

    return local_path

def etl_interaction(base_dir) -> str:
    product_engine = db_connection("coubee_product")
    order_engine = db_connection("coubee_order")
    product_conn = None
    order_conn = None
    try:
        product_conn = product_engine.raw_connection()
        order_conn = order_engine.raw_connection()

        sql_view_query = "SELECT user_id, product_id, unix_timestamp, event_type FROM coubee_product.product_view_record where user_id > 0"

        sql_product_query = """
                    SELECT o.user_id, oi.product_id, o.paid_at_unix, oi.event_type
                    FROM coubee_order.orders AS o
                    INNER JOIN coubee_order.order_items AS oi 
                        ON o.order_id = oi.order_id
                    WHERE o.status NOT IN ('CANCELLED_ADMIN', 'CANCELLED_USER', 'PENDING')
                    AND oi.event_type = 'PURCHASE'
                    AND o.paid_at_unix >= 1756354800;
        """
        
        #View 행동 데이터 수집 
        df_view = pd.read_sql_query(sql_view_query,product_conn)
        df_view['event_type']= df_view['event_type'].str.upper()

        #Purchase 행동 데이터 수집
        df_purchase = pd.read_sql_query(sql_product_query, order_conn)

        #view,purchase 행동 데이터 칼럼 명 변경 
        new_columns = ['USER_ID', 'ITEM_ID' , 'TIMESTAMP', 'EVENT_TYPE']
        df_view.columns = new_columns
        df_purchase.columns = new_columns

        # Interaction 칼럼명 수정
        df_sum = pd.concat([df_view, df_purchase], ignore_index=True)
        # 합쳐진 interaction 데이터셋 timestamp기준으로 오름차순 정렬
        df_sorted = df_sum.sort_values(by='TIMESTAMP', ascending=True)

        #csv파일로 저장
        time = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_path = os.path.join(base_dir, f"{time}_interaction_data.csv")
        df_sorted.to_csv(local_path, index = False)
    except Exception as e:
        print(f"Interaction 작업 중 에러 발생: {str(e)}")
    finally:
        if product_conn:
            product_conn.close()
        if order_conn:
            order_conn.close()
    return local_path