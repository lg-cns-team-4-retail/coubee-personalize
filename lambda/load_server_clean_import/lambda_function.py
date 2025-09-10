import os
import time
from urllib.parse import urlparse
from sqlalchemy import create_engine, text, Table, MetaData, Column, Text, BIGINT
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.schema import CreateSchema
from common.utils import get_personalize_client
from common.utils import get_s3_client
from common.utils import db_connection
from collections import Counter, defaultdict
import math, json as _json


def lambda_handler(event,context) -> dict:
    personalize_client = get_personalize_client()
    s3_client = get_s3_client()

    BUCKET_NAME = os.environ.get('BUCKET_NAME')
    SCHEMA_NAME = os.environ.get('SCHEMA_NAME')
    FILE_NAME = os.environ.get('FILE_NAME')

    try:
        etl_res = etl_recommend(s3_client, BUCKET_NAME, FILE_NAME, SCHEMA_NAME)
        if etl_res.get("statusCode") != 200:
            print("ETL 작업 실패:", etl_res.get("body"))
            return etl_res
        clean_s3(s3_client, BUCKET_NAME, "user/")
        clean_s3(s3_client, BUCKET_NAME, "interaction/")
        clean_s3(s3_client, BUCKET_NAME, "user_input/")
        clean_s3(s3_client, BUCKET_NAME, "batch_result/")
        clean_up(personalize_client)
    except Exception as e:
        raise Exception(f"Dataset 삭제 실패: {str(e)}")
    return{
       "statusCode": 200,
       "body": "s3 import/db 저장 완료 및 celan-up 완료"
    }

def etl_recommend(s3, bucket_name, file_key, SCHEMA_NAME) -> dict:
    raw = defaultdict(list)

    # --- (A) 모든 .json.out 순회해서 후보 수집 ---
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=file_key)

    CAND_K = 300  # 후보 확장 (후처리용)
    for page in pages:
        for obj in page.get('Contents', []) if 'Contents' in page else []:
            key = obj['Key']
            if not key.endswith('.json.out'):
                continue
            body = s3.get_object(Bucket=bucket_name, Key=key)['Body']
            for line in body.iter_lines():
                if not line:
                    continue
                res = _json.loads(line)
                user_id = str(res['input']['userId'])
                out = res.get('output', {})

                # 포맷 방어: itemScores / recommendedItems / itemList 모두 처리
                if 'itemScores' in out:
                    items = out['itemScores']  # [{"itemId": "...", "score": 0.xx}, ...]
                elif 'recommendedItems' in out:
                    items = [{"itemId": i, "score": 1.0} for i in out['recommendedItems']]
                elif 'itemList' in out:
                    items = [{"itemId": i, "score": 1.0} for i in out['itemList']]
                else:
                    continue

                for it in items[:CAND_K]:
                    iid = str(it.get('itemId') or it)
                    sc  = float(it.get('score', 1.0))
                    raw[user_id].append({"itemId": iid, "score": sc})

    if not raw:
        print("S3 파일에서 처리할 데이터가 없습니다.")
        return {"statusCode": 200, "body": "처리할 데이터 없음"}

    # --- (B) 공동출현 기반 유사도 준비 ---
    global_counts = Counter()
    for items in raw.values():
        global_counts.update([x["itemId"] for x in items])

    co_counts = defaultdict(int)
    for items in raw.values():
        # 사용자 후보에서 중복 제거 후 상위 일부만 사용(연산량 제한)
        ids = []
        seen = set()
        for x in sorted(items, key=lambda z: z["score"], reverse=True):
            iid = x["itemId"]
            if iid in seen:
                continue
            seen.add(iid)
            ids.append(iid)
            if len(ids) >= 100:
                break
        for i in range(len(ids)):
            for j in range(i+1, len(ids)):
                a, b = ids[i], ids[j]
                if a > b:
                    a, b = b, a
                co_counts[(a, b)] += 1

    def sim(i, j):
        if i == j: 
            return 1.0
        a, b = (i, j) if i < j else (j, i)
        co = co_counts.get((a, b), 0)
        if co == 0: 
            return 0.0
        return co / math.sqrt(global_counts[i] * global_counts[j])

    # 인기도 페널티(공통 상위 붕괴 유도)
    POP_PENALTY = 0.02
    max_cnt = max(global_counts.values()) or 1
    pop_penalty = {iid: POP_PENALTY * (global_counts[iid] / max_cnt) for iid in global_counts}

    # --- (C) MMR re-rank ---
    def mmr_rank(items, topk=25, lam=0.8):
        # dedup + 인기도 페널티
        pool, seen = [], set()
        for it in sorted(items, key=lambda x: x["score"], reverse=True):
            iid = it["itemId"]
            if iid in seen: 
                continue
            seen.add(iid)
            pool.append({"itemId": iid, "score": it["score"] - pop_penalty.get(iid, 0.0)})

        selected = []
        while pool and len(selected) < topk:
            best_idx, best_val = None, -1e9
            for idx, cand in enumerate(pool[:300]):  # 탐색 폭 제한
                rel = cand["score"]
                if not selected:
                    val = rel
                else:
                    div = 0.0
                    for s in selected:
                        div = max(div, sim(cand["itemId"], s["itemId"]))
                    val = lam * rel - (1.0 - lam) * div
                if val > best_val:
                    best_val, best_idx = val, idx
            if best_idx is None:
                break
            chosen = pool.pop(best_idx)
            selected.append(chosen)
        return [x["itemId"] for x in selected]

    # --- (D) 최종 upsert payload 생성 (ranked 사용) ---
    insert_db = []
    for uid, items in raw.items():
        ranked = mmr_rank(items, topk=25, lam=0.8)
        try:
            uid_int = int(uid)
        except Exception:
            # user_id가 숫자가 아닐 경우 문자열로 저장하도록 스키마 변경 필요
            # 여기선 스킵 or 로깅
            continue
        ranked_as_int = [int(item_id) for item_id in ranked]
        insert_db.append({
            "user_id": uid_int,
            "recommend_items": _json.dumps(ranked_as_int)
        })

    # --- (E) DB upsert ---
    try:
        engine = db_connection()
        with engine.connect() as conn:
            if not conn.dialect.has_schema(conn, SCHEMA_NAME):
                conn.execute(CreateSchema(SCHEMA_NAME))

            metadata = MetaData()
            recommendations_table = Table(
                'item_recommend', metadata,
                Column('user_id', BIGINT, primary_key=True),
                Column('recommend_items', Text),
                schema=SCHEMA_NAME
            )
            metadata.create_all(conn)

            stmt = insert(recommendations_table)
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=['user_id'],
                set_=dict(recommend_items=stmt.excluded.recommend_items)
            )

            if insert_db:
                conn.execute(upsert_stmt, insert_db)
                conn.commit()
    except Exception as e:
        raise Exception(f"DB upsert 실패: {str(e)}")

    return {"statusCode": 200, "body": f"DB 저장 완료: {len(insert_db)} users"}

def clean_s3(s3_client, bucket_name, folder_name):
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_name)
        delete_us = dict(Objects=[])
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    delete_us['Objects'].append(dict(Key=obj['Key']))

                    # batch 단위로 삭제
                    if len(delete_us['Objects']) >= 1000:
                        s3_client.delete_objects(Bucket=bucket_name, Delete=delete_us)
                        delete_us = dict(Objects=[])

        # 남아있는 객체 삭제
        if len(delete_us['Objects']):
            s3_client.delete_objects(Bucket=bucket_name, Delete=delete_us)

        print(f"S3 정리 완료: s3://{bucket_name}/{folder_name}")
    except Exception as e:
        raise Exception(f"S3 clean-up 실패: {str(e)}")

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
        SOLUTIONS_LENGTH = len(SOLUTION_LIST)
        while SOLUTIONS_LENGTH > 0:
            print(f"WAITING FOR SOLUTIONS TO DELETE... :: {SOLUTIONS_LENGTH}")
            time.sleep(30)
            SOLUTIONS_LENGTH = len(personalize.list_solutions(datasetGroupArn=DSG_ARN)['solutions'])

        for idx, dataset in enumerate(DS_LIST): 
            response = personalize.delete_dataset( 
            datasetArn=dataset['datasetArn'] 
            )
    
        DS_LENGTH = 1
        while DS_LENGTH > 0: 
            print("WAIT DS TO DELETE... :: ", DS_LENGTH) 
            time.sleep(30) 
            DS_LENGTH = len(personalize.list_datasets(datasetGroupArn=DSG_ARN)['datasets']) 
        response = personalize.delete_dataset_group( datasetGroupArn=DSG_ARN)
    except Exception as e:
        raise Exception(f"Dataset 삭제 실패: {str(e)}")
    