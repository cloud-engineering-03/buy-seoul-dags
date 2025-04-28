
import os
from dotenv import load_dotenv
import requests
import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from airflow import DAG
import numpy as np
from sqlalchemy.engine import make_url
import psycopg2
from psycopg2.extras import execute_values
from airflow.models import Variable







def fetch_raw_data(**context):
    # API 요청 + DataFrame 생성 + XCom push
    api_key = Variable.get("SEOUL_API_KEY")
    print(api_key)
    # 기본 설정 변수
    service_name = "tbLnOpendataRtmsV"  # API 서비스명 (거래 데이터)
    base_url = "http://openapi.seoul.go.kr:8088"
    start_index = 30
    end_index = 50

    # 오늘 날짜 기준 (예: 20250411 → 202504 형식)
    today = datetime.today()
    yyyymm = today.strftime("%Y%m")

    # 요청 URL 구성
    url = f"{base_url}/{api_key}/json/{service_name}/{start_index}/{end_index}/"
    params = {
        "DEAL_YMD": yyyymm  # 거래년월 필터
    }

    # 요청 실행
    response = requests.get(url, params=params)

    # 결과 처리
    if response.status_code == 200:
        print(response)
        data = response.json()
        items = data.get(service_name, {}).get("row", [])
        df = pd.DataFrame(items)
        print(df.head())
    else:
        print(f"요청 실패: {response.status_code}")
    df.head()

    column_mapping = {"RTRCN_DAY":"취소일","LAND_AREA":"토지면적(㎡)","STDG_CD":"법정동코드","BLDG_NM":"건물명","STDG_NM":"법정동명","MNO":"본번","THING_AMT":"물건금액(만원)","LOTNO_SE_NM":"지번구분명","LOTNO_SE":"지번구분","CTRT_DAY":"계약일","RCPT_YR":"접수연도","OPBIZ_RESTAGNT_SGG_NM":"신고한 개업공인중개사 시군구명","ARCH_AREA":"건물면적(㎡)","CGG_CD":"자치구코드","RGHT_SE":"권리구분","SNO":"부번","FLR":"층","CGG_NM":"자치구명","BLDG_USG":"건물용도","ARCH_YR":"건축년도","DCLR_SE":"신고구분"}

    # 'DATA' 부분을 DataFrame으로 변환
    df.rename(columns=column_mapping, inplace=True)
    context['ti'].xcom_push(key='raw_df', value=df.to_json())

def validate_data():
    # df 불러와 유효성 검사
    ...

def insert_data(**context):
    ti = context["ti"]
    raw_json = ti.xcom_pull(task_ids='fetch_raw_data', key='raw_df')
    df = pd.read_json(raw_json)

    # 전처리
    # 빈 문자열, 공백, 'null' 문자열을 None으로 변환
    df = df.drop(columns=["자치구명"], errors="ignore")
    df = df.drop(columns=["법정동명"], errors="ignore")
    for col in ["계약일", "취소일"]:
        df[col] = df[col].replace(['', ' ', 'null', 'N/A'], None)
        df[col] = df[col].apply(lambda x: pd.to_datetime(x, format='%Y%m%d', errors='coerce') if x is not None else None)
        df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)

    # 전역 None/결측 처리
    df = df.replace([pd.NaT,'', ' ', 'null', 'N/A'], None)

    print(df.head(10))

    # DB 접속 정보
    db_url = "postgresql+psycopg2://postgres:postgres@airflow-postgresql.airflow:5432/postgres"
    url = make_url(db_url)
    conn = psycopg2.connect(
        dbname=url.database,
        user=url.username,
        password=url.password,
        host=url.host,
        port=url.port
    )

    bulk_insert_data(df, "부동산데이터", conn, chunk_size=100)

def prune_old_data():
    db_url = "postgresql+psycopg2://postgres:postgres@airflow-postgresql.airflow:5432/postgres"
    engine = create_engine(db_url)

    cutoff = (datetime.today() - timedelta(days=3)).strftime("%Y%m%d")

    with engine.connect() as conn:
        conn.execute(text("""
            DELETE FROM 부동산데이터
            WHERE "계약일" < :cutoff
        """), {"cutoff": cutoff})

def t5_check_table():
    db_url = "postgresql+psycopg2://postgres:postgres@airflow-postgresql.airflow:5432/postgres"
    engine = create_engine(db_url)

    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM 부동산데이터", conn)
        print(df.to_string(index=False))
        
def bulk_insert_data(df, table_name, conn, chunk_size=100):
    cur = conn.cursor()

    columns = list(df.columns)
    quoted_columns = [f'"{col}"' for col in columns]

    insert_prefix = f"INSERT INTO {table_name} ({', '.join(quoted_columns)}) VALUES "

    success_count = 0
    fail_count = 0
    buffer = []

    for idx, row in df.iterrows():
        values = []
        for col in columns:
            val = row.get(col, None)
            if pd.isna(val):
                values.append('NULL')
            elif isinstance(val, str):
                val = val.replace("'", "''")
                values.append(f"'{val}'")
            elif isinstance(val, pd.Timestamp):
                values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
            else:
                values.append(str(val))

        buffer.append(f"({', '.join(values)})")

        # chunk_size마다 insert
        if (idx + 1) % chunk_size == 0:
            try:
                sql = insert_prefix + ",\n".join(buffer) + ";"

                print(sql)
                
                cur.execute(sql)
                print(cur.fetchall())

                conn.commit()
                print(f"[✅ {idx+1}개] 삽입 성공")
                success_count += len(buffer)
            except Exception as e:
                conn.rollback()
                print(f"[❌ {idx+1}개] 삽입 실패: {e}")
                fail_count += len(buffer)
            buffer = []
        # print(sql)
    # 남은 row 처리
    if buffer:
        try:
            sql = insert_prefix + ",\n".join(buffer) + ";"
            cur.execute(sql)
            conn.commit()
            print(f"[✅ 남은 {len(buffer)}개] 삽입 성공")
            success_count += len(buffer)
        except Exception as e:
            conn.rollback()
            print(f"[❌ 남은 {len(buffer)}개] 삽입 실패: {e}")
            fail_count += len(buffer)

    cur.close()
    conn.close()

    print(f"총 성공: {success_count} / 실패: {fail_count}")