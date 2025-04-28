
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
    start_index = 1
    end_index = 500

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
    # XCom에서 JSON 불러오기
    ti = context["ti"]
    raw_json = context['ti'].xcom_pull(task_ids='fetch_raw_data', key='raw_df')
    df = pd.read_json(raw_json)

    # 전처리
    # 빈 문자열, 공백, 'null' 문자열을 None으로 변환
    df["계약일"] = pd.to_datetime(df["계약일"], format="%Y%m%d", errors="coerce")
    df["취소일"] = pd.to_datetime(df["취소일"], format="%Y%m%d", errors="coerce")
    df = df.drop(columns=["자치구명", "법정동명"], errors="ignore")

    df["층"] = df["층"].replace(['', ' ', 'null', 'N/A'], np.nan).astype("Int64")

    # NaT → None 변환
    df['계약일'].replace({pd.NaT: None})
    df['취소일'].replace({pd.NaT: None})

    # 전역 None/결측 처리
    df = df.replace([pd.NaT,'', ' ', 'null', 'N/A'], None)
    # df = df.where(pd.notnull(df), None)

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
    cur = conn.cursor()

    columns = [
        "자치구코드", "법정동코드", "계약일", "취소일", "지번구분", "지번구분명",
        "본번", "부번", "건물명", "층", "건물면적(㎡)", "토지면적(㎡)", "물건금액(만원)",
        "건물용도", "건축년도", "신고구분", "권리구분", "접수연도", "신고한 개업공인중개사 시군구명"
    ]

    insert_sql = f"""
        INSERT INTO 부동산데이터 ({', '.join(f'"{col}"' for col in columns)})
        VALUES ({', '.join(['%s'] * len(columns))})
    """

    success_count = 0
    fail_count = 0

    for i, row in df.iterrows():
        values = [
        None if pd.isna(row.get(col, None)) else row.get(col, None)
        for col in columns
    ]
        sql_to_print = cur.mogrify(insert_sql, values).decode('utf-8')
        print(f"[SQL Preview] {sql_to_print}")

        print(values)
        try:
            conn.execute(insert_sql, values)
            success_count += 1
            print(f"[✅ row {i}] 삽입 성공")
        except Exception as e:
            conn.rollback()
            fail_count += 1
            print(f"[❌ row {i}] 삽입 실패: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"[결과 요약] 성공: {success_count}, 실패: {fail_count}, 전체: {len(df)}")
    # # XCom에서 JSON 불러오기
    # ti = context["ti"]
    # raw_json = context['ti'].xcom_pull(task_ids='fetch_raw_data', key='raw_df')
    # df = pd.read_json(raw_json)

    # # 제거할 컬럼
    # df = df.drop(columns=["자치구명", "법정동명"], errors="ignore")
    # df["계약일"] = pd.to_datetime(df["계약일"], format="%Y%m%d", errors="coerce")
    # df["취소일"] = pd.to_datetime(df["취소일"], format="%Y%m%d", errors="coerce")
    # df["층"] = df["층"].replace(['', ' ', 'null', 'N/A'], np.nan)

    # # 2. nullable integer로 변환 (Int64)
    # df["층"] = df["층"].astype("Int64")
    # df = df.replace(['', ' ', 'null', 'N/A'], None)


    # # DB 연결 (SQLAlchemy)
    # db_url = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
    # engine = create_engine(db_url)
    # # df = df.replace(['', ' ', 'null', 'N/A'], np.nan).where(pd.notnull(df), None)
    # # 테이블에 데이터 append (존재하는 테이블만 대상, CREATE는 안함)
    # df.to_sql(
    #     name="부동산데이터",
    #     con=engine,
    #     if_exists="append",   # append: 행 추가만 수행
    #     index=False,
    #     method="multi"        # (optional) 다중 insert 성능 향상
    # )

    # print("df row count:", len(df))

def prune_old_data():
    # 3일 초과된 데이터 delete
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
        print("DB 미리보기:")
        print(df.to_string(index=False))
