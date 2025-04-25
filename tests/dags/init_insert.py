
from sqlalchemy import create_engine, text
import pandas as pd

def insert_init_data():
    # DB 연결 설정
    db_url = "postgresql+psycopg2://postgres:postgres@airflow-postgresql.airflow:5432/postgres"
    engine = create_engine(db_url)

    # 1. CREATE TABLE 쿼리 실행
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS 자치구코드_군구명 (
                자치구코드 CHAR(5) PRIMARY KEY,
                군구명 VARCHAR(20)
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS 인접자치구_거리 (
                기준자치구코드 CHAR(5),
                인접자치구코드 CHAR(5),
                거리_km FLOAT,
                PRIMARY KEY (기준자치구코드, 인접자치구코드),
                FOREIGN KEY (기준자치구코드) REFERENCES 자치구코드_군구명(자치구코드),
                FOREIGN KEY (인접자치구코드) REFERENCES 자치구코드_군구명(자치구코드)
            );
        """))

    # 2. JSON 로드 및 INSERT
    gu_df = pd.read_json("/opt/airflow/dags/자치구코드_군구명_매핑.json")
    dist_df = pd.read_json("/opt/airflow/dags/인접자치구_거리.json")
    
    with engine.begin() as conn:
        for _, row in gu_df.iterrows():
            
            conn.execute(
                text("INSERT INTO 자치구코드_군구명 (자치구코드, 군구명) VALUES (:code, :name)"),
                {"code": row["자치구코드"], "name": row["군구명"]}
            )
        for _, row in dist_df.iterrows():
            row["기준자치구코드"] = str(int(row["기준자치구코드"])).zfill(5)
            row["인접자치구코드"] = str(int(row["인접자치구코드"])).zfill(5)
            conn.execute(
                text("INSERT INTO 인접자치구_거리 (기준자치구코드, 인접자치구코드, 거리_km) VALUES (:from_, :to_, :dist)"),
                {"from_": row["기준자치구코드"], "to_": row["인접자치구코드"], "dist": row["거리_km"]}
            )
