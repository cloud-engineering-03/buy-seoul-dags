
from sqlalchemy import create_engine, text
import pandas as pd
import os

def insert_init_data():
    # DB 연결 설정
    db_url = "postgresql+psycopg2://test:test@postgres-postgresql.postgres.svc.cluster.local:5432/testdb"
    engine = create_engine(db_url)

    # 1. CREATE TABLE 쿼리 실행
    with engine.begin() as conn:
        conn.execute(text("""
            DROP TABLE IF EXISTS public.CGG_NM CASCADE;
            CREATE TABLE public.CGG_NM (
                자치구코드 CHAR(5) PRIMARY KEY,
                군구명 VARCHAR(20)
            );
        """))

        conn.execute(text("""
            DROP TABLE IF EXISTS public.NEAR_CGG_NAME;
            CREATE TABLE public.NEAR_CGG_NAME (
                기준자치구코드 CHAR(5),
                인접자치구코드 CHAR(5),
                거리_km FLOAT,
                PRIMARY KEY (기준자치구코드, 인접자치구코드),
                FOREIGN KEY (기준자치구코드) REFERENCES CGG_NM(자치구코드),
                FOREIGN KEY (인접자치구코드) REFERENCES CGG_NM(자치구코드)
            );
        """))

        conn.execute(text("""
            DROP TABLE IF EXISTS public.SUBWAY_CGG_MAPPING;
            CREATE TABLE public.SUBWAY_CGG_MAPPING (
                역명 VARCHAR(50) PRIMARY KEY,
                자치구코드 CHAR(5) NOT NULL,
                FOREIGN KEY (자치구코드) REFERENCES CGG_NM(자치구코드)
            );
        """))
        
        conn.execute(text("""
            DROP TABLE IF EXISTS public.SIDO_NAME;
            CREATE TABLE public.SIDO_NAME (
                시도코드 CHAR(2) PRIMARY KEY,
                시도명 VARCHAR(50) NOT NULL
            );
        """))

    # 2. JSON 로드 및 INSERT
    curdir = os.path.dirname(os.path.abspath(__file__))
    # current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # gu_df = pd.read_json(curdir"/자치구코드_군구명_매핑.json")
    gu_df = pd.read_json(os.path.join(curdir,"자치구코드_군구명_매핑_서울경기인천.json"))

    sido_df = pd.read_json(os.path.join(curdir,"sido_code.json"))
    dist_df = pd.read_json(os.path.join(curdir,"인접자치구_거리.json"))
    cgg_station_map_df = pd.read_csv(os.path.join(curdir,"서울지하철_역위치_자치구매핑완료_서울경기인천.csv"))

    
    with engine.begin() as conn:
        
        for _, row in gu_df.iterrows():
            
            conn.execute(
                text("INSERT INTO CGG_NM (자치구코드, 군구명) VALUES (:code, :name)"),
                {"code": row["자치구코드"], "name": row["군구명"]}
            )
        for _, row in sido_df.iterrows():
            row["시도코드"] = str(int(row["시도코드"])).zfill(2)
            conn.execute(
                text("INSERT INTO SIDO_NAME (시도코드, 시도명) VALUES (:sido_code, :sido_name)"),
                {"sido_code": row["시도코드"], "sido_name": row["시도명"]}
            )
        print(len(dist_df))
        for _, row in dist_df.iterrows():
            row["기준자치구코드"] = str(int(row["기준자치구코드"])).zfill(5)
            row["인접자치구코드"] = str(int(row["인접자치구코드"])).zfill(5)
            row["거리_km"] = float(row["거리_km"])
            conn.execute(
                text("INSERT INTO NEAR_CGG_NAME (기준자치구코드, 인접자치구코드, 거리_km) VALUES (:from_, :to_, :dist)"),
                {"from_": row["기준자치구코드"], "to_": row["인접자치구코드"], "dist": row["거리_km"]}
            )

        
        # for _, row in cgg_station_map_df.iterrows():
            
        #     conn.execute(
        #         text("INSERT INTO SUBWAY_CGG_MAPPING (역명, 자치구코드) VALUES (:station_name, :cgg_name)"),
        #         {"station_name": row["역명"], "cgg_name": row["자치구코드"]}
        #     )

