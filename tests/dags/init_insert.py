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
            DROP TABLE IF EXISTS public.PROVINCE CASCADE;
            CREATE TABLE public.PROVINCE (
                province_code CHAR(2) PRIMARY KEY,
                province_name VARCHAR(50)
            );
        """))
        
        conn.execute(text("""
            DROP TABLE IF EXISTS public.DISTRICT CASCADE;
            CREATE TABLE public.DISTRICT (
                district_code CHAR(5) PRIMARY KEY,
                district_name VARCHAR(20),
                province_code CHAR(2),
                FOREIGN KEY (province_code) REFERENCES PROVINCE(province_code)
            );
        """))

        conn.execute(text("""
            DROP TABLE IF EXISTS public.NEARBY_DISTRICT CASCADE;
            CREATE TABLE public.NEARBY_DISTRICT (
                base_district_code CHAR(5),
                adjacent_district_code CHAR(5),
                distance DOUBLE PRECISION,
                FOREIGN KEY (base_district_code) REFERENCES DISTRICT(district_code),
                FOREIGN KEY (adjacent_district_code) REFERENCES DISTRICT(district_code),
                PRIMARY KEY (base_district_code, adjacent_district_code)
            );
        """))
        
        conn.execute(text("""
            DROP TABLE IF EXISTS public.STATION CASCADE;
            CREATE TABLE public.STATION (
                station_id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
                district_code CHAR(5),
                station_name VARCHAR(50),
                station_name_eng VARCHAR(50),
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                FOREIGN KEY (district_code) REFERENCES DISTRICT(district_code)
            );
        """))


        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.REAL_ESTATE_TRANSACTION  (
            id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
            district_code char(5) NULL,
            legal_dong_code varchar(10) NULL,
            contract_date date NULL,
            cancellation_date date NULL,
            lot_type int4 NULL,
            lot_type_name varchar(10) NULL,
            main_lot_number varchar(4) NULL,
            sub_lot_number varchar(4) NULL,
            building_name varchar(100) NULL,
            floor int4 NULL,
            building_area DOUBLE PRECISION NULL,
            land_area DOUBLE PRECISION NULL,
            transaction_amount DOUBLE PRECISION NULL,
            building_usage varchar(100) NULL,
            construction_year int4 NULL,
            report_type varchar(10) NULL,
            ownership_type varchar(10) NULL,
            report_year int4 NULL,
            agent_office_district_name varchar(40) NULL,
            CONSTRAINT ESTATE_DATA_pkey PRIMARY KEY (id),
            FOREIGN KEY (district_code) REFERENCES DISTRICT(district_code)
            );
        """))
        
        
        conn.execute(text("""
            DROP TABLE IF EXISTS public.STATION_LINE_MAP CASCADE;
            CREATE TABLE public.STATION_LINE_MAP (
                station_id integer NOT NULL,
                line_number VARCHAR(10),
                PRIMARY KEY (station_id,line_number),
                FOREIGN KEY (station_id) REFERENCES STATION(station_id)
            );
        """))

    # 2. JSON 로드 및 INSERT
    curdir = os.path.dirname(os.path.abspath(__file__))
    # current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # gu_df = pd.read_json(curdir"/자치구코드_군구명_매핑.json")
    gu_df = pd.read_json(os.path.join(curdir,"자치구코드_군구명_매핑_서울경기인천.json"))

    sido_df = pd.read_json(os.path.join(curdir,"sido_code.json"))
    dist_df = pd.read_json(os.path.join(curdir,"인접자치구_거리.json"))
    dist_df["기준자치구코드"] = dist_df["기준자치구코드"].astype(str).str.zfill(5)
    dist_df["인접자치구코드"] = dist_df["인접자치구코드"].astype(str).str.zfill(5)
    cgg_station_map_df = pd.read_csv(os.path.join(curdir,"서울지하철_역위치_영문명_자치구매핑완료.csv"))
    subway_line_df = pd.read_csv(os.path.join(curdir,"서울지하철_역호선_매핑테이블.csv"))

    
    with engine.begin() as conn:
        
        for _, row in gu_df.iterrows():
            
            conn.execute(
                text("INSERT INTO DISTRICT (district_code, district_name) VALUES (:code, :name)"),
                {"code": row["자치구코드"], "name": row["군구명"]}
            )
        for _, row in sido_df.iterrows():
            row["시도코드"] = str(int(row["시도코드"])).zfill(2)
            conn.execute(
                text("INSERT INTO PROVINCE (province_code, province_name) VALUES (:sido_code, :sido_name)"),
                {"sido_code": row["시도코드"], "sido_name": row["시도명"]}
            )

        print(len(dist_df))
        for _, row in dist_df.iterrows():
            conn.execute(
                text("INSERT INTO NEARBY_DISTRICT (base_district_code, adjacent_district_code, distance) VALUES (:from_, :to_, :dist)"),
                {"from_": row["기준자치구코드"], "to_": row["인접자치구코드"], "dist": row["거리_km"]}
            )

        
        for _, row in cgg_station_map_df.iterrows():
            conn.execute(
                text("INSERT INTO STATION (station_name, latitude, longitude, district_code, station_name_eng) VALUES (:station_name,:lat, :lon, :cgg_code, :eng_name)"),
                {"station_name": row["STATION_NM"], "cgg_code": row["자치구코드"],"lat": row["위도"], "lon": row["경도"],"eng_name": row["STATION_NM_ENG"]}
            )
        
        for _, row in subway_line_df.iterrows():
            
            conn.execute(
                text("INSERT INTO STATION_LINE_MAP (station_id, line_number) VALUES (:station_name, :line_num)"),
                {"station_name": row["STATION_NM"], "line_num": row["LINE_NUM"]}
            )

