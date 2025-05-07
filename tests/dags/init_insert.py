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
            -- 의존성이 있는 하위 테이블부터 삭제
            DROP TABLE IF EXISTS station_connection;
            DROP TABLE IF EXISTS station_line_map;
            DROP TABLE IF EXISTS real_estate_transaction;
            DROP TABLE IF EXISTS nearby_district;
            DROP TABLE IF EXISTS station;
            DROP TABLE IF EXISTS district;
            DROP TABLE IF EXISTS province;

            -- 시도 정보
            CREATE TABLE province (
            province_code CHAR(2) PRIMARY KEY,
            province_name VARCHAR(50) NOT NULL
            );

            -- 자치구 정보
            CREATE TABLE district (
            district_code CHAR(5) PRIMARY KEY,
            district_name VARCHAR(20) NOT NULL,
            province_code CHAR(2) NOT NULL,
            FOREIGN KEY (province_code) REFERENCES province(province_code)
                ON DELETE CASCADE ON UPDATE CASCADE
            );

            -- 지하철역 정보
            CREATE TABLE station (
            station_id SERIAL PRIMARY KEY,
            station_name VARCHAR(50) NOT NULL,
            station_name_eng VARCHAR(50),
            latitude REAL,
            longitude REAL,
            district_code CHAR(5),
            FOREIGN KEY (district_code) REFERENCES district(district_code)
                ON DELETE CASCADE ON UPDATE CASCADE
            );

            -- 지하철역 ↔ 노선 매핑
            CREATE TABLE station_line_map (
            station_id INTEGER NOT NULL,
            line_number VARCHAR(10) NOT NULL,
            PRIMARY KEY (station_id, line_number),
            FOREIGN KEY (station_id) REFERENCES station(station_id)
                ON DELETE CASCADE ON UPDATE CASCADE
            );

            -- 인접 자치구 관계
            CREATE TABLE nearby_district (
            base_district_code CHAR(5) NOT NULL,
            adjacent_district_code CHAR(5) NOT NULL,
            distance REAL,
            PRIMARY KEY (base_district_code, adjacent_district_code),
            FOREIGN KEY (base_district_code) REFERENCES district(district_code)
                ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (adjacent_district_code) REFERENCES district(district_code)
                ON DELETE CASCADE ON UPDATE CASCADE
            );

            -- 부동산 거래 정보
            CREATE TABLE real_estate_transaction (
            id SERIAL PRIMARY KEY,
            district_code CHAR(5),
            legal_dong_code VARCHAR(10),
            contract_date DATE,
            cancellation_date DATE,
            lot_type INT,
            lot_type_name VARCHAR(10),
            main_lot_number VARCHAR(4),
            sub_lot_number VARCHAR(4),
            building_name VARCHAR(100),
            floor INT,
            building_area REAL,
            land_area REAL,
            transaction_amount REAL,
            building_usage VARCHAR(100),
            construction_year INT,
            report_type VARCHAR(10),
            ownership_type VARCHAR(10),
            report_year INT,
            agent_office_district_name VARCHAR(40),
            FOREIGN KEY (district_code) REFERENCES district(district_code)
                ON DELETE CASCADE ON UPDATE CASCADE
            );

            -- 지하철역 간 연결 정보
            CREATE TABLE station_connection (
            from_station_id INT NOT NULL,
            to_station_id INT NOT NULL,
            time_minutes INT,
            transfer BOOLEAN,
            line_number VARCHAR(10),
            PRIMARY KEY (from_station_id, to_station_id, line_number),
            FOREIGN KEY (from_station_id) REFERENCES station(station_id)
                ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (to_station_id) REFERENCES station(station_id)
                ON DELETE CASCADE ON UPDATE CASCADE
            );
        """))
      

    # 2. JSON 로드 및 INSERT
    curdir = os.path.dirname(os.path.abspath(__file__))
    # current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # gu_df = pd.read_json(curdir"/자치구코드_군구명_매핑.json")
    prov_df = pd.read_json(os.path.join(curdir,"sido_code.json"))
    gu_df = pd.read_json(os.path.join(curdir,"자치구코드_군구명_매핑_서울경기인천.json"))

    sido_df = pd.read_json(os.path.join(curdir,"sido_code.json"))
    dist_df = pd.read_json(os.path.join(curdir,"인접자치구_거리.json"))
    dist_df["기준자치구코드"] = dist_df["기준자치구코드"].astype(str).str.zfill(5)
    dist_df["인접자치구코드"] = dist_df["인접자치구코드"].astype(str).str.zfill(5)
    cgg_station_map_df = pd.read_csv(os.path.join(curdir,"서울지하철_역위치_영문명_자치구매핑완료.csv"))
    subway_line_df = pd.read_csv(os.path.join(curdir,"서울지하철_역호선_매핑테이블.csv"))

    
    with engine.begin() as conn:
        
        for _, row in prov_df.iterrows():
            
            conn.execute(
                text("INSERT INTO PROVINCE (province_code, province_name) VALUES (:code, :name)"),
                {"code": row["시도코드"], "name": row["시도명"]}, 
            )

        for _, row in gu_df.iterrows():
            
            conn.execute(
                text("INSERT INTO DISTRICT (district_code, district_name,province_code) VALUES (:code, :name, :province_code)"),
                {"code": row["자치구코드"], "name": row["군구명"], "province_code": str(row["자치구코드"])[:2]}, 
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

