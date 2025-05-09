from sqlalchemy import create_engine, text


def init_tables():
    db_url = "postgresql+psycopg2://test:test@postgres-postgresql.postgres.svc.cluster.local:5432/testdb"
    engine = create_engine(db_url)

    with engine.begin() as conn:

        conn.execute(text("""
            -- 의존성이 있는 하위 테이블부터 삭제
            DROP TABLE IF EXISTS station_connection;
            DROP TABLE IF EXISTS station_line_map;
            DROP TABLE IF EXISTS station_line;
            DROP TABLE IF EXISTS station;
            DROP TABLE IF EXISTS real_estate_transaction;
            DROP TABLE IF EXISTS nearby_district;
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

            -- 지하철 노선 정보
            CREATE TABLE station_line (
                line_id INTEGER PRIMARY KEY,
                line_name VARCHAR(50) NOT NULL
            );
            
            -- 지하철역 ↔ 노선 매핑
            CREATE TABLE station_line_map (
            station_id INTEGER NOT NULL,
            line_id INTEGER NOT NULL,
            PRIMARY KEY (station_id, line_id),
            FOREIGN KEY (station_id) REFERENCES station(station_id)
                ON DELETE CASCADE ON UPDATE cascade,
            FOREIGN KEY (line_id) REFERENCES station_line(line_id)
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
