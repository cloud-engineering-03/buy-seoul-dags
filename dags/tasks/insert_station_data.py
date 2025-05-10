import pandas as pd
import re
import os
from .insert_district_data import load_district_codes
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

curdir = os.path.dirname(os.path.abspath(__file__))


def load_station_coordinates():
    path = os.path.join(
        curdir, '../resources/서울교통공사_1_8호선 역사 좌표(위경도) 정보_20241031.csv')
    df = pd.read_csv(path, encoding='cp949')
    df = df[['연번', '호선', '역명', '위도', '경도']]
    df.columns = ['station_id', 'line_id',
                  'station_name', 'latitude', 'longitude']
    return df


def load_district_station_map():
    path = os.path.join(curdir, '../resources/서울교통공사_자치구별지하철역정보_20250317.csv')
    df1 = pd.read_csv(path, encoding='cp949')
    expanded_rows = []

    for _, row in df1.iterrows():
        district = row['자치구']
        stations = row['해당역(호선)']
        matches = re.findall(r'([^(),]+)\(\d+[^)]*\)', stations)
        for station in matches:
            expanded_rows.append({
                'station_name': station.strip(),
                'district_name': district
            })

    return pd.DataFrame(expanded_rows)


def load_station_eng_names():
    path = os.path.join(curdir, '../resources/서울교통공사_노선별 지하철역 정보.csv')
    df2 = pd.read_csv(path, encoding='cp949')
    df2 = df2[['전철역명', '전철명명(영문)']]
    df2.columns = ['station_name', 'station_name_eng']
    return df2


def merge_all_data():
    df = load_station_coordinates()
    df2 = load_station_eng_names()
    expanded_df = load_district_station_map()
    df3 = load_district_codes()

    merged_df = pd.merge(pd.merge(pd.merge(df, df2, "inner", "station_name"),
                                  expanded_df, "inner", "station_name"),
                         df3, "inner", "district_name")

    merged_df = merged_df.drop_duplicates(['station_id'], ignore_index=True)
    print(merged_df.head())
    return merged_df


def insert_station_data():
    db_url = "postgresql+psycopg2://test:test@postgres-postgresql.postgres.svc.cluster.local:5432/testdb"
    engine = create_engine(db_url)
    metadata = MetaData()
    metadata.reflect(bind=engine)

    station = metadata.tables['station']
    station_line = metadata.tables['station_line']
    station_line_map = metadata.tables['station_line_map']

    line_dict = {
        1: '1호선', 2: '2호선', 3: '3호선', 4: '4호선',
        5: '5호선', 6: '6호선', 7: '7호선', 8: '8호선'
    }

    merged_df = merge_all_data()
    # station_line 테이블 데이터 생성
    station_line_df = merged_df[['line_id']].drop_duplicates()
    station_line_df['line_name'] = station_line_df['line_id'].apply(
        lambda x: line_dict[x]
    )

    with engine.begin() as conn:
        # station 테이블 insert
        for _, row in merged_df.iterrows():
            stmt = pg_insert(station).values(
                station_id=row['station_id'],
                station_name=row['station_name'],
                station_name_eng=row['station_name_eng'],
                latitude=row['latitude'],
                longitude=row['longitude'],
                district_code=row['district_code']
            ).on_conflict_do_nothing()
            conn.execute(stmt)

        # station_line 테이블 insert
        for _, row in station_line_df.iterrows():
            stmt = pg_insert(station_line).values(
                line_id=row['line_id'],
                line_name=row['line_name']
            ).on_conflict_do_nothing()
            conn.execute(stmt)

        # station_line_map 테이블 insert
        for _, row in merged_df.iterrows():
            stmt = pg_insert(station_line_map).values(
                station_id=row['station_id'],
                line_id=row['line_id']
            ).on_conflict_do_nothing()
            conn.execute(stmt)
