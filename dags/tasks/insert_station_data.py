import pandas as pd
import re
import os
from .insert_district_data import load_district_codes
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

curdir = os.path.dirname(os.path.abspath(__file__))


def handle_seoul_special_characters(station_name):
    special_characters = {
        '신내역': '신내',
        '당고개': '불암산'
    }
    return special_characters.get(station_name, station_name)


def get_line_id(line_name):
    line_dict = {
        '01호선': 1,
        '02호선': 2,
        '03호선': 3,
        '04호선': 4,
        '05호선': 5,
        '06호선': 6,
        '07호선': 7,
        '08호선': 8,
        '09호선': 9
    }
    return line_dict.get(line_name, None)


def load_station_coordinates():
    path = os.path.join(
        # Added by BlakeCho 20250702
        # 좌표 데이터 소스 변경
        curdir, '../resources/서울지하철_역사_좌표정보_20250702.csv')
    df = pd.read_csv(path, encoding='utf-8')
    df['자치구명'] = df['자치구명'].str.split().str[0]
    df = df[['역명', '위도', '경도', '자치구명']]
    df.columns = ['station_name', 'latitude', 'longitude', 'district_name']
    return df

def load_nearby_district():
    path = os.path.join(
        # Added by BlakeCho 20250702
        # 좌표 데이터 소스 변경
        curdir, '../resources/nearby_district.csv')
    df = pd.read_csv(path, encoding='utf-8')
    df = df[['base_district_code', 'adjacent_district_code', 'distance']]
    # df.columns = ['station_name', 'latitude', 'longitude']
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
            station = station.strip()
            expanded_rows.append({
                'station_name': station,
                'district_name': district
            })

    return pd.DataFrame(expanded_rows)


def load_station_eng_names():
    path = os.path.join(curdir, '../resources/서울교통공사_노선별 지하철역 정보.csv')
    df = pd.read_csv(path, encoding='cp949')
    df = df[['전철역코드', '전철역명', '전철명명(영문)', '호선']]
    df.columns = ['station_id', 'station_name',
                  'station_name_eng', 'line_name']

    df['line_id'] = df['line_name'].apply(get_line_id)
    return df

# Added by BlakeCho 20250526, 역간 연결정보 테스트 업로드용
def load_station_connection():
    path = os.path.join(curdir, '../resources/df_station_connection_20250526.csv')
    df = pd.read_csv(path, encoding='cp949')
    df = df[['from_station_id','to_station_id','time_minutes']]
    df.columns = ['from_station_id', 'to_station_id',
                  'time_minutes']
    return df


def load_connection_df():
    path = os.path.join(
        curdir, '../resources/서울교통공사 역간거리 및 소요시간_240810.csv')
    df = pd.read_csv(path, encoding='cp949')
    df.columns = ['index', 'line_number', 'station_name',
                  'time', 'distance_km', 'distance_nu']
    return df


def load_transfer_df():
    path = os.path.join(
        curdir, '../resources/서울교통공사_환승역거리 소요시간 정보_20250310.csv')
    df = pd.read_csv(path, encoding='cp949')
    df.columns = ['index', 'line_number', 'from_station',
                  'to_line_number', 'time_seconds', 'time_str']
    df['line_number'] = df['line_number'].astype(str)
    return df


def merge_all_data():
    df0 = load_station_eng_names()
    df1 = load_district_station_map()
    df2 = load_district_codes()
    df3 = load_station_coordinates()


    merged_df = pd.merge(df0, df1, on='station_name', how='left')
    merged_df = pd.merge(merged_df, df2, on='district_name', how='left')
    merged_df = pd.merge(merged_df, df3, on='station_name', how='left')
    df_missing_c = merged_df[merged_df['district_code'].isna()]

    # 외부 테이블과 B == B_2 기준으로 left join
    merged = df_missing_c.merge(
        df3,
        left_on='station_name',
        right_on='station_name',
        how='left'
    )
    print(merged)

    # 기존 df에서 C가 결측이었던 부분만 C_2 값으로 채움
    merged_df.loc[merged_df['district_code'].isna(), 'district_code'] = merged['district_code'].values

    merged_df = merged_df.drop_duplicates(['station_id'], ignore_index=True)
    first_ids = merged_df.groupby('station_name')[
        'station_id'].transform('first')
    merged_df['station_id'] = first_ids

    return merged_df


def insert_station_data():
    db_url = "postgresql+psycopg2://test:test@postgres-postgresql.postgres.svc.cluster.local:5432/testdb"
    engine = create_engine(db_url)
    metadata = MetaData()
    metadata.reflect(bind=engine)

    station = metadata.tables['station']
    station_line = metadata.tables['station_line']
    station_line_map = metadata.tables['station_line_map']
    station_connection = metadata.tables['station_connection']
    nearby_district = metadata.tables['nearby_district']

    line_dict = {
        1: '1호선', 2: '2호선', 3: '3호선', 4: '4호선',
        5: '5호선', 6: '6호선', 7: '7호선', 8: '8호선', 9: '9호선'
    }

    # station_line 테이블 데이터 생성
    station_line_df = pd.DataFrame(list(line_dict.items()), columns=[
                                   'line_id', 'line_name'])
    merged_df = merge_all_data()
    connection_df = load_connection_df()
    transfer_df = load_transfer_df()
    # Added by BlakeCho 2025026
    # , 역간 연결정보 테스트 업로드용
    station_connection_df = load_station_connection()
    nearby_district_df = load_nearby_district()

    merged_df = merged_df.where(pd.notnull(merged_df), None)

    name_to_id = dict(merged_df[['station_name', 'station_id']].values)

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
            if pd.notnull(row['station_id']) and pd.notnull(row['line_id']):
                stmt = pg_insert(station_line_map).values(
                    station_id=row['station_id'],
                    line_id=row['line_id']
                ).on_conflict_do_nothing()
                conn.execute(stmt)

        # station_connection 테이블 insert
        for i in range(len(connection_df) - 1):
            row1 = connection_df.iloc[i]
            row2 = connection_df.iloc[i + 1]

            from_id = name_to_id.get(
                handle_seoul_special_characters(row1['station_name'].strip()))
            to_id = name_to_id.get(handle_seoul_special_characters(
                row2['station_name'].strip()))
            if pd.notnull(from_id) and pd.notnull(to_id):
                continue
            stmt = pg_insert(station_connection).values(
                from_station_id=from_id,
                to_station_id=to_id,
                time_minutes=int(row2['time']) if pd.notnull(
                    row2['time']) else None,
                transfer=False,
                line_number=str(row1['line_number'])
            ).on_conflict_do_nothing()
            conn.execute(stmt)
        
        # Added by BlakeCho, 테스트 역간 이동시간 테스트 업로드
        for _, row in station_connection_df.iterrows():
            stmt = pg_insert(station_connection).values(
                from_station_id = row['from_station_id'],
                to_station_id = row['to_station_id'],
                time_minutes = row['time_minutes'],
                transfer = False,
                line_number = 0
            ).on_conflict_do_nothing()
            conn.execute(stmt)


        for _, row in nearby_district_df.iterrows():
            stmt = pg_insert(nearby_district).values(
                base_district_code = str(int(row['base_district_code'])).zfill(5),
                adjacent_district_code = str(int(row['adjacent_district_code'])).zfill(5),
                distance = row['distance'],
            ).on_conflict_do_nothing()
            conn.execute(stmt)

        for _, row in transfer_df.iterrows():
            from_id = name_to_id.get(row['from_station'])
            to_id = name_to_id.get(row['from_station'])
            if from_id is None or to_id is None:
                continue
            stmt = pg_insert(station_connection).values(
                from_station_id=from_id,
                to_station_id=to_id,
                time_minutes=int(row['time_seconds']) // 60,
                transfer=True,
                line_number=row['line_number']
            ).on_conflict_do_nothing()
            conn.execute(stmt)
