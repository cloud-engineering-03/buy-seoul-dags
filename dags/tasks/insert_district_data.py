import pandas as pd
import re
import os
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

curdir = os.path.dirname(os.path.abspath(__file__))


def load_district_codes():
    path = os.path.join(curdir, '../resources/국토교통부_법정동코드_20240805.csv')
    df3 = pd.read_csv(path, encoding='cp949')
    df3 = df3[['법정동코드', '법정동명']]
    df3.columns = ['district_code', 'district_name']

    sc = df3['district_code'].astype(str)
    df3 = df3[(sc.str[-5:] == '00000') & (sc.str[-8:] != '00000000')]
    df3['province_code'] = sc.str[:2]
    df3['province_name'] = df3['district_name'].str.split().str[0]
    df3['district_code'] = df3['district_code'].astype(str).str[:5]
    df3['district_name'] = df3['district_name'].str.split().str[-1]

    return df3


def insert_district_data():
    db_url = "postgresql+psycopg2://test:test@postgres-postgresql.postgres.svc.cluster.local:5432/testdb"
    engine = create_engine(db_url)
    metadata = MetaData()
    province = Table('province', metadata, autoload_with=engine)
    district = Table('district', metadata, autoload_with=engine)

    df = load_district_codes()
    province_df = df[['province_code', 'province_name']].drop_duplicates()
    district_df = df[['district_code', 'district_name', 'province_code']]

    with engine.connect() as conn:
        for _, row in province_df.iterrows():
            stmt = pg_insert(province).values(
                province_code=row['province_code'],
                province_name=row['province_name']
            ).on_conflict_do_nothing()
            conn.execute(stmt)

        for _, row in district_df.iterrows():
            stmt = pg_insert(district).values(
                district_code=row['district_code'],
                district_name=row['district_name'],
                province_code=row['province_code']
            ).on_conflict_do_nothing()
            conn.execute(stmt)

        conn.commit()
        conn.close()

    return df
