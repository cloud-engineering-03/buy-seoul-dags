# data_processing.py
import pandas as pd
import re
import os

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
