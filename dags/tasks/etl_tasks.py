import requests
import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from sqlalchemy.engine import make_url
import psycopg2
from airflow.models import Variable


db_url = "postgresql+psycopg2://test:test@postgres-postgresql.postgres.svc.cluster.local:5432/testdb"


def fetch_raw_data(**context):
    api_key = Variable.get("SEOUL_API_KEY")
    service_name = "tbLnOpendataRtmsV"
    base_url = "http://openapi.seoul.go.kr:8088"

    step = 1000  # API 최대 반환 건수 권장 단위
    today = datetime.today()
    year = today.strftime("%Y")

    all_rows = []

    for month in range(1, 13):
        deal_ymd = f"{year}{str(month).zfill(2)}"
        for start in range(1, 100000, step):  # 최대 10,000건까지 페이징
            end = start + step - 1
            url = f"{base_url}/{api_key}/json/{service_name}/{start}/{end}/"
            params = {"DEAL_YMD": deal_ymd}
            response = requests.get(url, params=params)
            if response.status_code != 200:
                print(f"[{deal_ymd}] 요청 실패: {response.status_code}")
                break
            data = response.json()
            items = data.get(service_name, {}).get("row", [])
            if not items:
                break  # 더 이상 없음
            all_rows.extend(items)

    if not all_rows:
        print("❌ 수집된 데이터가 없습니다.")
        return

    df = pd.DataFrame(all_rows)

    column_mapping = {
        "RTRCN_DAY": "취소일", "LAND_AREA": "토지면적", "STDG_CD": "법정동코드",
        "BLDG_NM": "건물명", "STDG_NM": "법정동명", "MNO": "본번",
        "THING_AMT": "물건금액", "LOTNO_SE_NM": "지번구분명", "LOTNO_SE": "지번구분",
        "CTRT_DAY": "계약일", "RCPT_YR": "접수연도", "OPBIZ_RESTAGNT_SGG_NM": "신고한 개업공인중개사 시군구명",
        "ARCH_AREA": "건물면적", "CGG_CD": "자치구코드", "RGHT_SE": "권리구분",
        "SNO": "부번", "FLR": "층", "CGG_NM": "자치구명",
        "BLDG_USG": "건물용도", "ARCH_YR": "건축년도", "DCLR_SE": "신고구분"
    }
    df.rename(columns=column_mapping, inplace=True)

    # XCom으로 전달
    context['ti'].xcom_push(key='raw_df', value=df.to_json())
    print(f"[✅ 수집 완료] 총 수집 행 수: {len(df)}")


def validate_data():
    # df 불러와 유효성 검사
    ...


def insert_data(**context):
    ti = context["ti"]
    raw_json = ti.xcom_pull(task_ids='fetch_raw_data', key='raw_df')
    df = pd.read_json(raw_json)

    # 전처리
    # 빈 문자열, 공백, 'null' 문자열을 None으로 변환
    df = df.drop(columns=["자치구명"], errors="ignore")
    df = df.drop(columns=["법정동명"], errors="ignore")
    for col in ["계약일", "취소일"]:
        df[col] = df[col].replace(['', ' ', 'null', 'N/A'], None)
        df[col] = df[col].apply(lambda x: pd.to_datetime(
            x, format='%Y%m%d', errors='coerce') if x is not None else None)
        df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)

    # 전역 None/결측 처리
    df = df.replace([pd.NaT, '', ' ', 'null', 'N/A'], None)

    df = df.rename(columns={
        "자치구코드": "district_code", "법정동코드": "legal_dong_code",
        "계약일": "contract_date", "취소일": "cancellation_date",
        "지번구분": "lot_type", "지번구분명": "lot_type_name",
        "본번": "main_lot_number", "부번": "sub_lot_number",
        "건물명": "building_name", "층": "floor",
        "건물면적": "building_area", "토지면적": "land_area",
        "물건금액": "transaction_amount", "건물용도": "building_usage",
        "건축년도": "construction_year", "신고구분": "report_type",
        "권리구분": "ownership_type", "접수연도": "report_year",
        "신고한 개업공인중개사 시군구명": "agent_office_district_name"
    })
    df = df[[  # Reorder and subset to ensure correct columns
        "district_code", "legal_dong_code", "contract_date", "cancellation_date", "lot_type", "lot_type_name",
        "main_lot_number", "sub_lot_number", "building_name", "floor", "building_area", "land_area",
        "transaction_amount", "building_usage", "construction_year", "report_type", "ownership_type",
        "report_year", "agent_office_district_name"
    ]]

    url = make_url(db_url)
    conn = psycopg2.connect(
        dbname=url.database,
        user=url.username,
        password=url.password,
        host=url.host,
        port=url.port
    )

    bulk_insert_data(df, "public.real_estate_transaction",
                     conn, chunk_size=100)


def prune_old_data():
    engine = create_engine(db_url)

    cutoff = (datetime.today() - timedelta(days=3)).strftime("%Y%m%d")

    with engine.connect() as conn:
        conn.execute(text("""
            DELETE FROM public.ESTATE_DATA
            WHERE "계약일" < :cutoff
        """), {"cutoff": cutoff})


def t5_check_table():
    engine = create_engine(db_url)

    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM public.ESTATE_DATA", conn)
        print(df.to_string(index=False))


def bulk_insert_data(df, table_name, conn, chunk_size=100):
    cur = conn.cursor()

    columns = [
        "district_code", "legal_dong_code", "contract_date", "cancellation_date", "lot_type", "lot_type_name",
        "main_lot_number", "sub_lot_number", "building_name", "floor", "building_area", "land_area",
        "transaction_amount", "building_usage", "construction_year", "report_type", "ownership_type",
        "report_year", "agent_office_district_name"
    ]
    quoted_columns = [f'"{col}"' for col in columns]

    insert_prefix = f"INSERT INTO {table_name} ({', '.join(quoted_columns)}) VALUES "

    success_count = 0
    fail_count = 0
    buffer = []

    for idx, row in df.iterrows():
        values = []
        for col in columns:
            val = row.get(col, None)
            if pd.isna(val):
                values.append('NULL')
            elif isinstance(val, str):
                val = val.replace("'", "''")
                values.append(f"'{val}'")
            elif isinstance(val, pd.Timestamp):
                values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
            elif val is None:
                values.append('NULL')
            else:
                values.append(str(val))

        buffer.append(f"({', '.join(values)})")

        # chunk_size마다 insert
        if (idx + 1) % chunk_size == 0:
            try:
                sql = insert_prefix + ",\n".join(buffer) + ";"
                cur.execute(sql)
                conn.commit()
                print(f"[✅ {idx+1}개] 삽입 성공")
                success_count += len(buffer)
            except Exception as e:
                conn.rollback()
                print(f"[❌ {idx+1}개] 삽입 실패: {e}")
                fail_count += len(buffer)
            buffer = []
        # print(sql)
    # 남은 row 처리
    if buffer:
        try:
            sql = insert_prefix + ",\n".join(buffer) + ";"
            cur.execute(sql)
            conn.commit()
            print(f"[✅ 남은 {len(buffer)}개] 삽입 성공")
            success_count += len(buffer)
        except Exception as e:
            conn.rollback()
            print(f"[❌ 남은 {len(buffer)}개] 삽입 실패: {e}")
            fail_count += len(buffer)

    cur.close()
    conn.close()

    print(f"총 성공: {success_count} / 실패: {fail_count}")
