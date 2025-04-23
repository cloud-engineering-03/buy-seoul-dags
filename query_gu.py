
from sqlalchemy import create_engine, text

def query_gu_table():
    db_url = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
    engine = create_engine(db_url)

    with engine.begin() as conn:
        result = conn.execute(text("SELECT * FROM 자치구코드_군구명 ORDER BY 자치구코드 LIMIT 5"))
        rows = result.fetchall()
        print("=== 자치구코드_군구명 테이블 일부 ===")
        for row in rows:
            print(row)

        result = conn.execute(text("SELECT * FROM 인접자치구_거리 ORDER BY 기준자치구코드 LIMIT 5"))
        rows = result.fetchall()
        print("=== 인접자치구_거리 테이블 일부 ===")
        for row in rows:
            print(row)

        result = conn.execute(text("SELECT * FROM 부동산데이터 LIMIT 5"))
        rows = result.fetchall()
        print("=== 부동산데이터 테이블 일부 ===")
        for row in rows:
            print(row)
