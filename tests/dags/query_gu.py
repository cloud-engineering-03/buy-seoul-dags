
from sqlalchemy import create_engine, text

def query_gu_table():
    db_url = "postgresql+psycopg2://postgres:postgres@airflow-postgresql.airflow:5432/postgres"
    engine = create_engine(db_url)

    with engine.begin() as conn:
        result = conn.execute(text("SELECT * FROM CGG_NM ORDER BY 자치구코드 LIMIT 5"))
        rows = result.fetchall()
        print("=== CGG_NM 테이블 일부 ===")
        for row in rows:
            print(row)

        result = conn.execute(text("SELECT * FROM NEAR_CGG_NAME ORDER BY 기준자치구코드 LIMIT 5"))
        rows = result.fetchall()
        print("=== NEAR_CGG_NAME 테이블 일부 ===")
        for row in rows:
            print(row)

        result = conn.execute(text("SELECT * FROM ESTATE_DATA LIMIT 5"))
        rows = result.fetchall()
        print("=== ESTATE_DATA 테이블 일부 ===")
        for row in rows:
            print(row)
