import os
import sys
import logging
import pandas as pd
import psycopg2
import psycopg2.extras
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

CSV_PATH = os.getenv('CSV_PATH', '/data/accidents_raw.csv')
DATABASE_URL = os.getenv('DATABASE_URL')
VIEW_SQL_PATH = Path('sql/create_view.sql')

DATE_FORMATS = ['%Y-%m-%d', '%d/%m/%Y']
TIME_FORMATS = ['%H:%M']


def parse_date(value: str):
    if pd.isna(value):
        return pd.NaT
    for fmt in DATE_FORMATS:
        try:
            return pd.to_datetime(value, format=fmt)
        except Exception:
            continue
    try:
        return pd.to_datetime(value, errors='coerce')
    except Exception:
        return pd.NaT


def parse_time(value: str):
    if pd.isna(value):
        return pd.NaT
    for fmt in TIME_FORMATS:
        try:
            return pd.to_datetime(value, format=fmt).time()
        except Exception:
            continue
    return pd.NaT


def normalize_alcool(value: str):
    if pd.isna(value):
        return None
    v = str(value).strip().lower()
    if v in ('oui', 'o', 'true', '1'):
        return 'Positif'
    if v in ('non', 'n', 'false', '0'):
        return 'Negatif'
    return None


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    total = len(df)
    df['parsed_date'] = df['date_accident'].apply(parse_date)
    before = len(df)
    df = df[~df['parsed_date'].isna()].copy()
    removed_no_date = before - len(df)

    df['parsed_time'] = df['heure_accident'].apply(parse_time)
    df['alcool_norm'] = df['alcool'].apply(normalize_alcool)

    df['year'] = df['parsed_date'].dt.year.astype('int64')
    df['month'] = df['parsed_date'].dt.month.astype('int64')
    df['hour'] = df['parsed_time'].apply(lambda t: t.hour if pd.notna(t) else None)

    # Deduplicate submission_id
    df['submission_id'] = pd.to_numeric(df['submission_id'], errors='coerce')
    before_dedup = len(df)
    df = df.dropna(subset=['submission_id']).drop_duplicates(subset=['submission_id'])
    dedup_removed = before_dedup - len(df)

    logging.info(f"Lines read: {total}")
    logging.info(f"Lines removed (invalid/missing date): {removed_no_date}")
    logging.info(f"Lines removed (duplicates or invalid submission_id): {dedup_removed}")

    return df


def ensure_table(conn):
    # Drop existing table if not partitioned (for migration)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = 'accidents_clean' AND table_type = 'BASE TABLE';
        """)
        exists = cur.fetchone()[0] > 0
        if exists:
            cur.execute("""
                SELECT COUNT(*) FROM pg_partitioned_table 
                WHERE partrelid = 'accidents_clean'::regclass;
            """)
            is_partitioned = cur.fetchone()[0] > 0
            if not is_partitioned:
                logging.warning("Existing non-partitioned table found. Dropping for partitioning.")
                cur.execute("DROP TABLE IF EXISTS accidents_clean CASCADE;")
                conn.commit()
    
    ddl = """
    CREATE TABLE IF NOT EXISTS accidents_clean (
        submission_id INTEGER NOT NULL,
        date_accident DATE NOT NULL,
        heure_accident TIME NULL,
        region TEXT NULL,
        type_accident TEXT NULL,
        alcool TEXT NULL CHECK (alcool IN ('Positif','Negatif') OR alcool IS NULL),
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        hour INTEGER NULL
    ) PARTITION BY RANGE (year);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
        # Create unique index on partitioned table (required for ON CONFLICT)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS accidents_clean_unique_idx 
            ON accidents_clean (submission_id, year);
        """)
        conn.commit()


def create_partitions(conn, df: pd.DataFrame):
    """Create partitions dynamically based on years in the dataset."""
    years = df['year'].dropna().unique()
    with conn.cursor() as cur:
        for year in sorted(years):
            year_int = int(year)
            partition_name = f"accidents_{year_int}"
            # Check if partition exists
            cur.execute("""
                SELECT COUNT(*) FROM pg_tables 
                WHERE tablename = %s;
            """, (partition_name,))
            if cur.fetchone()[0] == 0:
                cur.execute(f"""
                    CREATE TABLE {partition_name} PARTITION OF accidents_clean
                    FOR VALUES FROM ({year_int}) TO ({year_int + 1});
                """)
                logging.info(f"Partition created: {partition_name} (year={year_int})")
        conn.commit()


def create_or_replace_view(conn):
    sql = None
    if VIEW_SQL_PATH.exists():
        sql = VIEW_SQL_PATH.read_text(encoding='utf-8')
    else:
        sql = (
            "CREATE OR REPLACE VIEW accidents_monthly_stats AS "
            "SELECT EXTRACT(YEAR FROM date_accident)::int AS year, "
            "EXTRACT(MONTH FROM date_accident)::int AS month, "
            "COUNT(*) AS total_accidents, "
            "ROUND(AVG(CASE WHEN alcool = 'Positif' THEN 1.0 ELSE 0.0 END)::numeric, 4) AS taux_alcool_positif "
            "FROM accidents_clean GROUP BY 1,2 ORDER BY 1,2;"
        )
    with conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()


def insert_rows(conn, df: pd.DataFrame) -> int:
    rows = [
        (
            int(row['submission_id']),
            row['parsed_date'].date(),
            row['parsed_time'] if pd.notna(row['parsed_time']) else None,
            row['region'] if pd.notna(row['region']) else None,
            row['type_accident'] if pd.notna(row['type_accident']) else None,
            row['alcool_norm'],
            int(row['year']),
            int(row['month']),
            int(row['hour']) if pd.notna(row['hour']) else None,
        )
        for _, row in df.iterrows()
    ]

    insert_sql = """
        INSERT INTO accidents_clean (
            submission_id, date_accident, heure_accident, region, type_accident, alcool, year, month, hour
        ) VALUES %s;
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, insert_sql, rows, page_size=1000)
    conn.commit()
    return len(rows)


def main():
    database_url = DATABASE_URL
    if not database_url:
        user = os.getenv('POSTGRES_USER', 'app')
        pwd = os.getenv('POSTGRES_PASSWORD', 'app')
        db = os.getenv('POSTGRES_DB', 'data_lab')
        host = os.getenv('POSTGRES_HOST', 'postgres')
        database_url = f"postgresql://{user}:{pwd}@{host}:5432/{db}"

    logging.info(f"Reading CSV: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    cleaned = clean_dataframe(df)

    conn = psycopg2.connect(database_url)
    ensure_table(conn)
    create_partitions(conn, cleaned)
    inserted = insert_rows(conn, cleaned)
    logging.info(f"Rows inserted: {inserted}")

    create_or_replace_view(conn)
    logging.info("View accidents_monthly_stats created/updated")

    conn.close()


if __name__ == '__main__':
    try:
        main()
    except FileNotFoundError as e:
        logging.error(f"FATAL ERROR - File not found: {str(e)}", exc_info=True)
        sys.exit(1)
    except psycopg2.OperationalError as e:
        logging.error(f"FATAL ERROR - Database connection failed: {str(e)}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logging.error(f"FATAL ERROR - Unexpected exception: {str(e)}", exc_info=True)
        sys.exit(1)
