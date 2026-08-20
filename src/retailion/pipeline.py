from pathlib import Path
import logging
from datetime import datetime, timezone
from uuid import uuid4

import pandas as pd
from sqlalchemy import text

from .config import Settings
from .database import create_db_engine

LOGGER = logging.getLogger(__name__)


def run_bronze(engine, source_path: Path) -> int:
    LOGGER.info("Loading %s into bronze.superstore", source_path)
    frame = pd.read_csv(source_path, encoding="latin-1")
    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
    frame.to_sql("superstore", engine, schema="bronze", if_exists="replace", index=False)
    return len(frame)


def run_silver(engine, run_id: str, source_file: str, start_date=None, end_date=None) -> int:
    sql = """
    CREATE SCHEMA IF NOT EXISTS silver;
    DROP TABLE IF EXISTS silver.superstore;
    CREATE TABLE silver.superstore AS
    SELECT
        CAST("Row ID" AS INTEGER) AS row_id,
        "Order ID" AS order_id,
        TO_DATE("Order Date", 'MM/DD/YYYY') AS order_date,
        TO_DATE("Ship Date", 'MM/DD/YYYY') AS ship_date,
        "Ship Mode" AS ship_mode,
        "Customer ID" AS customer_id,
        TRIM("Customer Name") AS customer_name,
        "Segment" AS segment,
        "Country" AS country,
        "City" AS city,
        "State" AS state,
        COALESCE(CAST("Postal Code" AS VARCHAR), '00000') AS postal_code,
        "Region" AS region,
        "Product ID" AS product_id,
        "Category" AS category,
        "Sub-Category" AS sub_category,
        "Product Name" AS product_name,
        CAST("Sales" AS DOUBLE PRECISION) AS sales,
        CAST("Quantity" AS INTEGER) AS quantity,
        CAST("Discount" AS DOUBLE PRECISION) AS discount,
        CAST("Profit" AS DOUBLE PRECISION) AS profit,
        CURRENT_TIMESTAMP AS ingested_at,
        :run_id AS run_id,
        :source_file AS source_file,
        '1.0.0' AS pipeline_version
    FROM bronze.superstore
    WHERE "Order ID" IS NOT NULL
      AND (:start_date IS NULL OR TO_DATE("Order Date", 'MM/DD/YYYY') >= CAST(:start_date AS DATE))
      AND (:end_date IS NULL OR TO_DATE("Order Date", 'MM/DD/YYYY') <= CAST(:end_date AS DATE));
    """
    with engine.begin() as connection:
        connection.execute(text(sql), {"run_id": run_id, "source_file": source_file,
                                       "start_date": start_date, "end_date": end_date})
        connection.execute(text("ALTER TABLE silver.superstore ADD PRIMARY KEY (row_id)"))
        connection.execute(text("""
            CREATE SCHEMA IF NOT EXISTS quarantine;
            CREATE TABLE IF NOT EXISTS quarantine.superstore_invalid AS
            SELECT *, CAST(NULL AS TEXT) AS reason
            FROM silver.superstore WITH NO DATA;
            ALTER TABLE quarantine.superstore_invalid
                ADD COLUMN IF NOT EXISTS reason TEXT;
        """))
        connection.execute(text("""
            INSERT INTO quarantine.superstore_invalid
            SELECT s.*,
                   CASE WHEN quantity <= 0 THEN 'quantity <= 0'
                        WHEN discount NOT BETWEEN 0 AND 1 THEN 'discount outside [0,1]'
                        WHEN sales < 0 THEN 'sales < 0' END
            FROM silver.superstore s
            WHERE quantity <= 0 OR discount NOT BETWEEN 0 AND 1 OR sales < 0
        """), {"run_id": run_id})
        return connection.execute(text("SELECT COUNT(*) FROM silver.superstore")).scalar_one()


def run_gold(engine) -> int:
    sql = """
    CREATE SCHEMA IF NOT EXISTS gold;
    DROP TABLE IF EXISTS gold.fact_sales;
    DROP TABLE IF EXISTS gold.dim_date;
    DROP TABLE IF EXISTS gold.dim_location;
    DROP TABLE IF EXISTS gold.dim_products;
    DROP TABLE IF EXISTS gold.dim_customers;

    CREATE TABLE gold.dim_customers AS
    SELECT ROW_NUMBER() OVER (ORDER BY customer_id)::INTEGER AS customer_key,
           customer_id, customer_name, segment, ingested_at
    FROM (
        SELECT DISTINCT ON (customer_id) customer_id, customer_name, segment, ingested_at
        FROM silver.superstore
        ORDER BY customer_id, ingested_at DESC
    ) latest_customers;
    ALTER TABLE gold.dim_customers ADD PRIMARY KEY (customer_key);
    ALTER TABLE gold.dim_customers ADD CONSTRAINT uq_dim_customers_business_key UNIQUE (customer_id);

    CREATE TABLE IF NOT EXISTS gold.dim_customers_scd2 (
        customer_key BIGSERIAL PRIMARY KEY,
        customer_id TEXT NOT NULL,
        customer_name TEXT,
        segment TEXT,
        valid_from TIMESTAMPTZ NOT NULL,
        valid_to TIMESTAMPTZ,
        is_current BOOLEAN NOT NULL,
        UNIQUE (customer_id, valid_from)
    );
    ALTER TABLE gold.dim_customers_scd2
        ADD COLUMN IF NOT EXISTS customer_key BIGINT;
    CREATE SEQUENCE IF NOT EXISTS gold.dim_customers_scd2_customer_key_seq;
    ALTER TABLE gold.dim_customers_scd2
        ALTER COLUMN customer_key SET DEFAULT
            nextval('gold.dim_customers_scd2_customer_key_seq');
    SELECT setval(
        'gold.dim_customers_scd2_customer_key_seq',
        COALESCE((SELECT MAX(customer_key) FROM gold.dim_customers_scd2), 0) + 1,
        FALSE
    );
    UPDATE gold.dim_customers_scd2 history
    SET valid_to = CURRENT_TIMESTAMP,
        is_current = FALSE
    FROM gold.dim_customers current_dim
    WHERE history.customer_id = current_dim.customer_id
      AND history.is_current
      AND (history.customer_name, history.segment)
          IS DISTINCT FROM (current_dim.customer_name, current_dim.segment);
    INSERT INTO gold.dim_customers_scd2
        (customer_id, customer_name, segment, valid_from, valid_to, is_current)
    SELECT current_dim.customer_id, current_dim.customer_name, current_dim.segment,
           current_dim.ingested_at, NULL, TRUE
    FROM gold.dim_customers current_dim
    WHERE NOT EXISTS (
        SELECT 1
        FROM gold.dim_customers_scd2 history
        WHERE history.customer_id = current_dim.customer_id
          AND history.valid_from = current_dim.ingested_at
    );

    CREATE TABLE gold.dim_products AS
    SELECT ROW_NUMBER() OVER (ORDER BY product_id)::INTEGER AS product_key,
           product_id, product_name, category, sub_category, ingested_at
    FROM (
        SELECT DISTINCT ON (product_id) product_id, product_name, category, sub_category, ingested_at
        FROM silver.superstore
        ORDER BY product_id, ingested_at DESC
    ) latest_products;
    ALTER TABLE gold.dim_products ADD PRIMARY KEY (product_key);
    ALTER TABLE gold.dim_products ADD CONSTRAINT uq_dim_products_business_key UNIQUE (product_id);

    CREATE TABLE gold.dim_location AS
    SELECT ROW_NUMBER() OVER (ORDER BY country, region, state, city, postal_code)::INTEGER AS location_id,
           country, region, state, city, postal_code
    FROM (SELECT DISTINCT country, region, state, city, postal_code FROM silver.superstore) locations;
    ALTER TABLE gold.dim_location ADD PRIMARY KEY (location_id);

    CREATE TABLE gold.dim_date AS
    WITH dates AS (
        SELECT order_date AS date_key FROM silver.superstore
        UNION
        SELECT ship_date FROM silver.superstore
    )
    SELECT date_key, EXTRACT(YEAR FROM date_key)::INTEGER AS year,
           EXTRACT(MONTH FROM date_key)::INTEGER AS month,
           EXTRACT(DAY FROM date_key)::INTEGER AS day,
           EXTRACT(QUARTER FROM date_key)::INTEGER AS quarter,
           TRIM(TO_CHAR(date_key, 'Day')) AS day_name,
           TRIM(TO_CHAR(date_key, 'Month')) AS month_name,
           EXTRACT(DOW FROM date_key) IN (0, 6) AS is_weekend
    FROM dates WHERE date_key IS NOT NULL;
    ALTER TABLE gold.dim_date ADD PRIMARY KEY (date_key);

    CREATE TABLE gold.fact_sales AS
    SELECT s.row_id, s.order_id, s.order_date, s.ship_date,
           c.customer_key, p.product_key, l.location_id,
           s.ship_mode, s.sales, s.quantity, s.discount, s.profit,
           s.run_id, s.ingested_at
    FROM silver.superstore s
    JOIN gold.dim_customers c ON s.customer_id = c.customer_id
    JOIN gold.dim_products p ON s.product_id = p.product_id
    LEFT JOIN gold.dim_location l USING (country, region, state, city, postal_code);
    ALTER TABLE gold.fact_sales ADD PRIMARY KEY (row_id);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_customer FOREIGN KEY (customer_key) REFERENCES gold.dim_customers(customer_key);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_product FOREIGN KEY (product_key) REFERENCES gold.dim_products(product_key);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_location FOREIGN KEY (location_id) REFERENCES gold.dim_location(location_id);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_order_date FOREIGN KEY (order_date) REFERENCES gold.dim_date(date_key);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_ship_date FOREIGN KEY (ship_date) REFERENCES gold.dim_date(date_key);
    CREATE INDEX idx_fact_customer ON gold.fact_sales(customer_key);
    CREATE INDEX idx_fact_product ON gold.fact_sales(product_key);
    CREATE INDEX idx_fact_location ON gold.fact_sales(location_id);
    CREATE INDEX idx_fact_order_date ON gold.fact_sales(order_date);
    """
    with engine.begin() as connection:
        connection.execute(text(sql))
        return connection.execute(text("SELECT COUNT(*) FROM gold.fact_sales")).scalar_one()


def validate(engine, silver_count: int, gold_count: int, run_id: str) -> None:
    with engine.connect() as connection:
        null_locations = connection.execute(text(
            "SELECT COUNT(*) = 0 FROM gold.fact_sales WHERE location_id IS NULL"
        )).scalar_one()
        valid_measures = connection.execute(text(
            "SELECT COUNT(*) = 0 FROM gold.fact_sales WHERE sales < 0 OR quantity <= 0 OR discount NOT BETWEEN 0 AND 1"
        )).scalar_one()
        totals_match = connection.execute(text("""
            SELECT ABS(COALESCE(s.sales, 0) - COALESCE(g.sales, 0)) <= 0.000001
               AND ABS(COALESCE(s.profit, 0) - COALESCE(g.profit, 0)) <= 0.000001
               AND COALESCE(s.quantity, 0) = COALESCE(g.quantity, 0)
            FROM (
                SELECT SUM(sales) AS sales, SUM(profit) AS profit, SUM(quantity) AS quantity
                FROM silver.superstore
            ) s
            CROSS JOIN (
                SELECT SUM(sales) AS sales, SUM(profit) AS profit, SUM(quantity) AS quantity
                FROM gold.fact_sales
            ) g
        """)).scalar_one()
        checks = {
        "silver/gold row count": silver_count == gold_count,
        "null location keys": null_locations,
        "invalid measures": valid_measures,
        "silver/gold totals": totals_match,
    }
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS control.data_quality_results (
                run_id UUID, rule_name TEXT, passed BOOLEAN NOT NULL,
                checked_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_id, rule_name)
            )
        """))
        connection.execute(text("""
            INSERT INTO control.data_quality_results (run_id, rule_name, passed)
            VALUES (:run_id, :rule_name, :passed)
            ON CONFLICT (run_id, rule_name) DO UPDATE SET passed = EXCLUDED.passed,
                checked_at = CURRENT_TIMESTAMP
        """), [{"run_id": run_id, "rule_name": name, "passed": passed}
               for name, passed in checks.items()])
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS control.reconciliation_results (
                run_id UUID PRIMARY KEY, silver_rows BIGINT, gold_rows BIGINT,
                silver_sales NUMERIC, gold_sales NUMERIC,
                silver_profit NUMERIC, gold_profit NUMERIC,
                silver_quantity BIGINT, gold_quantity BIGINT,
                passed BOOLEAN NOT NULL, checked_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """))
        connection.execute(text("""
            INSERT INTO control.reconciliation_results
            SELECT :run_id, s.rows, g.rows, s.sales, g.sales, s.profit, g.profit,
                   s.quantity, g.quantity, :passed, CURRENT_TIMESTAMP
            FROM (SELECT COUNT(*) rows, SUM(sales) sales, SUM(profit) profit, SUM(quantity) quantity FROM silver.superstore) s,
                 (SELECT COUNT(*) rows, SUM(sales) sales, SUM(profit) profit, SUM(quantity) quantity FROM gold.fact_sales) g
        """), {"run_id": run_id, "passed": checks["silver/gold totals"]})
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise RuntimeError("Data quality checks failed: " + ", ".join(failed))
    LOGGER.info("All data quality checks passed")


def run(source_path: Path, start_date=None, end_date=None) -> None:
    settings = Settings.from_env()
    engine = create_db_engine(settings)
    run_id = str(uuid4())
    started_at = datetime.now(timezone.utc)
    try:
        with engine.begin() as connection:
            connection.execute(text("CREATE SCHEMA IF NOT EXISTS control"))
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS control.pipeline_runs (
                    run_id UUID PRIMARY KEY,
                    pipeline_name VARCHAR(100) NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL,
                    finished_at TIMESTAMPTZ,
                    status VARCHAR(20) NOT NULL,
                    bronze_rows INTEGER,
                    silver_rows INTEGER,
                    gold_rows INTEGER,
                    error_message TEXT
                )
            """))
            connection.execute(text("""
                INSERT INTO control.pipeline_runs (run_id, pipeline_name, started_at, status)
                VALUES (:run_id, 'retailion_superstore', :started_at, 'STARTED')
            """), {"run_id": run_id, "started_at": started_at})
        bronze_count = run_bronze(engine, source_path)
        silver_count = run_silver(engine, run_id, source_path.name, start_date, end_date)
        gold_count = run_gold(engine)
        validate(engine, silver_count, gold_count, run_id)
        with engine.begin() as connection:
            connection.execute(text("""
                UPDATE control.pipeline_runs
                SET finished_at = :finished_at, status = 'SUCCESS',
                    bronze_rows = :bronze_rows, silver_rows = :silver_rows, gold_rows = :gold_rows
                WHERE run_id = :run_id
            """), {"finished_at": datetime.now(timezone.utc), "bronze_rows": bronze_count,
                   "silver_rows": silver_count, "gold_rows": gold_count, "run_id": run_id})
        LOGGER.info("Pipeline completed run_id=%s bronze=%s silver=%s gold=%s", run_id, bronze_count, silver_count, gold_count)
    except Exception as error:
        try:
            with engine.begin() as connection:
                connection.execute(text("""
                    UPDATE control.pipeline_runs
                    SET finished_at = :finished_at, status = 'FAILED', error_message = :error_message
                    WHERE run_id = :run_id
                """), {"finished_at": datetime.now(timezone.utc), "error_message": str(error), "run_id": run_id})
        except Exception:
            LOGGER.exception("Could not persist failed pipeline status run_id=%s", run_id)
        LOGGER.exception("Pipeline failed run_id=%s", run_id)
        raise
    finally:
        engine.dispose()
