from pathlib import Path
import logging
import os
import secrets
import time
import uuid
from datetime import datetime, timedelta, timezone

import pandas as pd
from sqlalchemy import text

from .config import Settings
from .database import create_db_engine

LOGGER = logging.getLogger(__name__)
QUALITY_FAILURE_MODE = os.getenv("QUALITY_FAILURE_MODE", "STOP").upper()
QUALITY_RULE_VERSION = os.getenv("QUALITY_RULE_VERSION", "1.0.0")
REQUIRED_SOURCE_COLUMNS = {
    "Row ID", "Order ID", "Order Date", "Ship Date", "Customer ID",
    "Product ID", "Sales", "Quantity", "Discount", "Profit",
}


def validate_source_schema(columns) -> None:
    """Fail fast when the source contract changes unexpectedly."""
    missing = REQUIRED_SOURCE_COLUMNS.difference(columns)
    if missing:
        raise ValueError(
            "Source schema validation failed; missing columns: "
            + ", ".join(sorted(missing))
        )


def ensure_watermark_table(engine) -> None:
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE SCHEMA IF NOT EXISTS control;
            CREATE TABLE IF NOT EXISTS control.pipeline_watermarks (
                pipeline_name TEXT PRIMARY KEY,
                watermark_date DATE,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """))


def read_watermark(engine, pipeline_name: str):
    with engine.connect() as connection:
        return connection.execute(text("""
            SELECT watermark_date
            FROM control.pipeline_watermarks
            WHERE pipeline_name = :pipeline_name
        """), {"pipeline_name": pipeline_name}).scalar_one_or_none()


def advance_watermark(engine, pipeline_name: str, watermark_date) -> None:
    if watermark_date is None:
        return
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO control.pipeline_watermarks
                (pipeline_name, watermark_date)
            VALUES (:pipeline_name, :watermark_date)
            ON CONFLICT (pipeline_name) DO UPDATE SET
                watermark_date = GREATEST(
                    control.pipeline_watermarks.watermark_date,
                    EXCLUDED.watermark_date
                ),
                updated_at = CURRENT_TIMESTAMP
        """), {"pipeline_name": pipeline_name, "watermark_date": watermark_date})


def new_run_id() -> str:
    """Return an RFC 9562 version-7 UUID without external dependencies."""
    timestamp_ms = time.time_ns() // 1_000_000
    random_bits = secrets.randbits(76)
    value = (timestamp_ms << 80) | (0x7 << 76) | random_bits
    value = (value & ~(0b11 << 62)) | (0b10 << 62)
    return str(uuid.UUID(int=value))


def run_bronze(engine, source_path: Path, load_mode: str = "full", run_id: str | None = None) -> int:
    if load_mode not in {"full", "append", "upsert", "snapshot"}:
        raise ValueError(f"Unsupported load mode: {load_mode}")
    LOGGER.info("Loading %s into bronze.superstore", source_path)
    frame = pd.read_csv(source_path, encoding="latin-1")
    validate_source_schema(frame.columns)
    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
    if load_mode == "snapshot":
        snapshot = frame.copy()
        snapshot["snapshot_run_id"] = run_id
        snapshot["snapshot_at"] = datetime.now(timezone.utc)
        snapshot.to_sql("superstore_snapshots", engine, schema="bronze", if_exists="append", index=False)
    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        connection.execute(text("DROP TABLE IF EXISTS bronze.superstore_stage"))
    frame.to_sql("superstore_stage", engine, schema="bronze", if_exists="replace", index=False)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS bronze.superstore
            (LIKE bronze.superstore_stage INCLUDING DEFAULTS);
        """))
        if load_mode == "full" or load_mode == "snapshot":
            connection.execute(text("TRUNCATE TABLE bronze.superstore"))
        elif load_mode == "upsert":
            connection.execute(text("""
                DELETE FROM bronze.superstore target
                USING bronze.superstore_stage stage
                WHERE target."Row ID" = stage."Row ID"
            """))
        connection.execute(text("""
            INSERT INTO bronze.superstore
            SELECT DISTINCT ON ("Row ID") *
            FROM bronze.superstore_stage
            ORDER BY "Row ID";
            DROP TABLE bronze.superstore_stage;
            COMMENT ON TABLE bronze.superstore IS
                'Raw source snapshot loaded through a staging table.';
        """))
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
    COMMENT ON TABLE silver.superstore IS
        'Grain: one row per source transaction row_id.';
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
        silver_count = connection.execute(text("SELECT COUNT(*) FROM silver.superstore")).scalar_one()
        bronze_count = connection.execute(text("""
            SELECT COUNT(*)
            FROM bronze.superstore
            WHERE "Order ID" IS NOT NULL
              AND (:start_date IS NULL OR TO_DATE("Order Date", 'MM/DD/YYYY') >= CAST(:start_date AS DATE))
              AND (:end_date IS NULL OR TO_DATE("Order Date", 'MM/DD/YYYY') <= CAST(:end_date AS DATE))
        """), {"start_date": start_date, "end_date": end_date}).scalar_one()
        if silver_count != bronze_count:
            raise RuntimeError(
                f"Bronze/Silver reconciliation failed: bronze={bronze_count}, silver={silver_count}"
            )
        return silver_count


def count_source_rows(engine, start_date=None, end_date=None) -> int:
    """Count source rows in the requested window before replacing Silver."""
    with engine.connect() as connection:
        return connection.execute(text("""
            SELECT COUNT(*)
            FROM bronze.superstore
            WHERE "Order ID" IS NOT NULL
              AND (:start_date IS NULL OR TO_DATE("Order Date", 'MM/DD/YYYY') >= CAST(:start_date AS DATE))
              AND (:end_date IS NULL OR TO_DATE("Order Date", 'MM/DD/YYYY') <= CAST(:end_date AS DATE))
        """), {"start_date": start_date, "end_date": end_date}).scalar_one()


def mark_pipeline_run(engine, run_id: str, status: str, bronze_rows: int,
                       silver_rows: int, gold_rows: int) -> None:
    with engine.begin() as connection:
        connection.execute(text("""
            UPDATE control.pipeline_runs
            SET finished_at = :finished_at, status = :status,
                bronze_rows = :bronze_rows, silver_rows = :silver_rows,
                gold_rows = :gold_rows
            WHERE run_id = :run_id
        """), {
            "finished_at": datetime.now(timezone.utc),
            "status": status,
            "bronze_rows": bronze_rows,
            "silver_rows": silver_rows,
            "gold_rows": gold_rows,
            "run_id": run_id,
        })


def run_gold(engine) -> int:
    sql = """
    CREATE SCHEMA IF NOT EXISTS gold;
    DROP TABLE IF EXISTS gold.fact_sales;
    DROP TABLE IF EXISTS gold.sales_daily;
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

    CREATE TABLE gold.fact_sales_stage AS
    SELECT s.row_id, s.order_id, s.order_date, s.ship_date,
           c.customer_key, p.product_key, l.location_id,
           s.ship_mode, s.sales, s.quantity, s.discount, s.profit,
           s.run_id, s.ingested_at
    FROM silver.superstore s
    JOIN gold.dim_customers c ON s.customer_id = c.customer_id
    JOIN gold.dim_products p ON s.product_id = p.product_id
    LEFT JOIN gold.dim_location l USING (country, region, state, city, postal_code);
    CREATE TABLE gold.fact_sales (LIKE gold.fact_sales_stage INCLUDING DEFAULTS)
        PARTITION BY RANGE (order_date);
    CREATE TABLE gold.fact_sales_default PARTITION OF gold.fact_sales DEFAULT;
    INSERT INTO gold.fact_sales SELECT * FROM gold.fact_sales_stage;
    DROP TABLE gold.fact_sales_stage;
    ALTER TABLE gold.fact_sales ADD PRIMARY KEY (row_id, order_date);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_customer FOREIGN KEY (customer_key) REFERENCES gold.dim_customers(customer_key);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_product FOREIGN KEY (product_key) REFERENCES gold.dim_products(product_key);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_location FOREIGN KEY (location_id) REFERENCES gold.dim_location(location_id);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_order_date FOREIGN KEY (order_date) REFERENCES gold.dim_date(date_key);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_ship_date FOREIGN KEY (ship_date) REFERENCES gold.dim_date(date_key);
    CREATE INDEX idx_fact_customer ON gold.fact_sales(customer_key);
    CREATE INDEX idx_fact_product ON gold.fact_sales(product_key);
    CREATE INDEX idx_fact_location ON gold.fact_sales(location_id);
    CREATE INDEX idx_fact_order_date ON gold.fact_sales(order_date);
    COMMENT ON TABLE gold.fact_sales IS
        'Grain: one row per source transaction row_id, partitioned by order_date.';
    CREATE TABLE gold.sales_daily AS
    SELECT order_date, customer_key, product_key, location_id,
           SUM(sales) AS sales, SUM(quantity) AS quantity,
           SUM(profit) AS profit, COUNT(*) AS transaction_count
    FROM gold.fact_sales
    GROUP BY order_date, customer_key, product_key, location_id;
    COMMENT ON TABLE gold.sales_daily IS
        'Semantic serving grain: one row per order_date, customer, product and location.';
    """
    with engine.begin() as connection:
        connection.execute(text(sql))
        boundary_match = connection.execute(text("""
            SELECT ABS(COALESCE(f.sales, 0) - COALESCE(d.sales, 0)) <= 0.000001
               AND ABS(COALESCE(f.profit, 0) - COALESCE(d.profit, 0)) <= 0.000001
               AND COALESCE(f.quantity, 0) = COALESCE(d.quantity, 0)
               AND f.rows = d.rows
            FROM (SELECT SUM(sales) sales, SUM(profit) profit,
                         SUM(quantity) quantity, COUNT(*) rows
                  FROM gold.fact_sales) f
            CROSS JOIN
                 (SELECT SUM(sales) sales, SUM(profit) profit,
                         SUM(quantity) quantity, SUM(transaction_count) rows
                  FROM gold.sales_daily) d
        """)).scalar_one()
        if not boundary_match:
            raise RuntimeError(
                "Fact/serving reconciliation failed"
            )
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
            CREATE TABLE IF NOT EXISTS control.data_contract_rules (
                rule_name TEXT PRIMARY KEY,
                contract_name TEXT NOT NULL,
                owner TEXT NOT NULL,
                severity TEXT NOT NULL CHECK (severity IN ('STOP', 'WARN', 'QUARANTINE')),
                rule_version TEXT NOT NULL,
                description TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """))
        connection.execute(text("""
            INSERT INTO control.data_contract_rules
                (rule_name, contract_name, owner, severity, rule_version, description)
            VALUES
                ('silver/gold row count', 'superstore_sales',
                 'data-engineering', 'STOP', :version,
                 'Silver and Gold must contain the same number of rows'),
                ('null location keys', 'superstore_sales',
                 'data-engineering', 'STOP', :version,
                 'Published fact rows must resolve to a location key'),
                ('invalid measures', 'superstore_sales',
                 'data-quality', 'QUARANTINE', :version,
                 'Sales, quantity and discount must satisfy business constraints'),
                ('silver/gold totals', 'superstore_sales',
                 'finance-data-owner', 'STOP', :version,
                 'Silver and Gold measures must reconcile within tolerance')
            ON CONFLICT (rule_name) DO UPDATE SET
                owner = EXCLUDED.owner,
                severity = EXCLUDED.severity,
                rule_version = EXCLUDED.rule_version,
                description = EXCLUDED.description,
                updated_at = CURRENT_TIMESTAMP
        """), {"version": QUALITY_RULE_VERSION})
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS control.data_quality_results (
                run_id UUID, rule_name TEXT, passed BOOLEAN NOT NULL,
                rule_version TEXT NOT NULL DEFAULT '1.0.0',
                checked_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_id, rule_name)
            )
        """))
        connection.execute(text("""
            ALTER TABLE control.data_quality_results
                ADD COLUMN IF NOT EXISTS rule_version TEXT NOT NULL DEFAULT '1.0.0'
        """))
        connection.execute(text("""
            INSERT INTO control.data_quality_results
                (run_id, rule_name, passed, rule_version)
            VALUES (:run_id, :rule_name, :passed, :version)
            ON CONFLICT (run_id, rule_name) DO UPDATE SET passed = EXCLUDED.passed,
                rule_version = EXCLUDED.rule_version,
                checked_at = CURRENT_TIMESTAMP
        """), [{"run_id": run_id, "rule_name": name, "passed": passed}
               | {"version": QUALITY_RULE_VERSION}
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
        message = (
            f"Data quality checks failed under {QUALITY_FAILURE_MODE} policy "
            f"(version {QUALITY_RULE_VERSION}): " + ", ".join(failed)
        )
        if QUALITY_FAILURE_MODE == "WARN":
            LOGGER.warning(message)
        elif QUALITY_FAILURE_MODE == "QUARANTINE":
            LOGGER.error(message + "; invalid rows were routed to quarantine")
        else:
            raise RuntimeError(message)
    LOGGER.info("All data quality checks passed")


def run(source_path: Path, start_date=None, end_date=None, replay=False,
        load_mode: str = "full") -> None:
    settings = Settings.from_env()
    engine = create_db_engine(settings)
    run_id = new_run_id()
    started_at = datetime.now(timezone.utc)
    pipeline_name = "retailion_superstore"
    try:
        ensure_watermark_table(engine)
        if load_mode in {"append", "upsert"} and not replay and start_date is None:
            watermark = read_watermark(engine, pipeline_name)
            if watermark is not None:
                start_date = watermark + timedelta(days=1)
                LOGGER.info("Using watermark start date: %s", start_date)
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
                VALUES (:run_id, :pipeline_name, :started_at, 'STARTED')
            """), {"run_id": run_id, "pipeline_name": pipeline_name, "started_at": started_at})
        # A replay/backfill is explicit: it bypasses any future watermark state
        # and is still bounded by the optional date window.
        bronze_count = run_bronze(engine, source_path, load_mode, run_id)
        source_window_count = count_source_rows(engine, start_date, end_date)
        if source_window_count == 0 and load_mode in {"append", "upsert"} and not replay:
            with engine.connect() as connection:
                gold_exists = connection.execute(text(
                    "SELECT to_regclass('gold.fact_sales') IS NOT NULL"
                )).scalar_one()
                existing_gold_count = connection.execute(text(
                    "SELECT COUNT(*) FROM gold.fact_sales"
                )).scalar_one() if gold_exists else 0
            mark_pipeline_run(
                engine, run_id, "NOOP", bronze_count, 0, existing_gold_count or 0
            )
            LOGGER.info(
                "No source rows after watermark; pipeline completed as NOOP run_id=%s",
                run_id,
            )
            return
        silver_count = run_silver(engine, run_id, source_path.name, start_date, end_date)
        gold_count = run_gold(engine)
        validate(engine, silver_count, gold_count, run_id)
        with engine.connect() as connection:
            max_date = connection.execute(text(
                "SELECT MAX(order_date) FROM silver.superstore"
            )).scalar_one()
        with engine.begin() as connection:
            if not replay and max_date is not None:
                connection.execute(text("""
                    INSERT INTO control.pipeline_watermarks
                        (pipeline_name, watermark_date)
                    VALUES (:pipeline_name, :watermark_date)
                    ON CONFLICT (pipeline_name) DO UPDATE SET
                        watermark_date = GREATEST(
                            control.pipeline_watermarks.watermark_date,
                            EXCLUDED.watermark_date
                        ),
                        updated_at = CURRENT_TIMESTAMP
                """), {"pipeline_name": pipeline_name, "watermark_date": max_date})
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
