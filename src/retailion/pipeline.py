from pathlib import Path
import logging

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


def run_silver(engine) -> int:
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
        CURRENT_TIMESTAMP AS ingested_at
    FROM bronze.superstore
    WHERE "Order ID" IS NOT NULL;
    """
    with engine.begin() as connection:
        connection.execute(text(sql))
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
    SELECT DISTINCT ON (customer_id) customer_id, customer_name, segment
    FROM silver.superstore
    ORDER BY customer_id, ingested_at DESC;
    ALTER TABLE gold.dim_customers ADD PRIMARY KEY (customer_id);

    CREATE TABLE gold.dim_products AS
    SELECT DISTINCT ON (product_id) product_id, product_name, category, sub_category
    FROM silver.superstore
    ORDER BY product_id, ingested_at DESC;
    ALTER TABLE gold.dim_products ADD PRIMARY KEY (product_id);

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
    SELECT s.row_id, s.order_id, s.order_date, s.ship_date, s.customer_id, s.product_id,
           l.location_id, s.ship_mode, s.sales, s.quantity, s.discount, s.profit
    FROM silver.superstore s
    LEFT JOIN gold.dim_location l USING (country, region, state, city, postal_code);
    ALTER TABLE gold.fact_sales ADD PRIMARY KEY (row_id);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_customer FOREIGN KEY (customer_id) REFERENCES gold.dim_customers(customer_id);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_product FOREIGN KEY (product_id) REFERENCES gold.dim_products(product_id);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_location FOREIGN KEY (location_id) REFERENCES gold.dim_location(location_id);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_order_date FOREIGN KEY (order_date) REFERENCES gold.dim_date(date_key);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_ship_date FOREIGN KEY (ship_date) REFERENCES gold.dim_date(date_key);
    CREATE INDEX idx_fact_customer ON gold.fact_sales(customer_id);
    CREATE INDEX idx_fact_product ON gold.fact_sales(product_id);
    CREATE INDEX idx_fact_location ON gold.fact_sales(location_id);
    CREATE INDEX idx_fact_order_date ON gold.fact_sales(order_date);
    """
    with engine.begin() as connection:
        connection.execute(text(sql))
        return connection.execute(text("SELECT COUNT(*) FROM gold.fact_sales")).scalar_one()


def validate(engine, silver_count: int, gold_count: int) -> None:
    with engine.connect() as connection:
        null_locations = connection.execute(text(
            "SELECT COUNT(*) = 0 FROM gold.fact_sales WHERE location_id IS NULL"
        )).scalar_one()
        valid_measures = connection.execute(text(
            "SELECT COUNT(*) = 0 FROM gold.fact_sales WHERE sales < 0 OR quantity <= 0 OR discount NOT BETWEEN 0 AND 1"
        )).scalar_one()
    checks = {
        "silver/gold row count": silver_count == gold_count,
        "null location keys": null_locations,
        "invalid measures": valid_measures,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise RuntimeError("Data quality checks failed: " + ", ".join(failed))
    LOGGER.info("All data quality checks passed")


def run(source_path: Path) -> None:
    settings = Settings.from_env()
    engine = create_db_engine(settings)
    try:
        bronze_count = run_bronze(engine, source_path)
        silver_count = run_silver(engine)
        gold_count = run_gold(engine)
        validate(engine, silver_count, gold_count)
        LOGGER.info("Pipeline completed: bronze=%s, silver=%s, gold=%s", bronze_count, silver_count, gold_count)
    finally:
        engine.dispose()
