# DuckDB to PostgreSQL Migration Guide

## Overview

This project has been migrated from DuckDB (embedded file-based database) to PostgreSQL (server-based database). All notebooks have been updated to use PostgreSQL with SQLAlchemy as the ORM layer.

## Changes Made

### 1. Dependencies (`requirements.txt`)
- **Removed:** `duckdb`
- **Added:** `psycopg2-binary`, `sqlalchemy`, `python-dotenv`

### 2. Connection Layer
All three notebooks now use:
- **SQLAlchemy** engine with psycopg2 driver
- **python-dotenv** for environment variable management
- Connection strings built from environment variables

### 3. CSV Data Loading (`01_bronze.ipynb`)
- **Old:** `read_csv_auto()` (DuckDB-specific)
- **New:** `pandas.read_csv()` + `df.to_sql()` for bulk loading

### 4. SQL Syntax Changes

#### CREATE OR REPLACE TABLE → DROP TABLE IF EXISTS
```sql
-- Old (DuckDB)
CREATE OR REPLACE TABLE schema.table_name AS
SELECT ...

-- New (PostgreSQL)
DROP TABLE IF EXISTS schema.table_name;
CREATE TABLE schema.table_name AS
SELECT ...
```

#### Date Functions
| DuckDB | PostgreSQL |
|--------|------------|
| `YEAR(date)` | `EXTRACT(YEAR FROM date)::INT` |
| `MONTH(date)` | `EXTRACT(MONTH FROM date)::INT` |
| `DAY(date)` | `EXTRACT(DAY FROM date)::INT` |
| `QUARTER(date)` | `EXTRACT(QUARTER FROM date)::INT` |
| `DAYOFWEEK(date)` | `EXTRACT(DOW FROM date)::INT` |
| `DAYNAME(date)` | `TRIM(TO_CHAR(date, 'Day'))` |
| `MONTHNAME(date)` | `TRIM(TO_CHAR(date, 'Month'))` |

#### Result Materialization
```python
# Old (DuckDB)
con.execute(sql).df()

# New (PostgreSQL + pandas)
pd.read_sql(sql, engine)
```

#### Data Type Casting
```sql
-- DuckDB
CAST(column AS DOUBLE)

-- PostgreSQL
CAST(column AS DOUBLE PRECISION)
```

### 5. Window Functions
- **Old:** `ROW_NUMBER() OVER ()` (non-deterministic)
- **New:** `ROW_NUMBER() OVER (ORDER BY ...)` (deterministic)

## Setup Instructions

### Prerequisites
1. **PostgreSQL installed and running** on your local machine
   - Default: `localhost:5432`
   - Create a database: `createdb retailion`

2. **Python 3.9+** with dependencies installed
   ```bash
   pip install -r requirements.txt
   ```

### Configuration

1. **Copy the environment template:**
   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` with your PostgreSQL credentials:**
   ```
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=retailion
   DB_USER=postgres
   DB_PASSWORD=<your_password>
   ```

3. **Ensure `.env` is in `.gitignore`** (already configured)

### Running the Notebooks

Run the notebooks in order (prerequisite dependencies):

1. **`01_bronze.ipynb`** - Raw data ingestion
   - Loads CSV into `bronze.superstore` table
   - Creates `bronze` schema

2. **`02_silver.ipynb`** - Data cleaning and standardization
   - Transforms `bronze` data
   - Creates `silver.superstore` with proper types and naming conventions
   - Requires: completed `01_bronze.ipynb`

3. **`03_gold.ipynb`** - Dimensional modeling
   - Creates star schema with 4 dimensions + 1 fact table
   - Ready for BI tool integration
   - Requires: completed `02_silver.ipynb`

### Verification

After running all notebooks, verify the migration:

```sql
-- Check schemas exist
\dn

-- Check table counts
SELECT COUNT(*) FROM bronze.superstore;      -- Should be 9,627 rows
SELECT COUNT(*) FROM silver.superstore;      -- Should be 9,627 rows
SELECT COUNT(*) FROM gold.fact_sales;        -- Should be 9,627 rows

-- Check dimensions
SELECT COUNT(*) FROM gold.dim_customers;     -- 787 unique customers
SELECT COUNT(*) FROM gold.dim_products;      -- 1,832 unique products
SELECT COUNT(*) FROM gold.dim_location;      -- 628 unique locations
SELECT COUNT(*) FROM gold.dim_date;          -- 1,430 unique dates
```

## Files Modified

| File | Changes |
|------|---------|
| `requirements.txt` | Replaced duckdb with psycopg2-binary, sqlalchemy, python-dotenv |
| `notebooks/01_bronze.ipynb` | Connection setup, CSV loading via pandas |
| `notebooks/02_silver.ipynb` | Connection setup, DROP TABLE IF EXISTS syntax |
| `notebooks/03_gold.ipynb` | Connection setup, date functions, window functions |
| `.gitignore` | New file (excludes .env, *.duckdb) |
| `.env.example` | New file (template for connection config) |

## Files Removed

- `data/retailion.duckdb` - No longer needed (to be deleted after verification)

## Troubleshooting

### Error: "Connection refused"
- Ensure PostgreSQL is running: `pg_isready -h localhost -p 5432`
- Check DB_HOST and DB_PORT in `.env`

### Error: "Database retailion does not exist"
- Create the database: `createdb retailion`

### Error: "psycopg2 not found"
- Install dependencies: `pip install -r requirements.txt`

### Error: "No such file or directory: '.env'"
- Create `.env` from `.env.example`: `cp .env.example .env`

## Rollback

If needed, the original DuckDB database (`data/retailion.duckdb`) can be kept temporarily as a reference for comparison.

## Notes

- All date functions now use PostgreSQL `EXTRACT()` and `TO_CHAR()`
- Window functions now include explicit `ORDER BY` for determinism
- NUMERIC rounding in SQL queries uses `::NUMERIC` for precise decimal arithmetic
- The data warehouse is now production-ready for live BI tool integration
