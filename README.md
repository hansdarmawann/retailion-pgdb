# Retailion: Superstore Data Warehouse

A complete **data engineering project** demonstrating a modern data warehouse architecture using the **medallion pattern** (Bronze → Silver → Gold layers) with PostgreSQL, Python, and Jupyter notebooks.

**Data Source:** [Kaggle Superstore Dataset](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final)

---

## 📊 Project Overview

This project transforms raw superstore transaction data into a clean, analytical star schema optimized for business intelligence and reporting. The pipeline demonstrates industry-standard data engineering practices:

- **Raw data ingestion** (Bronze layer)
- **Data quality audits & transformation** (Silver layer)  
- **Dimensional modeling & analytics** (Gold layer)

**Dataset Size:** 9,627 transactions with 21 fields covering customers, products, locations, and financial metrics.

---

## 🏗️ Architecture: Medallion Pattern

The medallion architecture organizes data into three layers:

```
┌──────────────────────────────────────────────────────────────┐
│                     DATA WAREHOUSE LAYERS                    │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  BRONZE LAYER          SILVER LAYER          GOLD LAYER      │
│  ─────────────         ─────────────         ──────────      │
│                                                              │
│  • Raw CSV data       • Cleaned data        • Star Schema    │
│  • No transforms      • Type-casted         • Dimensions     │
│  • Minimal QA         • Audited & QA'd      • Facts          │
│  • Audit trail        • Standardized        • Optimized      │
│                       • EDA complete       • Analytics-ready │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### Layer Details

| Layer | Purpose | Key Operations | Output |
|-------|---------|-----------------|--------|
| **Bronze** | Raw landing zone | CSV → Table, minimal errors | `bronze.superstore` |
| **Silver** | Trusted zone | Cleaning, QA, exploration | `silver.superstore` |
| **Gold** | Analytics zone | Dimensional modeling | Star schema (dims + facts) |

---

## 📁 Project Structure

```
retailion/
├── README.md                          # This file
├── MIGRATION_GUIDE.md                 # DuckDB → PostgreSQL migration notes
├── requirements.txt                   # Python dependencies
├── .env.example                       # PostgreSQL connection template
├── .gitignore                         # Git ignore rules
│
├── notebooks/                         # Jupyter notebooks (layer-by-layer)
│   ├── 01_bronze.ipynb               # Raw data ingestion
│   ├── 02_silver.ipynb               # Data cleaning & EDA
│   └── 03_gold.ipynb                 # Dimensional modeling & analytics
│
└── data/                              # Data directory
    └── Sample - Superstore.csv       # Raw CSV source (9,627 rows)
```

**Note:** PostgreSQL stores all data on a running server. The `.env` file contains connection credentials and should not be committed to version control.

---

## 🚀 Quick Start

### Prerequisites

- Python 3.9+ 
- Jupyter Notebook or Jupyter Lab
- PostgreSQL 12+ (installed and running locally)
- Dependencies: psycopg2-binary, sqlalchemy, pandas, matplotlib, seaborn (see requirements.txt)

### Installation

```bash
# 1. Clone/navigate to project directory
cd retailion

# 2. Ensure PostgreSQL is running
# On Windows: Start PostgreSQL service
# On macOS: brew services start postgresql
# On Linux: sudo systemctl start postgresql

# 3. Create database
createdb retailion

# 4. Configure environment
cp .env.example .env
# Edit .env with your PostgreSQL credentials

# 5. Install dependencies
pip install -r requirements.txt

# 6. Start Jupyter
jupyter notebook
```

### Running the Pipeline

Execute notebooks in order:

1. **`01_bronze.ipynb`** → Loads raw CSV into PostgreSQL bronze schema
2. **`02_silver.ipynb`** → Cleans data, performs QA, explores patterns
3. **`03_gold.ipynb`** → Builds star schema, validates, runs sample queries

**Total Runtime:** ~5-10 minutes (mostly for visualizations in silver layer)

**Important:** Ensure your `.env` file is configured with PostgreSQL credentials before running.

### Verify PostgreSQL Setup

Before running notebooks, verify PostgreSQL is ready:

```bash
# Test PostgreSQL connection
psql -h localhost -U postgres -d postgres -c "SELECT version();"

# Create the database if it doesn't exist
createdb retailion

# Verify database was created
psql -l | grep retailion
```

If you get connection errors, check:
- ✅ PostgreSQL service is running (`sudo systemctl status postgresql` on Linux)
- ✅ Correct host/port in `.env` file (default: localhost:5432)
- ✅ Database user exists and password is correct
- ✅ Database `retailion` exists (`createdb retailion`)

---

## 🔧 Troubleshooting

### "FATAL: role 'postgres' does not exist"
**Solution:** Check PostgreSQL installation. On Windows, default user might be different. Verify with:
```bash
psql -U postgres  # Try default user
psql -U [your_username]  # Or use your Windows username
```

### "ERROR: could not connect to server"
**Solution:** PostgreSQL service not running:
- **Windows:** Start PostgreSQL from Services or `net start postgresql-x64-15`
- **macOS:** `brew services start postgresql`
- **Linux:** `sudo systemctl start postgresql`

### "ModuleNotFoundError: No module named 'psycopg2'"
**Solution:** Install dependencies:
```bash
pip install -r requirements.txt
```

### "UNIQUE violation on bronze.superstore"
**Solution:** Table already exists. This is normal on re-runs. The notebook uses `if_exists='replace'` to overwrite. If you get an error:
```sql
DROP TABLE IF EXISTS bronze.superstore CASCADE;
```

### "No missing values found" but data looks incomplete
**Solution:** Check the `.env` file has correct PostgreSQL credentials. The bronze layer loads raw CSV as-is; quality checks happen in silver layer.

---

## 📓 Notebook Descriptions

### 1️⃣ Bronze Layer: `01_bronze.ipynb`

**Objective:** Ingest raw CSV data with minimal transformation.

**What it does:**
- Connects to PostgreSQL database
- Loads CSV using pandas and writes to PostgreSQL via SQLAlchemy
- Creates `bronze.superstore` table (9,627 rows × 21 columns)
- Previews ingested data

**Output:** 
- PostgreSQL schema: `bronze`
- Table: `bronze.superstore`

**Key Code:**
```python
df = pd.read_csv('../data/Sample - Superstore.csv')
df.to_sql('superstore', engine, schema='bronze', if_exists='replace', index=False)
```

---

### 2️⃣ Silver Layer: `02_silver.ipynb`

**Objective:** Clean, validate, and explore data quality.

**What it does:**

1. **Data Quality Audits**
   - ✅ Missing values check (0 nulls found)
   - ✅ Duplicate detection (no duplicates)
   - ✅ Type inspection (21 fields properly typed)

2. **Exploratory Data Analysis (EDA)**
   - Numerical distributions (sales, quantity, discount, profit)
   - Categorical value counts (segment, region, category, etc.)
   - Outlier detection via boxplots
   - Statistical summaries

3. **Transformations Applied**
   - Column name standardization (snake_case)
   - Explicit type casting
   - NULL handling (postal codes → '00000')
   - Whitespace trimming (customer names)
   - Audit timestamps (`ingested_at`)

**Output:**
- PostgreSQL schema: `silver`
- Table: `silver.superstore` (9,627 rows × 22 columns with standardized types)

**Key Transformations:**
```sql
CREATE TABLE silver.superstore AS
SELECT 
    CAST("Row ID" AS INTEGER) AS row_id,
    CAST("Order Date" AS DATE) AS order_date,
    TRIM("Customer Name") AS customer_name,
    COALESCE(CAST("Postal Code" AS VARCHAR), '00000') AS postal_code,
    CAST("Sales" AS DOUBLE PRECISION) AS sales,
    CAST("Profit" AS DOUBLE PRECISION) AS profit,
    CURRENT_TIMESTAMP AS ingested_at
FROM bronze.superstore;
```

---

### 3️⃣ Gold Layer: `03_gold.ipynb`

**Objective:** Build analytical star schema optimized for BI tools.

**What it does:**

1. **Creates Dimension Tables**
   - `dim_customers` (787 unique customers)
   - `dim_products` (1,832 unique products)
   - `dim_location` (628 unique locations with surrogate key)
   - `dim_date` (1,430 unique dates with temporal attributes)

2. **Creates Fact Table**
   - `fact_sales` (9,627 transaction records)
   - Stores measures: sales, quantity, discount, profit
   - References all dimensions via foreign keys

3. **Validates Data Integrity**
   - Row count matching (silver → fact = 1:1)
   - Dimension cardinality verification
   - Foreign key verification

4. **Demonstrates Analytical Queries**
   - Sales by customer segment
   - Sales by region and month
   - Top 10 products by profit

**Output:**
- PostgreSQL schema: `gold`
- Tables: 
  - `gold.dim_customers`
  - `gold.dim_products`
  - `gold.dim_location`
  - `gold.dim_date`
  - `gold.fact_sales`

**Star Schema:**
```
                    ┌─────────────────┐
                    │  dim_customers  │
                    │ (787 rows)      │
                    └────────┬────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
   ┌────▼───┐          ┌──────▼──────┐     ┌──────▼──────┐
   │dim_    │          │  dim_       │     │  dim_date   │
   │products│          │ location    │     │(1430 rows)  │
   │(1832)  │          │(628 rows)   │     │             │
   └────┬───┘          └──────┬──────┘     └──────┬──────┘
        │                    │                    │
        └────────────────────┼────────────────────┘
                             │
                    ┌────────▼────────┐
                    │  fact_sales     │
                    │  (9,627 rows)   │
                    └─────────────────┘
```

---

## 📊 Data Model & Schema

### Dimension Tables

#### `dim_customers`
| Column | Type | Notes |
|--------|------|-------|
| customer_id | VARCHAR | Primary key |
| customer_name | VARCHAR | Customer name |
| segment | VARCHAR | Consumer, Corporate, Home Office |

#### `dim_products`
| Column | Type | Notes |
|--------|------|-------|
| product_id | VARCHAR | Primary key |
| product_name | VARCHAR | Product name |
| category | VARCHAR | Furniture, Office Supplies, Technology |
| sub_category | VARCHAR | Detailed product category |

#### `dim_location`
| Column | Type | Notes |
|--------|------|-------|
| location_id | INT | Surrogate key (auto-generated) |
| country | VARCHAR | United States |
| region | VARCHAR | West, East, Central, South |
| state | VARCHAR | 2-letter state code |
| city | VARCHAR | City name |
| postal_code | VARCHAR | Postal code |

#### `dim_date`
| Column | Type | Notes |
|--------|------|-------|
| date_key | DATE | Primary key |
| year | INT | Year (2016-2017) |
| month | INT | Month (1-12) |
| day | INT | Day of month |
| quarter | INT | Quarter (Q1-Q4) |
| day_name | VARCHAR | Monday, Tuesday, etc. |
| month_name | VARCHAR | January, February, etc. |
| is_weekend | BOOLEAN | TRUE for Saturday/Sunday |

### Fact Table

#### `fact_sales`
| Column | Type | Role | Notes |
|--------|------|------|-------|
| row_id | INT | ID | Unique transaction line item |
| order_id | VARCHAR | ID | Order identifier |
| order_date | DATE | FK | Links to dim_date |
| ship_date | DATE | FK | Links to dim_date |
| customer_id | VARCHAR | FK | Links to dim_customers |
| product_id | VARCHAR | FK | Links to dim_products |
| location_id | INT | FK | Links to dim_location |
| ship_mode | VARCHAR | Attribute | First Class, Second Class, etc. |
| **sales** | DOUBLE | **Measure** | **Revenue per line item** |
| **quantity** | INT | **Measure** | **Units sold** |
| **discount** | DOUBLE | **Measure** | **Discount applied (0-1)** |
| **profit** | DOUBLE | **Measure** | **Net profit (can be negative)** |

---

## 🔍 Key Data Insights

### Data Quality
✅ **Excellent Quality**
- **Completeness:** 9,627 rows × 21 columns, 0 nulls
- **Uniqueness:** No duplicate records
- **Validity:** All dates parseable, numerics in expected ranges

### Business Dimensions
- **Customers:** 787 unique customers across 4 segments (78% Consumer)
- **Products:** 1,832 unique products in 3 categories (Tech dominates)
- **Locations:** 628 unique locations in 4 US regions (West largest)
- **Time Range:** 4 years (2016-2017) with 1,430 distinct transaction dates

### Financial Patterns
- **Sales Range:** $0 - $22,638 per transaction (right-skewed distribution)
- **Profitability:** 9,627 transactions; some orders unprofitable (negative profit)
- **Discounting:** Most orders have 0% discount; heavy discounts rare
- **Quantity:** Most orders are 1-3 items (bulk orders less common)

### Outliers
- **High-value orders:** Few transactions > $10,000 (legitimate high-value sales)
- **Bulk orders:** Some orders > 10 items (wholesale or bulk customers)
- **Loss-making orders:** Negative profit cases suggest pricing/cost issues worth investigating

---

## 💾 Database & Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Database** | PostgreSQL | Production-grade SQL database with server |
| **ORM/Connector** | SQLAlchemy + psycopg2 | Python database abstraction layer |
| **Processing** | Python + Pandas | Data transformation & analysis |
| **Visualization** | Matplotlib + Seaborn | EDA charts & distributions |
| **Notebooks** | Jupyter | Interactive analysis & documentation |
| **Version Control** | Git | Change tracking |

### Why PostgreSQL?
- ✅ Production-ready (industry-standard data warehouse database)
- ✅ SQL support (standard ANSI SQL with extensions)
- ✅ Excellent Pandas integration via SQLAlchemy
- ✅ Scalable (handles large datasets efficiently)
- ✅ ACID compliance (data integrity guarantees)
- ✅ Rich ecosystem (BI tools, migration tools, extensions)

---

## 📦 Dependencies

```
psycopg2-binary     # PostgreSQL adapter for Python
sqlalchemy          # SQL toolkit & ORM
numpy               # Numerical computing
pandas              # Data manipulation
seaborn             # Statistical visualization
matplotlib          # Plotting library
python-dotenv       # Environment variable management
```

Install all at once:
```bash
pip install -r requirements.txt
```

**Note:** PostgreSQL 12+ must be installed separately and running on your system.

---

## 📈 DuckDB → PostgreSQL Migration

### What Changed

This project was originally built with **DuckDB** (an in-process SQL database) but was migrated to **PostgreSQL** (a client-server SQL database). Here's what shifted:

| Aspect | DuckDB | PostgreSQL |
|--------|--------|-----------|
| **Architecture** | In-process (embedded) | Client-Server (runs on a server) |
| **Data Location** | Single `.duckdb` file | Managed by running PostgreSQL service |
| **Connection** | Direct file access via Python | Network connection (host:port) |
| **Python Library** | `duckdb` module + `read_csv_auto()` | `psycopg2` + `SQLAlchemy` |
| **CSV Ingestion** | DuckDB's native CSV reader | Pandas → SQLAlchemy → PostgreSQL |
| **Best For** | Laptop analytics, one-off queries, embedded OLAP | Production warehouse, multiple users, ACID compliance |
| **Scalability** | Single machine | Multiple users, backups, replication |

### Why PostgreSQL

✅ **Production-Ready** - Industry-standard for data warehouses and analytics  
✅ **Concurrent Access** - Multiple analysts can query simultaneously  
✅ **ACID Compliance** - Data integrity guaranteed  
✅ **BI Tool Integration** - Native support in Tableau, Power BI, Looker, etc.  
✅ **Ecosystem** - Rich tooling for backups, replication, monitoring  
✅ **Long-term Sustainability** - Widely deployed in enterprises  

### Code Changes

**Before (DuckDB):**
```python
import duckdb
conn = duckdb.connect('retailion.duckdb')
df = conn.execute("SELECT * FROM bronze.superstore").fetch_df()
conn.execute("CREATE TABLE silver.superstore AS SELECT ...")
```

**After (PostgreSQL):**
```python
from sqlalchemy import create_engine
engine = create_engine("postgresql+psycopg2://user:pass@localhost:5432/retailion")
df = pd.read_sql("SELECT * FROM bronze.superstore", engine)
df.to_sql('superstore', engine, schema='silver', if_exists='replace')
```

### Setup Difference

**DuckDB:** Just run code, database file created automatically  
**PostgreSQL:** Requires:
1. PostgreSQL server installed and running
2. Database created (`createdb retailion`)
3. Connection parameters in `.env` file

### When to Use Each

| Use Case | DuckDB | PostgreSQL |
|----------|--------|-----------|
| **Local development/prototyping** | ✅ Excellent | ✅ Good |
| **Team collaboration** | ❌ Difficult (file-based) | ✅ Perfect |
| **Production data warehouse** | ❌ Not recommended | ✅ Industry standard |
| **Multi-user analytics** | ❌ Single user only | ✅ Concurrent access |
| **Cloud deployment** | ⚠️ Possible but tricky | ✅ Easy (RDS, etc.) |
| **Embedded analytics** | ✅ Purpose-built | ❌ Overkill |
| **Complex ETL pipelines** | ✅ Good for pure OLAP | ✅ Excellent (with orchestration) |

**For this project:** PostgreSQL was chosen because it's the industry-standard approach for building scalable, production-grade data warehouses that multiple analysts can access simultaneously.

---

## 🎯 Key Learning Outcomes

This project demonstrates:

1. **Medallion Architecture** - Industry-standard data warehouse pattern
2. **Data Quality Practices** - Audits, validation, documentation
3. **Exploratory Data Analysis** - Statistical and visual techniques
4. **Dimensional Modeling** - Star schema design for analytics
5. **SQL Proficiency** - Complex queries, joins, aggregations
6. **Python Data Stack** - Pandas, PostgreSQL, SQLAlchemy, visualization libraries
7. **Jupyter as Documentation** - Narrative notebooks with code & insights

---

## ⚡ Performance & Optimization

### Current Performance

- **Bronze Layer:** ~10 seconds (CSV ingestion via Pandas)
- **Silver Layer:** ~5-8 seconds (type casting, EDA visualizations)
- **Gold Layer:** ~3-5 seconds (dimension/fact table creation)
- **Example Queries:** <100ms each

### Optimization Opportunities

For larger datasets or production use:

1. **Add Indexes** (improves query speed)
   ```sql
   CREATE INDEX idx_fact_sales_customer ON gold.fact_sales(customer_id);
   CREATE INDEX idx_fact_sales_date ON gold.fact_sales(order_date);
   CREATE INDEX idx_dim_location ON gold.dim_location(region, state);
   ```

2. **Partition Large Tables** (for very large datasets)
   ```sql
   -- Partition fact table by year for faster queries
   CREATE TABLE gold.fact_sales_2016 PARTITION OF gold.fact_sales
       FOR VALUES FROM ('2016-01-01') TO ('2017-01-01');
   ```

3. **Materialized Views** (pre-aggregate heavy queries)
   ```sql
   CREATE MATERIALIZED VIEW gold.vw_sales_by_region AS
   SELECT region, SUM(sales) FROM gold.fact_sales
   GROUP BY region;
   ```

4. **Batch Processing** (for incremental loads)
   - Use `INSERT INTO` instead of `replace` to add new data
   - Track processed rows with timestamps

---

## 🔮 Future Enhancements

### Short Term
- [ ] Add data quality KPIs (row counts, null %, duplicates %)
- [ ] Create aggregated fact tables (daily_sales, monthly_sales)
- [ ] Add slowly-changing dimensions (SCD Type 2) for products
- [ ] Implement incremental loading logic
- [ ] Add database indexes for common queries

### Medium Term
- [ ] Migrate from notebooks to Python modules
- [ ] Add orchestration (Airflow, dbt, or Prefect)
- [ ] Connect BI tool (Tableau, Power BI, Looker, Metabase)
- [ ] Add data validation framework (Great Expectations)
- [ ] Implement monitoring & alerting
- [ ] Create materialized views for common aggregations

### Long Term
- [ ] Scale to cloud data warehouse (Snowflake, BigQuery, Redshift)
- [ ] Build ML pipeline for demand forecasting
- [ ] Implement real-time data ingestion
- [ ] Add data governance & lineage tracking (OpenLineage)
- [ ] Create self-service analytics platform

---

## 🚀 Next Steps

### For Data Engineers
1. Convert notebooks to production Python scripts
2. Set up workflow orchestration (Airflow/dbt)
3. Add data quality tests using Great Expectations
4. Implement incremental data loads

### For Data Analysts
1. Connect PostgreSQL to your BI tool
2. Build dashboards on the gold layer star schema
3. Run ad-hoc exploratory queries
4. Create KPI reports

### For Data Scientists
1. Use the gold layer as training data source
2. Analyze customer segments with clustering
3. Build demand forecasting models
4. Analyze profitability drivers

---

## 📖 Additional Resources

- [Kaggle Dataset](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [Medallion Architecture Concept](https://www.databricks.com/blog/2022/06/24/multi-hop-architecture-is-pat-of-modern-data-platforms.html)
- [Star Schema Design](https://learn.microsoft.com/en-us/power-bi/guidance/star-schema)
- [Dimensional Modeling Fundamentals](https://www.kimballgroup.com/)
- [Migration Guide](MIGRATION_GUIDE.md) - Details on DuckDB → PostgreSQL changes

---

## 📝 License

This project uses publicly available Kaggle dataset. Feel free to use this code for learning and educational purposes.

---

## 👤 Author

Created as a comprehensive data engineering project demonstrating modern data warehouse practices.

**Last Updated:** August 2026 (PostgreSQL migration)

---

**Questions? Ideas? Found a bug?**

Feel free to check the notebooks for detailed explanations of each step, or refer to the data model section above for schema details.
