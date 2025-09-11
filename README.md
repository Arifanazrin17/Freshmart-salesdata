[Store Uploads CSV → GCS Bucket (freshmart-sales-data)]
                │
                ▼
 [Airflow DAG Trigger (GCS Sensor / Pub/Sub Notification)]
                │
                ▼
 [Data Validation Step]
   - Check schema (Store ID, Location, Product, Quantity, etc.)
   - Check missing values / duplicates
   - Check sales_date matches upload date
                │
                ▼
 [Dataflow Job (ETL)]
   - Standardize formats (currency, date, product codes)
   - Enrich with master data (product/category mapping)
   - Compute derived metrics (daily totals, margins)
                │
                ▼
 [BigQuery Data Warehouse]
   - Centralized sales table (partitioned by sales_date)
   - 3+ years of historical data
                │
                ▼
 [Airflow Validation Step]
   - Row count check
   - Store count check (all 150 stores uploaded?)
                │
                ▼
     ┌───────────────┴───────────────┐
     ▼                               ▼
[Looker Dashboard Refresh]   [Alerts → Email/Slack]
