import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import uuid
from datetime import datetime

# ---------- BigQuery Schemas ----------
STORE_SCHEMA = {
    "fields": [
        {"name": "store_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "store_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "location", "type": "STRING", "mode": "NULLABLE"},
        {"name": "region", "type": "STRING", "mode": "NULLABLE"}
    ]
}

PRODUCT_SCHEMA = {
    "fields": [
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "category", "type": "STRING", "mode": "NULLABLE"},
        {"name": "sub_category", "type": "STRING", "mode": "NULLABLE"}
    ]
}

SALES_SCHEMA = {
    "fields": [
        {"name": "sales_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "store_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "price", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "total_sales_price", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "sales_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "date", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "month", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "year", "type": "INTEGER", "mode": "NULLABLE"}
    ]
}

STORE_PERFORMANCE_SCHEMA = {
    "fields": [
        {"name": "store_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "location", "type": "STRING", "mode": "NULLABLE"},
        {"name": "region", "type": "STRING", "mode": "NULLABLE"},
        {"name": "total_revenue", "type": "FLOAT", "mode": "NULLABLE"}
    ]
}

TOPSELLING_PRODUCT_SCHEMA = {
    "fields": [
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "quantity", "type": "STRING", "mode": "NULLABLE"},
        {"name": "total_revenue", "type": "FLOAT", "mode": "NULLABLE"}
    ]
}

# ---------- CSV Parsing ----------
class ParseCSV(beam.DoFn):
    def process(self, element):
        row = element.strip().split(",")

        try:
            quantity = int(row[5]) if row[5] else 0
        except:
            quantity = 0

        try:
            price = float(row[6]) if row[6] else 0.0
        except:
            price = 0.0

        try:
            total = float(row[7]) if row[7] else (price * quantity)
        except:
            total = price * quantity

        sales_date = row[8]
        try:
            dt = datetime.strptime(sales_date, "%Y-%m-%d")
            day, month, year = dt.day, dt.month, dt.year
        except:
            day, month, year = None, None, None

        store = {
            "store_id": row[0],
            "store_name": f"Store_{row[0]}",
            "location": row[1],
            "region": None
        }

        
        product = {
            "product_id": row[2],
            "product_name": row[3],
            "sub_category": "test12"
        }

        sale = {
            "sales_id": str(uuid.uuid4()),
            "store_id": row[0],
            "product_id": row[2],
            "price": price,
            "quantity": quantity,
            "total_sales_price": total,
            "sales_date": dt.date() if dt else None,
            "date": day,
            "month": month,
            "year": year
        }

        return [{"store": store, "product": product, "sale": sale}]

# ---------- Pipeline ----------
def run():
    pipeline_options = PipelineOptions(
        streaming=False,
        save_main_session=True,
        runner="DataflowRunner",  # Change to DirectRunner for local testing
        project="acoustic-apex-469415-m0",
        region="us-central1",
        temp_location="gs://freshmart_daily_store_sales/Temp",
        staging_location="gs://freshmart_daily_store_sales/Staging",
        job_name="freshmart-sales-job2"
    )

    input_file = "gs://freshmart_daily_store_sales/Store-data/*.csv"

    with beam.Pipeline(options=pipeline_options) as p:
        parsed = (
            p
            | "Read CSV from GCS" >> beam.io.ReadFromText(input_file, skip_header_lines=1)
            | "Parse CSV" >> beam.ParDo(ParseCSV())
        )

        # # --- Store dimension ---
        stores = (
            parsed
            | "Extract store" >> beam.Map(lambda x: x["store"])
        )

        stores | "Write stores" >> beam.io.WriteToBigQuery(
            "acoustic-apex-469415-m0:Freshmart_branch_daily_sales.Store-table",
            schema=STORE_SCHEMA,
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED"
        )

        # --- Product dimension ---
        products = (
            parsed
            | "Extract product" >> beam.Map(lambda x: x["product"])
        )

        products | "Write products" >> beam.io.WriteToBigQuery(
            "acoustic-apex-469415-m0:Freshmart_branch_daily_sales.Product-table",
            schema=PRODUCT_SCHEMA,
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED"
        )

        # --- Sales fact ---
        sales = (
            parsed
            | "Extract sales" >> beam.Map(lambda x: x["sale"])
        )

        sales | "Write sales" >> beam.io.WriteToBigQuery(
            "acoustic-apex-469415-m0:Freshmart_branch_daily_sales.Sales-table",
            schema=SALES_SCHEMA,
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED"
        )

        # --- Store Performance ---
        store_revenue = (
            sales
            | "Store revenue KV" >> beam.Map(lambda s: (s["store_id"], s["total_sales_price"]))
            | "Sum revenue per store" >> beam.CombinePerKey(sum)
        )

        store_perf = (
            stores
            | "Store KV for perf" >> beam.Map(lambda s: (s["store_id"], s))
        )

        (
            {'store': store_perf, 'revenue': store_revenue}
            | "Join store revenue" >> beam.CoGroupByKey()
            | "Format store performance" >> beam.Map(lambda kv: {
                "store_id": kv[0],
                "location": kv[1]['store'][0]["location"] if kv[1]['store'] else None,
                "region": kv[1]['store'][0].get("region") if kv[1]['store'] else None,
                "total_revenue": kv[1]['revenue'][0] if kv[1]['revenue'] else 0.0
            })
            | "Write store performance" >> beam.io.WriteToBigQuery(
                "acoustic-apex-469415-m0:Freshmart_branch_daily_sales.Store_performance",
                schema=STORE_PERFORMANCE_SCHEMA,
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_IF_NEEDED"
            )
        )

        # --- Top Selling Products ---
        # product_revenue = (
        #     sales
        #     | "Product revenue KV" >> beam.Map(lambda s: (s["product_id"], s["total_sales_price"]))
        #     | "Sum revenue per product" >> beam.CombinePerKey(sum)
        # )

        # product_info = (
        #     products
        #     | "Product KV for top selling" >> beam.Map(lambda p: (p["product_id"], p))
        # )

        # (
        #     {'product': product_info, 'revenue': product_revenue}
        #     | "Join product revenue" >> beam.CoGroupByKey()
        #     | "Format top products" >> beam.Map(lambda kv: {
        #         "product_id": kv[0],
        #         "quantity": kv[1]['product'][0]["category"] if kv[1]['product'] else "",
        #         "total_revenue": kv[1]['revenue'][0] if kv[1]['revenue'] else 0.0
        #     })
        #     | "Write top products" >> beam.io.WriteToBigQuery(
        #         "acoustic-apex-469415-m0:Freshmart_branch_daily_sales.Top_selling_products",
        #         schema=TOPSELLING_PRODUCT_SCHEMA,
        #         write_disposition="WRITE_APPEND",
        #         create_disposition="CREATE_IF_NEEDED"
        #     )
        # )


if __name__ == "__main__":
    run()
