import logging
import os
import csv
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_table_arrow(table_name, tmp_dir):

    hook = PostgresHook(postgres_conn_id="postgres_ecommerce")
    conn = hook.get_conn()
    cursor = conn.cursor()

    path = f"{tmp_dir}/{table_name}.parquet"

    copy_sql = f"COPY {table_name} TO STDOUT WITH CSV HEADER"

    temp_file = f"/tmp/{table_name}.csv"

    logger.info(f"Starting COPY for {table_name}")

    # COPY ke CSV sementara
    with open(temp_file, "w") as f:
        cursor.copy_expert(copy_sql, f)

    batch_size = 50000
    rows = []
    writer = None
    total_rows = 0

    with open(temp_file, "r") as f:

        reader = csv.DictReader(f)

        for row in reader:

            rows.append(row)
            total_rows += 1

            if len(rows) >= batch_size:

                table = pa.Table.from_pylist(rows)

                if writer is None:
                    writer = pq.ParquetWriter(path, table.schema)

                writer.write_table(table)
                rows = []

        # batch terakhir
        if rows:

            table = pa.Table.from_pylist(rows)

            if writer is None:
                writer = pq.ParquetWriter(path, table.schema)

            writer.write_table(table)

    if writer:
        writer.close()

    cursor.close()
    conn.close()

    logger.info(f"{table_name} total rows extracted: {total_rows}")
    logger.info(f"Saved parquet {path}")


# =============================
# MAIN
# =============================

if __name__ == "__main__":

    tables = [
        "categories",
        "customers",
        "employees",
        "order_items",
        "orders",
        "products",
        "stores",
    ]

    tmp_dir = "/home/airflow/gcs/data/tmp"
    os.makedirs(tmp_dir, exist_ok=True)

    logger.info("Folder tmp siap")

    for table in tables:

        logger.info(f"Extracting {table}")

        extract_table_arrow(table, tmp_dir)