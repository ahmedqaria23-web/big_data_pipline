import sys
import time
import logging
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Callable, Optional
from pymongo.database import Database

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config.settings import MONGODB_URI, MONGODB_DATABASE, COLLECTION_RAW
from src.mongodb.repositories import get_ingestion_checkpoint, save_ingestion_checkpoint

logger = logging.getLogger(__name__)


def configure_spark_env():
    import os, sys, shutil, subprocess
    import pyspark

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Setup HADOOP_HOME in user home directory (pure ASCII path) to prevent Windows CMD / non-ASCII path crashes
    user_hadoop = Path.home() / ".hadoop"
    user_hadoop_bin = user_hadoop / "bin"
    user_hadoop_bin.mkdir(parents=True, exist_ok=True)
    local_winutils = ROOT_DIR / ".hadoop" / "bin" / "winutils.exe"
    target_winutils = user_hadoop_bin / "winutils.exe"

    if local_winutils.exists() and not target_winutils.exists():
        try:
            shutil.copy2(local_winutils, target_winutils)
        except Exception:
            pass

    os.environ["HADOOP_HOME"] = str(user_hadoop)
    os.environ["hadoop.home.dir"] = str(user_hadoop)

    # Ensure pure ASCII C:\jdk17 link exists to prevent Windows CMD path escaping issues
    if os.path.exists(r"C:\Program Files\Java\jdk-17.0.10+7") and not os.path.exists(r"C:\jdk17"):
        try:
            subprocess.run(["powershell", "-Command", "New-Item -ItemType Junction -Path 'C:\\jdk17' -Target 'C:\\Program Files\\Java\\jdk-17.0.10+7' -Force"], capture_output=True)
        except Exception:
            pass

    jdk_candidates = [
        r"C:\jdk17",
        r"C:\Program Files\Java\jdk-17.0.10+7",
        r"C:\Program Files\Java\jdk-17",
        os.getenv("JAVA_HOME", "")
    ]

    for jdk in jdk_candidates:
        if jdk and os.path.exists(os.path.join(jdk, "bin", "java.exe")):
            os.environ["JAVA_HOME"] = jdk
            os.environ["PATH"] = os.path.join(jdk, "bin") + os.pathsep + os.environ.get("PATH", "")
            break

    # Setup pure ASCII C:\pyspark_home Junction link for SPARK_HOME to bypass non-ASCII (Arabic) project paths in Windows cmd.exe
    if hasattr(pyspark, "__file__") and pyspark.__file__:
        pyspark_dir = os.path.dirname(pyspark.__file__)
        if any(ord(c) > 127 for c in pyspark_dir):
            if not os.path.exists(r"C:\pyspark_home"):
                try:
                    subprocess.run(["powershell", "-Command", f"New-Item -ItemType Junction -Path 'C:\\pyspark_home' -Target '{pyspark_dir}' -Force"], capture_output=True)
                except Exception:
                    pass
            if os.path.exists(r"C:\pyspark_home"):
                pyspark_dir = r"C:\pyspark_home"
        os.environ["SPARK_HOME"] = pyspark_dir


def get_csv_fixed_schema(file_path: Optional[Path] = None):
    """
    Returns a predefined, explicit fixed StructType schema for order CSV files.
    All sensitive fields (price, qty, phone, email, date, items, status, total_amount, currency)
    are explicitly typed as StringType to preserve original raw values before transformation.
    This fulfills the ELT raw-first requirement: never transform or coerce types before Raw load.
    No inferSchema is used.
    """
    from pyspark.sql.types import StructType, StructField, StringType

    # Fixed Schema A: Standard Orders CSV (16 columns)
    standard_schema = StructType([
        StructField("id_order",         StringType(), True),
        StructField("order_date",       StringType(), True),
        StructField("status",           StringType(), True),
        StructField("customer_id",      StringType(), True),
        StructField("customer_name",    StringType(), True),
        StructField("phone",            StringType(), True),
        StructField("email",            StringType(), True),
        StructField("shipping_city",    StringType(), True),
        StructField("district",         StringType(), True),
        StructField("items",            StringType(), True),
        StructField("payment_method",   StringType(), True),
        StructField("payment_status",   StringType(), True),
        StructField("currency",         StringType(), True),
        StructField("total_amount",     StringType(), True),
        StructField("delivery_type",    StringType(), True),
        StructField("delivery_cost",    StringType(), True),
    ])

    # Fixed Schema B: 5M Huge Orders CSV (17 columns)
    huge_schema = StructType([
        StructField("order_id",         StringType(), True),
        StructField("order_date",       StringType(), True),
        StructField("status",           StringType(), True),
        StructField("customer_id",      StringType(), True),
        StructField("customer_name",    StringType(), True),
        StructField("customer_phone",   StringType(), True),
        StructField("customer_email",   StringType(), True),
        StructField("city",             StringType(), True),
        StructField("district",         StringType(), True),
        StructField("delivery_type",    StringType(), True),
        StructField("delivery_cost",    StringType(), True),
        StructField("payment_method",   StringType(), True),
        StructField("payment_status",   StringType(), True),
        StructField("payment_amount",   StringType(), True),
        StructField("currency",         StringType(), True),
        StructField("total_amount",     StringType(), True),
        StructField("items_json",       StringType(), True),
    ])

    if file_path is not None:
        try:
            with open(file_path, "r", encoding="utf-8-sig", errors="ignore") as f:
                header_line = f.readline().lower()
            cols = [c.strip().strip('"') for c in header_line.split(",") if c.strip()]
            if len(cols) == 17 and ("items_json" in cols or "customer_phone" in cols):
                return huge_schema
            elif len(cols) == 16 and "id_order" in cols:
                return standard_schema
            else:
                return StructType([StructField(col_name, StringType(), True) for col_name in cols])
        except Exception:
            return standard_schema

    return standard_schema


def load_spark_to_raw(
    file_path: str,
    id_run: str,
    db: Database,
    mongo_uri: str = MONGODB_URI,
    mongo_db: str = MONGODB_DATABASE,
    progress_callback: Optional[Callable[[str, float], None]] = None
) -> Dict[str, Any]:
    """
    High-Throughput PySpark loader function using official MongoDB Spark Connector.
    Uses explicit fixed StructType schema (all StringType for sensitive fields) to ensure
    raw data integrity before transformation. Provides 100% Idempotency via deterministic
    _id matching and persistent batch checkpointing.

    Architecture: ELT (Extract, Load, Transform)
    - Extract: Read CSV/JSONL with fixed schema -> preserve all original dirty values
    - Load: Write to orders_raw with metadata (id_run, file_source, number_row_source, engine_used)
    - Transform: Happens LATER in quality/classifier pipeline (NOT here)
    """
    path = Path(file_path).resolve()
    stat = path.stat()
    sig = f"{path.name}_{stat.st_size}_{stat.st_mtime}"
    file_fingerprint = hashlib.md5(sig.encode('utf-8')).hexdigest()

    target_db_name = db.name if db is not None else mongo_db

    # Check persistent checkpoint prior to launching Spark
    checkpoint = get_ingestion_checkpoint(db, file_fingerprint, id_run=id_run)
    if checkpoint and checkpoint.get("status") == "COMPLETED":
        raw_count = db[COLLECTION_RAW].count_documents({"id_run": id_run})
        if raw_count > 0:
            logger.info(f"[CHECKPOINT] PySpark ingestion for '{path.name}' (id_run='{id_run}') is marked COMPLETED in meta_state and orders_raw contains {raw_count:,} records. Fast-returning.")
            return {
                "id_run": id_run,
                "engine_used": "pyspark",
                "file_source": path.name,
                "file_size_mb": round(stat.st_size / (1024 * 1024), 2),
                "read_rows": raw_count,
                "loaded_raw": raw_count,
                "partitions": 1,
                "elapsed_seconds": 0.0,
                "throughput": 0.0,
                "errors": 0
            }
        else:
            logger.warning(f"[CHECKPOINT] File '{path.name}' (id_run='{id_run}') was marked COMPLETED, but orders_raw has 0 records. Re-ingesting.")

    start_time = time.perf_counter()
    read_rows = 0
    loaded_raw = 0
    partitions = 1
    errors = 0
    spark = None

    try:
        configure_spark_env()

        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, LongType, StringType, Row
        import pyspark.sql.functions as F

        java_opens_flag = (
            "--add-opens=java.base/java.lang=ALL-UNNAMED "
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
            "--add-opens=java.base/java.io=ALL-UNNAMED "
            "--add-opens=java.base/java.net=ALL-UNNAMED "
            "--add-opens=java.base/java.nio=ALL-UNNAMED "
            "--add-opens=java.base/java.util=ALL-UNNAMED "
            "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
        )

        builder = (
            SparkSession.builder
            .appName(f"OrderIngestion_{id_run}")
            .config("spark.master", "local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.extraJavaOptions", java_opens_flag)
            .config("spark.executor.extraJavaOptions", java_opens_flag)
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.3.0,org.mongodb:mongodb-driver-sync:4.8.2,org.mongodb:mongodb-driver-core:4.8.2,org.mongodb:bson:4.8.2,org.mongodb:bson-record-codec:4.8.2")
        )

        spark = builder.getOrCreate()

        suffix = path.suffix.lower()
        if suffix == ".csv":
            # Use explicit fixed StructType schema for CSV files
            # All sensitive fields (price, qty, phone, email, date, items, status) are StringType
            # to preserve original raw dirty values before transformation (ELT raw-first principle)
            fixed_schema = get_csv_fixed_schema(path)
            logger.info(f"[SCHEMA] Fixed StructType schema applied directly: {len(fixed_schema.fields)} fields for '{path.name}' (inferSchema=False)")

            df_in = (
                spark.read
                .schema(fixed_schema)
                .option("header", "true")
                .option("quote", '"')
                .option("escape", '"')
                .option("multiLine", "true")
                .csv(str(path))
            )
        else:
            # For JSONL/JSON: read with primitivesAsString to preserve raw values
            df_in = (
                spark.read
                .option("primitivesAsString", "true")
                .json(str(path))
            )
            logger.info(f"[SCHEMA] JSONL/JSON read with primitivesAsString=true for '{path.name}'")

        partitions = df_in.rdd.getNumPartitions()
        logger.info(f"[IDEMPOTENCY] PySpark engine initialized with {partitions} partitions for file '{path.name}' (id_run='{id_run}')")

        if progress_callback:
            progress_callback(f"Step 2/6: PySpark initialized ({partitions} partitions) for {path.name} ({round(stat.st_size / (1024*1024), 1)} MB)...", 0.32)

        # Deterministic 1-based source row indexing via zipWithIndex
        schema = df_in.schema
        indexed_schema = StructType(schema.fields + [StructField("__row_idx", LongType(), False)])
        rdd_indexed = df_in.rdd.zipWithIndex().map(lambda pair: tuple(list(pair[0]) + [pair[1]]))
        df_indexed = spark.createDataFrame(rdd_indexed, indexed_schema)

        ingest_time_str = datetime.now(timezone.utc).isoformat()
        raw_cols = df_in.columns
        avail_cols = set(raw_cols)

        if "id_order" in avail_cols and "order_id" in avail_cols:
            id_order_expr = F.coalesce(F.col("id_order"), F.col("order_id"))
        elif "id_order" in avail_cols:
            id_order_expr = F.col("id_order")
        elif "order_id" in avail_cols:
            id_order_expr = F.col("order_id")
        else:
            id_order_expr = F.lit(None).cast("string")

        trimmed_id_order = F.trim(id_order_expr)
        number_row_source_col = F.col("__row_idx") + 1
        _id_expr = F.concat(F.lit(f"{id_run}:"), number_row_source_col.cast("string"))

        df_raw_meta = (
            df_indexed
            .withColumn("id_run", F.lit(id_run))
            .withColumn("file_source", F.lit(path.name))
            .withColumn("number_row_source", number_row_source_col)
            .withColumn("at_ingested", F.lit(ingest_time_str))
            .withColumn("engine_used", F.lit("pyspark"))
            .withColumn("id_order", F.when((trimmed_id_order.isNotNull()) & (trimmed_id_order != ""), trimmed_id_order).otherwise(F.lit(None)))
            .withColumn("record_raw", F.struct(*[F.col(c) for c in raw_cols]))
            .withColumn("_id", _id_expr)
        )

        if progress_callback:
            progress_callback(f"Step 2/6: PySpark writing records to MongoDB via Spark Connector...", 0.40)

        # High-throughput batch writing via MongoDB Spark Connector with replace/upsert on _id
        (
            df_raw_meta.write
            .format("mongodb")
            .mode("append")
            .option("spark.mongodb.write.connection.uri", mongo_uri)
            .option("database", target_db_name)
            .option("collection", COLLECTION_RAW)
            .option("operationType", "replace")
            .option("upsertDocument", "true")
            .option("idFieldList", "_id")
            .option("batchSize", "80000")
            .option("ordered", "false")
            .save()
        )

        loaded_raw = df_raw_meta.count()
        read_rows = loaded_raw

        # Save persistent checkpoint AFTER write succeeds
        save_ingestion_checkpoint(db, {
            "file_fingerprint": file_fingerprint,
            "file_name": path.name,
            "file_path": str(path),
            "file_size_bytes": stat.st_size,
            "id_run": id_run,
            "last_completed_batch": 1,
            "processed_rows": read_rows,
            "status": "COMPLETED"
        })
        logger.info(f"[CHECKPOINT] PySpark ingestion COMPLETED for file '{path.name}' ({read_rows:,} records). Saved COMPLETED checkpoint.")

        if progress_callback:
            progress_callback(f"Step 2/6: PySpark Completed Native Connector Ingestion ({loaded_raw:,} records)", 0.55)

    except Exception as err:
        errors += 1
        logger.error(f"[RETRY] PySpark Execution Error: {err}")
        raise RuntimeError(f"PySpark ingestion failed for file '{path.name}': {err}") from err

    finally:
        if spark is not None:
            spark.stop()

    elapsed_seconds = round(time.perf_counter() - start_time, 4)
    throughput = round(read_rows / elapsed_seconds, 2) if elapsed_seconds > 0 else 0.0

    return {
        "id_run": id_run,
        "engine_used": "pyspark",
        "file_source": path.name,
        "file_size_mb": round(stat.st_size / (1024 * 1024), 2),
        "read_rows": read_rows,
        "loaded_raw": loaded_raw,
        "partitions": partitions,
        "elapsed_seconds": elapsed_seconds,
        "throughput": throughput,
        "errors": errors
    }