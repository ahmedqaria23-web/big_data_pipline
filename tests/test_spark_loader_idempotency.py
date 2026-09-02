import pytest
import os
import json
from pathlib import Path
from src.mongodb.mongo_setup import initialize_database, get_mongo_db
from src.mongodb.repositories import (
    count_raw,
    count_validated,
    find_raw_sample,
    get_ingestion_checkpoint,
    save_ingestion_checkpoint
)
from src.ingestion.spark_loader import load_spark_to_raw
from src.ingestion.batch_loader import load_batch_to_raw
from src.pipeline.elt_pipeline import run_elt_pipeline


@pytest.fixture
def temp_sample_file(tmp_path):
    sample_data = [
        {
            "id_order": "ORD-SPARK-DUP-001",
            "order_date": "2025-01-31T10:00:00Z",
            "status": "مؤكد",
            "customer": {
                "customer_id": "CUS-100",
                "name": "Spark Test User",
                "phone": "967771234567",
                "email": "spark@example.com",
                "address": {"city": "صنعاء", "district": "حدة"}
            },
            "items": [{"sku": "SKU-SPARK-1", "name": "Spark Item 1", "qty": 2, "unit_price": 2500.0, "total": 5000.0}],
            "payment": {"method": "بطاقة", "status": "تم الدفع", "amount": 5000.0, "currency": "YER"},
            "total_amount": 5000.0
        },
        # Duplicate record inside the same file (Must be preserved in Raw, consolidated in Validated)
        {
            "id_order": "ORD-SPARK-DUP-001",
            "order_date": "2025-01-31T10:00:00Z",
            "status": "مؤكد",
            "customer": {
                "customer_id": "CUS-100",
                "name": "Spark Test User",
                "phone": "967771234567",
                "email": "spark@example.com",
                "address": {"city": "صنعاء", "district": "حدة"}
            },
            "items": [{"sku": "SKU-SPARK-1", "name": "Spark Item 1", "qty": 2, "unit_price": 2500.0, "total": 5000.0}],
            "payment": {"method": "بطاقة", "status": "تم الدفع", "amount": 5000.0, "currency": "YER"},
            "total_amount": 5000.0
        },
        # Record with trailing spaces in id_order
        {
            "id_order": " ORD-SPARK-DUP-002 ",
            "order_date": "2025-01-31T11:00:00Z",
            "status": "مؤكد",
            "customer": {
                "customer_id": "CUS-101",
                "name": "Spark Test User 2",
                "phone": "967731234567",
                "email": "spark2@example.com",
                "address": {"city": "عدن", "district": "المنصورة"}
            },
            "items": [{"sku": "SKU-SPARK-2", "name": "Spark Item 2", "qty": 1, "unit_price": 3000.0, "total": 3000.0}],
            "payment": {"method": "بطاقة", "status": "تم الدفع", "amount": 3000.0, "currency": "YER"},
            "total_amount": 3000.0
        }
    ]

    file_path = tmp_path / "spark_test_sample.jsonl"
    with open(file_path, "w", encoding="utf-8") as f:
        for item in sample_data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    return file_path


def test_spark_loader_idempotency_reingestion(temp_sample_file):
    db = initialize_database(db_name="test_spark_db")
    
    try:
        # Run 1: First Spark ingestion - Raw receives all 3 records as-is
        res1 = load_spark_to_raw(str(temp_sample_file), id_run="test_run_1", db=db)
        raw_count_run1 = count_raw(db, {"id_run": "test_run_1"})
        assert raw_count_run1 == 3, f"Expected 3 records in orders_raw for run 1, got {raw_count_run1}"

        # Run 2: Re-ingest exact same file with new id_run (Historical Traceability)
        res2 = load_spark_to_raw(str(temp_sample_file), id_run="test_run_2", db=db)
        raw_count_run2 = count_raw(db, {"id_run": "test_run_2"})
        assert raw_count_run2 == 3, f"Expected 3 records in orders_raw for run 2, got {raw_count_run2}"

        # Total in Raw is 6 records (3 from run 1, 3 from run 2)
        total_raw = count_raw(db)
        assert total_raw == 6, f"Expected 6 historical raw records, got {total_raw}"

    finally:
        db.client.drop_database("test_spark_db")


def test_spark_loader_historical_traceability(temp_sample_file):
    db = initialize_database(db_name="test_spark_trace_db")
    try:
        res1 = load_spark_to_raw(str(temp_sample_file), id_run="trace_run_1", db=db)
        res2 = load_spark_to_raw(str(temp_sample_file), id_run="trace_run_2", db=db)
        
        # Verify docs have unique _id scoped by run:row
        docs_run1 = list(db["orders_raw"].find({"id_run": "trace_run_1"}))
        docs_run2 = list(db["orders_raw"].find({"id_run": "trace_run_2"}))
        
        assert len(docs_run1) == 3
        assert len(docs_run2) == 3
        assert all(d["_id"].startswith("trace_run_1:") for d in docs_run1)
        assert all(d["_id"].startswith("trace_run_2:") for d in docs_run2)
    finally:
        db.client.drop_database("test_spark_trace_db")


def test_noid_records_deterministic_hash_idempotency(tmp_path):
    no_id_data = [
        {
            "order_date": "2025-02-01T10:00:00Z",
            "status": "مؤكد",
            "customer": {"name": "No ID Customer 1", "phone": "96777000111"},
            "total_amount": 4000.0
        },
        {
            "order_date": "2025-02-01T11:00:00Z",
            "status": "مؤكد",
            "customer": {"name": "No ID Customer 2", "phone": "96777000222"},
            "total_amount": 6000.0
        }
    ]

    file_path = tmp_path / "noid_sample.jsonl"
    with open(file_path, "w", encoding="utf-8") as f:
        for item in no_id_data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    db = initialize_database(db_name="test_noid_db")
    try:
        # Run 1: Ingest Python batch loader
        res1 = load_batch_to_raw(str(file_path), id_run="noid_run_1", db=db)
        docs1 = list(db["orders_raw"].find({"id_run": "noid_run_1"}))
        assert len(docs1) == 2, f"Expected 2 records in orders_raw for run 1, got {len(docs1)}"
        assert all(d["_id"].startswith("noid_run_1:") for d in docs1)

        # Run 2: Re-ingest with new id_run
        res2 = load_batch_to_raw(str(file_path), id_run="noid_run_2", db=db)
        docs2 = list(db["orders_raw"].find({"id_run": "noid_run_2"}))
        assert len(docs2) == 2, f"Expected 2 records in orders_raw for run 2, got {len(docs2)}"
        assert all(d["_id"].startswith("noid_run_2:") for d in docs2)

    finally:
        db.client.drop_database("test_noid_db")


def test_crash_recovery_and_checkpoint_resume(tmp_path):
    sample_data = [
        {"id_order": f"ORD-CRASH-{i:03d}", "order_date": "2025-02-01T10:00:00Z", "total_amount": 1000.0 * i}
        for i in range(1, 11)
    ]
    file_path = tmp_path / "crash_sample.jsonl"
    with open(file_path, "w", encoding="utf-8") as f:
        for item in sample_data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    db = initialize_database(db_name="test_crash_db")
    try:
        # Step 1: Simulate that first 5 rows were written to orders_raw before crash
        first_batch = [
            {
                "_id": f"crash_run_1:{i}",
                "id_run": "crash_run_1",
                "file_source": file_path.name,
                "number_row_source": i,
                "record_raw": sample_data[i-1]
            }
            for i in range(1, 6)
        ]
        db["orders_raw"].insert_many(first_batch)

        stat = file_path.stat()
        fp = f"{file_path.name}_{stat.st_size}_{stat.st_mtime}"
        
        # Simulate checkpoint saved after batch 1
        save_ingestion_checkpoint(db, {
            "file_fingerprint": fp,
            "file_name": file_path.name,
            "file_path": str(file_path),
            "file_size_bytes": stat.st_size,
            "id_run": "crash_run_1",
            "last_completed_batch": 1,
            "processed_rows": 5,
            "batch_size": 5,
            "status": "IN_PROGRESS"
        })

        # Step 2: Resume loader after crash -> skips rows 1-5 and processes rows 6-10
        res = load_batch_to_raw(str(file_path), id_run="crash_run_1", db=db, batch_size=5)
        total_raw = count_raw(db, {"id_run": "crash_run_1"})

        assert total_raw == 10, f"Expected 10 total records in orders_raw for crash_run_1, got {total_raw}"
        assert res["loaded_raw"] == 10

    finally:
        db.client.drop_database("test_crash_db")


def test_mongodb_unique_protection(tmp_path):
    db = initialize_database(db_name="test_unique_idx_db")
    try:
        raw_coll = db["orders_raw"]
        raw_coll.replace_one({"_id": "run_1:1"}, {"_id": "run_1:1", "val": 100}, upsert=True)
        raw_coll.replace_one({"_id": "run_1:1"}, {"_id": "run_1:1", "val": 200}, upsert=True)

        cnt = raw_coll.count_documents({"_id": "run_1:1"})
        assert cnt == 1, f"Expected 1 document under _id run_1:1, got {cnt}"
        doc = raw_coll.find_one({"_id": "run_1:1"})
        assert doc["val"] == 200, "Upsert should replace with latest document payload"
    finally:
        db.client.drop_database("test_unique_idx_db")


def test_spark_csv_flat_column_accuracy(tmp_path):
    import csv, io
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["id_order", "order_date", "status", "customer_id", "customer_name", "phone", "email", "shipping_city", "district", "items", "payment_method", "total_amount"])
    items_json = json.dumps([{"sku": "SKU-CSV-1", "name": "CSV Item", "qty": 1, "unit_price": 1000.0, "total": 1000.0}], ensure_ascii=False)
    row = ["ORD-CSV-001", "2025-01-31T10:00:00Z", "مؤكد", "CUS-300", "Ahmad", "967771234567", "ahmad@example.com", "صنعاء", "حدة", items_json, "بطاقة", "1000.0"]
    writer.writerow(row)
    writer.writerow(row)

    csv_file = tmp_path / "sample_flat.csv"
    csv_file.write_text(out.getvalue(), encoding="utf-8")

    db = initialize_database(db_name="test_spark_csv_db")
    try:
        load_spark_to_raw(str(csv_file), id_run="test_csv_run", db=db)
        raw_count = count_raw(db, {"id_order": "ORD-CSV-001"})
        # Raw preserves both duplicate rows from source
        assert raw_count == 2, f"Expected 2 raw records in orders_raw for CSV, got {raw_count}"

        # Classify raw doc to verify accuracy and flat column synthesis
        raw_doc = db["orders_raw"].find_one({"id_order": "ORD-CSV-001"})
        from src.quality.classifier import classify_record
        outcome, payload = classify_record(raw_doc)

        assert outcome in ("VALID", "CORRECTED"), f"Expected CSV record to be valid/corrected, got {outcome}, errors: {payload.get('codes_error')} - {payload.get('details_error')}"
    finally:
        db.client.drop_database("test_spark_csv_db")
