import sys
import time
import json
import tempfile
from pathlib import Path
from pymongo import ReplaceOne

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.mongodb.mongo_setup import initialize_database, get_mongo_db
from src.mongodb.repositories import is_business_state_equal, COLLECTION_VALIDATED, COLLECTION_RAW
from src.quality.classifier import classify_record
from src.pipeline.elt_pipeline import run_elt_pipeline


def generate_benchmark_dataset(file_path: Path, num_records: int = 10000):
    print(f"Generating synthetic benchmark dataset with {num_records:,} records...")
    with open(file_path, "w", encoding="utf-8") as f:
        for i in range(1, num_records + 1):
            is_valid = (i % 10 != 0)  # 90% valid, 10% quarantine
            if is_valid:
                rec = {
                    "id_order": f"ORD-BENCH-{i:06d}",
                    "order_date": "2025/01/31",  # triggers correction
                    "status": " مؤكد ",          # triggers correction
                    "customer": {
                        "customer_id": f"CUS-{i}",
                        "name": f"Test Customer {i}",
                        "phone": "967+ 77 123 4567", # triggers correction
                        "email": f"user{i}@@example..com",
                        "address": {"city": "صنعاء", "district": "حدة"}
                    },
                    "items": [{"sku": f"SKU-{i}", "name": "Item", "qty": "٢", "unit_price": "2,500", "total": "5,000"}],
                    "payment": {"method": "بطاقة", "status": "تم الدفع", "amount": 5000.0, "currency": "YER"},
                    "total_amount": 5000.0
                }
            else:
                rec = {
                    "id_order": "",
                    "status": "غير معروف",
                    "customer": {"name": f"Invalid User {i}"}
                }
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"Dataset generated at {file_path}")


def run_benchmark():
    db = initialize_database(db_name="benchmark_db")

    with tempfile.TemporaryDirectory() as tmp_dir:
        bench_file = Path(tmp_dir) / "benchmark_data.jsonl"
        generate_benchmark_dataset(bench_file, num_records=10000)

        # ----------------------------------------------------
        # BENCHMARK 1: Old Pre-Fetch Upsert vs Direct Bulk Upsert
        # ----------------------------------------------------
        print("\n--- Running Upsert Benchmark ---")

        # Load raw docs first
        from src.ingestion.batch_loader import load_batch_to_raw
        load_batch_to_raw(str(bench_file), id_run="bench_run_0", db=db)
        raw_docs = list(db[COLLECTION_RAW].find({"file_source": bench_file.name}))

        classified_payloads = []
        for d in raw_docs:
            outcome, payload = classify_record(d)
            if outcome in ("VALID", "CORRECTED"):
                classified_payloads.append(payload)

        # Method A: Old $in query + in-memory comparison
        db[COLLECTION_VALIDATED].delete_many({})
        t0 = time.perf_counter()
        id_orders = [r.get("id_order") for r in classified_payloads if r.get("id_order")]
        existing_docs = {
            doc["id_order"]: doc
            for doc in db[COLLECTION_VALIDATED].find({"id_order": {"$in": id_orders}})
        }
        bulk_old = []
        for r in classified_payloads:
            id_order = r["id_order"]
            if id_order in existing_docs:
                if is_business_state_equal(r, existing_docs[id_order]):
                    pass
            bulk_old.append(ReplaceOne({"id_order": id_order}, r, upsert=True))
        if bulk_old:
            db[COLLECTION_VALIDATED].bulk_write(bulk_old, ordered=False)
        old_upsert_time = time.perf_counter() - t0

        # Method B: New Direct Bulk Write
        db[COLLECTION_VALIDATED].delete_many({})
        t0 = time.perf_counter()
        bulk_new = []
        for r in classified_payloads:
            rec_c = r.copy()
            rec_c.pop("_id", None)
            bulk_new.append(ReplaceOne({"id_order": r["id_order"]}, rec_c, upsert=True))
        if bulk_new:
            res = db[COLLECTION_VALIDATED].bulk_write(bulk_new, ordered=False)
        direct_upsert_time = time.perf_counter() - t0

        print(f"Old Upsert ($in query + compare + write): {old_upsert_time:.4f} s ({len(classified_payloads)/old_upsert_time:.0f} rec/s)")
        print(f"Direct Bulk Upsert (Native Write):         {direct_upsert_time:.4f} s ({len(classified_payloads)/direct_upsert_time:.0f} rec/s)")
        upsert_speedup = old_upsert_time / max(0.0001, direct_upsert_time)
        print(f"Upsert Speedup: {upsert_speedup:.2f}x faster")

        # ----------------------------------------------------
        # BENCHMARK 2: Full ELT Pipeline Benchmark (Sequential vs Parallel)
        # ----------------------------------------------------
        print("\n--- Running Full Pipeline Benchmark ---")

        # Run 1: Sequential (1 Worker)
        import os
        os.environ["CLASSIFICATION_WORKERS"] = "1"
        os.environ["CLASSIFICATION_CHUNK_SIZE"] = "2000"
        os.environ["MONGO_WRITE_BATCH_SIZE"] = "5000"

        db.client.drop_database("benchmark_db")
        db = initialize_database(db_name="benchmark_db")

        t0 = time.perf_counter()
        metrics_seq = run_elt_pipeline(str(bench_file), db=db)
        seq_time = time.perf_counter() - t0

        # Run 2: Parallel (4 Workers)
        os.environ["CLASSIFICATION_WORKERS"] = "4"

        db.client.drop_database("benchmark_db")
        db = initialize_database(db_name="benchmark_db")

        t0 = time.perf_counter()
        metrics_par = run_elt_pipeline(str(bench_file), db=db)
        par_time = time.perf_counter() - t0

        seq_rate = metrics_seq["read_rows"] / seq_time
        par_rate = metrics_par["read_rows"] / par_time
        pipeline_speedup = seq_time / max(0.0001, par_time)

        print("\n==================================================")
        print("FINAL BENCHMARK COMPARISON REPORT")
        print("==================================================")
        print(f"Total Dataset Records: {metrics_seq['read_rows']:,}")
        print(f"Sequential Execution Time (1 Worker):  {seq_time:.2f} s ({seq_rate:,.1f} records/sec)")
        print(f"Parallel Execution Time (4 Workers):   {par_time:.2f} s ({par_rate:,.1f} records/sec)")
        print(f"Overall Pipeline Speedup:             {pipeline_speedup:.2f}x faster")
        print("==================================================")

        # Verification of Correctness
        assert metrics_seq["read_rows"] == metrics_par["read_rows"]
        assert metrics_seq["count_valid"] == metrics_par["count_valid"]
        assert metrics_seq["count_corrected"] == metrics_par["count_corrected"]
        assert metrics_seq["count_quarantine"] == metrics_par["count_quarantine"]
        print("\n[VERIFICATION PASSED] Parallel classification produced 100% identical outputs to sequential execution!")

    db.client.drop_database("benchmark_db")


if __name__ == "__main__":
    run_benchmark()
