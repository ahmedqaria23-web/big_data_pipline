import pytest
from src.mongodb.repositories import is_business_state_equal, compute_idempotency_key


def test_business_state_equal_identical_records():
    rec1 = {
        "id_run": "run_100",
        "id_order": "ORD-001",
        "status": "مؤكد",
        "total_amount": 5000.0,
        "at_ingested": "2026-08-18T10:00:00Z",
        "processed_at": "2026-08-18T10:00:05Z",
        "corrections": [
            {"field": "phone", "original_value": "0771234567", "corrected_value": "967771234567", "timestamp": "2026-08-18T10:00:05Z"}
        ]
    }

    rec2 = {
        "id_run": "run_101",
        "id_order": "ORD-001",
        "status": "مؤكد",
        "total_amount": 5000.0,
        "at_ingested": "2026-08-18T11:00:00Z",
        "processed_at": "2026-08-18T11:00:05Z",
        "corrections": [
            {"field": "phone", "original_value": "0771234567", "corrected_value": "967771234567", "timestamp": "2026-08-18T11:00:05Z"}
        ]
    }

    assert is_business_state_equal(rec1, rec2) is True


def test_business_state_equal_modified_record():
    rec1 = {
        "id_order": "ORD-001",
        "status": "قيد الانتظار",
        "total_amount": 5000.0,
    }

    rec2 = {
        "id_order": "ORD-001",
        "status": "تم التسليم",
        "total_amount": 5000.0,
    }

    assert is_business_state_equal(rec1, rec2) is False


def test_compute_idempotency_key_with_id_order_and_whitespace():
    # TEST 3: id_order with spaces -> normalized key
    key1 = compute_idempotency_key({"record_raw": {"id_order": " ORD-100 "}}, id_order_val=" ORD-100 ")
    key2 = compute_idempotency_key({"record_raw": {"id_order": "ORD-100"}}, id_order_val="ORD-100")

    assert key1 == "ORD-100"
    assert key2 == "ORD-100"
    assert key1 == key2, "Whitespace in id_order must be normalized to identical key"


def test_compute_idempotency_key_without_id_order_deterministic_hash():
    # TEST 4: record without id_order -> deterministic SHA-256 hash
    rec_a = {"item": "Laptop", "amount": 1200, "customer": "Ali", "id_run": "run_111", "at_ingested": "2026-01-01"}
    rec_b = {"item": "Laptop", "amount": 1200, "customer": "Ali", "id_run": "run_999", "at_ingested": "2026-12-31"}

    key_a = compute_idempotency_key(rec_a, id_order_val=None)
    key_b = compute_idempotency_key(rec_b, id_order_val=None)

    assert key_a.startswith("HASH_"), f"Key without id_order must start with HASH_, got {key_a}"
    assert key_a == key_b, f"Hash must be deterministic and independent of id_run / timestamps ({key_a} vs {key_b})"
