import pytest
from src.quality.classifier import classify_record


def test_classify_valid_record():
    raw_doc = {
        "id_run": "test_run",
        "number_row_source": 1,
        "record_raw": {
            "id_order": "ORD-VALID-100",
            "order_date": "2025-01-31T10:00:00Z",
            "status": "مؤكد",
            "customer": {
                "customer_id": "CUS-1",
                "name": "Ahmed",
                "phone": "967771234567",
                "email": "ahmed@example.com",
                "address": {"city": "صنعاء", "district": "حدة"}
            },
            "items": [{"sku": "SKU-1", "name": "Item 1", "qty": 1, "unit_price": 5000.0, "total": 5000.0}],
            "delivery": {"type": "سريع", "cost": 1000.0},
            "payment": {"method": "بطاقة", "status": "تم الدفع", "amount": 6000.0, "currency": "YER"},
            "total_amount": 6000.0
        }
    }
    outcome, payload = classify_record(raw_doc)
    assert outcome == "VALID"
    assert payload["quality_status"] == "valid"


def test_classify_corrected_record():
    raw_doc = {
        "id_run": "test_run",
        "number_row_source": 2,
        "record_raw": {
            "id_order": "ORD-CORRECTED-101",
            "order_date": "2025/01/31",
            "status": " مؤكد ",
            "customer": {
                "customer_id": "CUS-2",
                "name": "Fatima",
                "phone": "967+ 77 123 4567",
                "email": "fatima@@example..com",
                "address": {"city": "عدن", "district": "المنصورة"}
            },
            "items": [{"sku": "SKU-2", "name": "Item 2", "qty": "٥", "unit_price": "2,000", "total": "10,000"}],
            "payment": {"method": "بطاقة", "status": "تم الدفع", "amount": 10000.0, "currency": "5000 لاير"},
            "total_amount": 10000.0
        }
    }
    outcome, payload = classify_record(raw_doc)
    assert outcome == "CORRECTED"
    assert payload["quality_status"] == "corrected"
    assert len(payload["corrections"]) > 0


def test_classify_quarantine_record():
    raw_doc = {
        "id_run": "test_run",
        "number_row_source": 3,
        "record_raw": {
            "id_order": "",
            "status": "حالة غير معروفة",
            "customer": {"name": "No ID"},
            "items": []
        }
    }
    outcome, payload = classify_record(raw_doc)
    assert outcome == "QUARANTINED"
    assert "ID_ORDER_MISSING" in payload["codes_error"]
