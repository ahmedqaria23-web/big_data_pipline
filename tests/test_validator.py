import pytest
from src.quality.validator import validate_order


def test_validate_order_valid():
    record = {
        "id_order": "ORD-1",
        "order_date": "2025-01-31T10:00:00Z",
        "status": "مؤكد",
        "customer": {
            "customer_id": "CUS-1",
            "name": "Ali",
            "phone": "967771234567",
            "email": "ali@example.com",
            "address": {"city": "صنعاء", "district": "حدة"}
        },
        "items": [{"sku": "SKU-1", "name": "Item 1", "qty": 1, "unit_price": 5000.0, "total": 5000.0}],
        "delivery": {"type": "سريع", "cost": 1000.0},
        "payment": {"method": "بطاقة", "status": "تم الدفع", "amount": 6000.0, "currency": "YER"},
        "total_amount": 6000.0
    }
    errors = validate_order(record)
    assert errors == []


def test_validate_order_missing_customer_name():
    record = {
        "id_order": "ORD-2",
        "order_date": "2025-01-31T10:00:00Z",
        "status": "مؤكد",
        "customer": {
            "customer_id": "CUS-2",
            "phone": "967771234567",
            "email": "ali@example.com",
            "address": {"city": "صنعاء", "district": "حدة"}
        },
        "items": [{"sku": "SKU-1", "name": "Item 1", "qty": 1, "unit_price": 5000.0, "total": 5000.0}],
        "payment": {"method": "بطاقة", "status": "تم الدفع", "amount": 5000.0, "currency": "YER"},
        "total_amount": 5000.0
    }
    errors = validate_order(record)
    assert any(code == "ID_CUSTOMER_MISSING" for code, msg in errors)


def test_validate_order_missing_item_sku_name():
    record = {
        "id_order": "ORD-3",
        "order_date": "2025-01-31T10:00:00Z",
        "status": "مؤكد",
        "customer": {
            "customer_id": "CUS-3",
            "name": "Sami",
            "phone": "967771234567",
            "email": "sami@example.com",
            "address": {"city": "صنعاء", "district": "حدة"}
        },
        "items": [{"qty": 1, "unit_price": 5000.0, "total": 5000.0}],
        "payment": {"method": "بطاقة", "status": "تم الدفع", "amount": 5000.0, "currency": "YER"},
        "total_amount": 5000.0
    }
    errors = validate_order(record)
    assert any(code == "JSON_ITEMS_CORRUPTED" for code, msg in errors)
