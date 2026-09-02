import pytest
from src.quality.quality_rules import apply_quality_rules


def test_arabic_digits():
    record = {"id_order": "ORD-1", "items": [{"sku": "SKU-1", "qty": "٥", "unit_price": "١٠٠0", "total": "5000"}]}
    cleaned = apply_quality_rules(record)
    assert cleaned["items"][0]["qty"] == 5


def test_currency_and_thousands_separator():
    record = {
        "id_order": "ORD-2",
        "payment": {"method": "بطاقة", "status": "تم الدفع", "currency": "5000 لاير", "amount": 5000},
        "items": [{"sku": "SKU-2", "qty": 1, "unit_price": "125,000.00", "total": "125,000.00"}]
    }
    cleaned = apply_quality_rules(record)
    assert cleaned["payment"]["currency"] == "YER"
    assert cleaned["items"][0]["unit_price"] == 125000.0


def test_email_repair():
    record = {
        "id_order": "ORD-3",
        "customer": {"customer_id": "C-1", "name": "Test", "phone": "777123456", "email": "user@@mail..com", "address": {"city": "Sanaa", "district": "Hadda"}}
    }
    cleaned = apply_quality_rules(record)
    assert cleaned["customer"]["email"] == "user@mail.com"
    assert len(cleaned["corrections"]) >= 1
    assert cleaned["quality_status"] == "corrected"


def test_number_words_conversion():
    record = {"id_order": "ORD-4", "items": [{"sku": "SKU-4", "qty": "ألفان", "unit_price": 10, "total": 20000}]}
    cleaned = apply_quality_rules(record)
    assert cleaned["items"][0]["qty"] == 2000
