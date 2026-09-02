import pytest
from src.routing.file_router import inspect_and_route


def test_file_router_small_file(tmp_path):
    small_file = tmp_path / "test_small.csv"
    small_file.write_text("id_order,status\nORD-1,مؤكد\n", encoding="utf-8")

    res = inspect_and_route(small_file, threshold_mb=200.0)

    assert res["selected_engine"] == "python_batch"
    assert res["file_size_mb"] <= 200.0
    assert "id_run" in res


def test_file_router_large_file_threshold(tmp_path):
    sample_file = tmp_path / "test_large.csv"
    sample_file.write_text("id_order,status\nORD-1,مؤكد\n" * 100, encoding="utf-8")

    res = inspect_and_route(sample_file, threshold_mb=0.000001)

    assert res["selected_engine"] == "pyspark"
