# Big Data Midterm Pipeline — Main Entry Point

import sys
import argparse
import logging
from pathlib import Path

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from config.settings import setup_logging, DATA_DIR
from src.pipeline.pipeline_controller import run_pipeline_for_file, check_system_status, get_latest_metrics


def print_banner():
    print("=" * 60)
    print("  Big Data Midterm Pipeline — E-Commerce Orders")
    print("  ELT | Python Batch | PySpark | MongoDB")
    print("=" * 60)


def print_router_summary(metrics: dict):
    """Print the file routing decision summary."""
    print(f"\n{'─'*60}")
    print("  FILE ROUTING DECISION")
    print(f"{'─'*60}")
    print(f"  File        : {metrics.get('file_name', '?')}")
    print(f"  Size        : {metrics.get('file_size_mb', '?')} MB")
    print(f"  Threshold   : {metrics.get('threshold_mb', 200.0)} MB")
    print(f"  Engine      : {metrics.get('used_engine', '?').upper()}")
    print(f"  Reason      : {metrics.get('engine_selection_reason', '?')}")
    print(f"  id_run      : {metrics.get('id_run', '?')}")
    print(f"{'─'*60}\n")


def print_metrics_summary(metrics: dict):
    """Print the post-run metrics summary."""
    print(f"\n{'═'*60}")
    print("  PIPELINE RESULTS")
    print(f"{'═'*60}")
    print(f"  id_run           : {metrics.get('id_run', '?')}")
    print(f"  File             : {metrics.get('file_name', '?')}")
    print(f"  Engine Used      : {metrics.get('used_engine', '?').upper()}")
    print(f"  Read Rows        : {metrics.get('read_rows', 0):,}")
    print(f"  Loaded Raw       : {metrics.get('loaded_raw', 0):,}")
    print(f"  Valid            : {metrics.get('count_valid', 0):,}")
    print(f"  Corrected        : {metrics.get('count_corrected', 0):,}")
    print(f"  Quarantined      : {metrics.get('count_quarantine', 0):,}")
    print(f"  Inserted         : {metrics.get('count_inserted', 0):,}")
    print(f"  Updated          : {metrics.get('count_updated', 0):,}")
    print(f"  Unchanged        : {metrics.get('count_unchanged', 0):,}")
    eq_ok = metrics.get('consistency_equation_verified', False)
    eq_sym = "[PASS]" if eq_ok else "[FAIL]"
    print(f"  Consistency Eq.  : {eq_sym}  ({metrics.get('loaded_raw', 0)} = {metrics.get('count_valid', 0)} + {metrics.get('count_corrected', 0)} + {metrics.get('count_quarantine', 0)})")
    print(f"  Elapsed          : {metrics.get('seconds_elapsed', 0):.2f} s")
    print(f"  Throughput       : {metrics.get('throughput_records_per_sec', 0):,.1f} rec/s")
    print(f"{'═'*60}")

    if not eq_ok:
        print("\n  [ERROR] Consistency equation FAILED! Pipeline integrity violation detected.")
        sys.exit(1)


def cmd_status():
    """Check system/MongoDB status."""
    status = check_system_status()
    print(f"\n  MongoDB Connected : {'YES' if status['mongodb_connected'] else 'NO'}")
    print(f"  Database          : {status['database_name']}")
    for col, cnt in status.get("collections", {}).items():
        print(f"  {col:<25}: {cnt:,} documents")
    if "error" in status:
        print(f"\n  [ERROR] {status['error']}")


def cmd_run(file_path: str):
    """Run the full ELT pipeline for a given file."""
    p = Path(file_path)
    if not p.exists():
        candidates = [
            ROOT_DIR / file_path,
            DATA_DIR / file_path,
            DATA_DIR / p.name
        ]
        found = False
        for c in candidates:
            if c.exists():
                p = c
                found = True
                break
        if not found:
            print(f"[ERROR] File not found: {file_path}")
            sys.exit(1)

    metrics = run_pipeline_for_file(str(p))
    print_router_summary(metrics)
    print_metrics_summary(metrics)
    print(f"\n  Results saved to: reports/results.json")


def cmd_metrics():
    """Show the latest pipeline run metrics."""
    metrics = get_latest_metrics()
    if not metrics:
        print("  No metrics found. Run the pipeline first.")
        return
    print_metrics_summary(metrics)


def main():
    print_banner()
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Big Data Midterm Pipeline — ELT Pipeline Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  run <file>    Run the full ELT pipeline for a CSV or JSONL file
  status        Check MongoDB connection and collection counts
  metrics       Show the latest pipeline run metrics

Examples:
  python run_pipeline.py run data/orders_mixed_bad_good.jsonl
  python run_pipeline.py run data/orders_1_million_from_5m.csv
  python run_pipeline.py status
  python run_pipeline.py metrics
        """
    )
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run ELT pipeline for a file")
    run_parser.add_argument("file", help="Path to the input CSV or JSONL file")

    subparsers.add_parser("status", help="Check MongoDB connection status")
    subparsers.add_parser("metrics", help="Show latest pipeline metrics")

    args = parser.parse_args()

    if args.command == "run":
        cmd_run(args.file)
    elif args.command == "status":
        cmd_status()
    elif args.command == "metrics":
        cmd_metrics()
    else:
        parser.print_help()
        print("\n  Quick Start:")
        print("    python run_pipeline.py status")
        print("    python run_pipeline.py run data/orders_mixed_bad_good.jsonl")


if __name__ == "__main__":
    main()
