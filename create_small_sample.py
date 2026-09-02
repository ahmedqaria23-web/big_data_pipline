import argparse
import json
import csv
from pathlib import Path

from config.settings import DATA_DIR, BASE_DIR


def generate_sample(input_path: str, rows_count: int, output_path: str = None) -> str:
    in_p = Path(input_path)
    
    # Try finding input file across workspace
    if not in_p.exists():
        candidates = [
            BASE_DIR / input_path,
            DATA_DIR / input_path,
            Path(__file__).parent / input_path,
            Path("orders_mixed_bad_good.jsonl").resolve()
        ]
        # Also search for matching filename in BASE_DIR
        for p in BASE_DIR.glob("**/*"):
            if p.name == input_path or p.name == Path(input_path).name:
                candidates.append(p)
                
        found = False
        for cand in candidates:
            if cand.exists():
                in_p = cand.resolve()
                found = True
                break
        if not found:
            raise FileNotFoundError(f"Input file for sampling not found: {input_path}")

    if output_path is None:
        out_p = DATA_DIR / f"sample_{rows_count}_{in_p.name}"
    else:
        out_p = Path(output_path).resolve()

    out_p.parent.mkdir(parents=True, exist_ok=True)

    suffix = in_p.suffix.lower()
    written = 0

    with open(in_p, "r", encoding="utf-8") as fin:
        with open(out_p, "w", encoding="utf-8", newline="") as fout:
            if suffix == ".jsonl":
                for line in fin:
                    if line.strip():
                        fout.write(line)
                        written += 1
                        if written >= rows_count:
                            break
            elif suffix == ".csv":
                reader = csv.reader(fin)
                writer = csv.writer(fout)
                header = next(reader, None)
                if header:
                    writer.writerow(header)
                for row in reader:
                    writer.writerow(row)
                    written += 1
                    if written >= rows_count:
                        break
            else:
                for line in fin:
                    fout.write(line)
                    written += 1
                    if written >= rows_count:
                        break

    print(f"Sample generated successfully:")
    print(f"Input: {in_p.name}")
    print(f"Output: {str(out_p).encode('ascii', errors='backslashreplace').decode('ascii')}")
    print(f"Rows: {written}")
    return str(out_p)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reproducible Sample Dataset Generator CLI")
    parser.add_argument("--input", type=str, default="orders_mixed_bad_good.jsonl", help="Input dataset filename or path")
    parser.add_argument("--rows", type=int, default=1000, help="Number of rows to include in sample")
    parser.add_argument("--output", type=str, default=None, help="Optional output path")

    args = parser.parse_args()
    generate_sample(args.input, args.rows, args.output)
