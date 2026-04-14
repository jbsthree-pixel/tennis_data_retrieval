from __future__ import annotations

import csv
import sys
from pathlib import Path


INPUT_CSV = Path("output/singles_converted.csv")
OUTPUT_CSV = Path("output/singles_excel_safe.csv")
TEXT_COLUMNS = {"matchId"}


def excel_text_formula(value: str) -> str:
    """Force Excel to treat a numeric-looking identifier as text."""
    escaped = value.replace('"', '""')
    return f'="{escaped}"'


def protect_numeric_ids(row: dict[str, str]) -> bool:
    """Wrap all-digit ID values so Excel will not convert them to numbers."""
    changed = False
    for column in TEXT_COLUMNS:
        value = row.get(column, "")
        if value.isdigit():
            row[column] = excel_text_formula(value)
            changed = True
    return changed


def convert_csv(input_path: Path, output_path: Path) -> tuple[int, int, set[str]]:
    """Create an Excel-safe copy of the CSV and return summary counts."""
    output_path.parent.mkdir(exist_ok=True)

    with input_path.open("r", newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        if not reader.fieldnames:
            raise RuntimeError(f"No CSV header found in {input_path}")

        with output_path.open("w", newline="", encoding="utf-8-sig") as target:
            writer = csv.DictWriter(target, fieldnames=reader.fieldnames)
            writer.writeheader()

            row_count = 0
            protected_ids: set[str] = set()
            for row in reader:
                original_match_id = row.get("matchId", "")
                if protect_numeric_ids(row):
                    protected_ids.add(original_match_id)
                writer.writerow(row)
                row_count += 1

    return row_count, len(protected_ids), protected_ids


def main() -> int:
    try:
        rows, id_count, protected_ids = convert_csv(INPUT_CSV, OUTPUT_CSV)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"Rows written: {rows}")
    print(f"Numeric matchId values protected: {id_count}")
    for match_id in sorted(protected_ids):
        print(f"Protected matchId: {match_id}")
    print(f"Excel-safe CSV output: {OUTPUT_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
