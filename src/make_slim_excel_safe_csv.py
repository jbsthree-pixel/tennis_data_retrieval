from __future__ import annotations

import csv
import sys
from pathlib import Path


INPUT_CSV = Path("output/singles_converted.csv")
OUTPUT_CSV = Path("output/singles_slim_excel_safe.csv")
TEXT_COLUMNS = {"matchId"}
DROP_COLUMNS = {"matchLink", "preprocessedUri", "postprocessedUri"}


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


def convert_csv(input_path: Path, output_path: Path) -> tuple[int, int, set[str], list[str]]:
    """Drop URL columns, protect numeric IDs, and write an Excel-ready CSV."""
    output_path.parent.mkdir(exist_ok=True)

    with input_path.open("r", newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        if not reader.fieldnames:
            raise RuntimeError(f"No CSV header found in {input_path}")

        fieldnames = [name for name in reader.fieldnames if name not in DROP_COLUMNS]

        with output_path.open("w", newline="", encoding="utf-8-sig") as target:
            writer = csv.DictWriter(target, fieldnames=fieldnames)
            writer.writeheader()

            row_count = 0
            protected_ids: set[str] = set()
            for row in reader:
                original_match_id = row.get("matchId", "")
                if protect_numeric_ids(row):
                    protected_ids.add(original_match_id)
                writer.writerow({name: row.get(name, "") for name in fieldnames})
                row_count += 1

    return row_count, len(protected_ids), protected_ids, fieldnames


def main() -> int:
    try:
        rows, id_count, protected_ids, fieldnames = convert_csv(INPUT_CSV, OUTPUT_CSV)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"Rows written: {rows}")
    print(f"Columns written: {len(fieldnames)}")
    print(f"Dropped columns: {', '.join(sorted(DROP_COLUMNS))}")
    print(f"Numeric matchId values protected: {id_count}")
    for match_id in sorted(protected_ids):
        print(f"Protected matchId: {match_id}")
    print(f"Slim Excel-safe CSV output: {OUTPUT_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
