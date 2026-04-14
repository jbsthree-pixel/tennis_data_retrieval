from __future__ import annotations

import sys

from cizr_singles import retrieve_singles


def print_summary(summary: dict[str, object]) -> None:
    """Print quick checks that the export is useful."""
    print(f"Rows written: {summary['rows']}")
    if "unique_matches" in summary:
        print(f"Unique matches: {summary['unique_matches']}")
    if "first_date" in summary and "last_date" in summary:
        print(f"Date range: {summary['first_date']} through {summary['last_date']}")
    print(f"Raw response: {summary['raw_output']}")
    print(f"CSV gzip output: {summary['csv_output']}")


def main() -> int:
    try:
        summary = retrieve_singles()
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"Endpoint: {summary['url']}")
    print(f"Status: {summary['status']}")
    print(f"Content-Type: {summary['content_type']}")
    print_summary(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
