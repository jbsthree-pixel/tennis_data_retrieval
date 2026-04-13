from __future__ import annotations

import csv
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


TEAM_ID = os.environ.get("CIZR_TEAM_ID", "5a7c84871100000100e61426")
STAT_GROUP = os.environ.get("CIZR_STAT_GROUP", "Singles")
URL = f"https://www.cizrtennis.com/api/matchStats/team/{TEAM_ID}/{STAT_GROUP}"

OUTPUT_DIR = Path("output")
RAW_OUTPUT = OUTPUT_DIR / "singles.json"
CSV_OUTPUT = OUTPUT_DIR / "singles.csv"


def get_token() -> str:
    """Read the bearer token from the current shell environment."""
    token = os.environ.get("CIZR_TOKEN")
    if not token:
        raise RuntimeError("Missing CIZR_TOKEN environment variable.")
    return token


def fetch_endpoint(token: str) -> tuple[int, dict[str, str], str]:
    """Call the CIZR endpoint and return status, headers, and response body."""
    request = urllib.request.Request(
        URL,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json,text/csv,*/*",
            "User-Agent": "CIZR-python-fetch/1.0",
        },
        method="GET",
    )

    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read().decode("utf-8", errors="replace")
            return response.status, dict(response.headers.items()), body
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return exc.code, dict(exc.headers.items()), body


def flatten_json(value: Any, prefix: str = "") -> dict[str, Any]:
    """Flatten nested JSON into CSV-friendly columns."""
    if isinstance(value, dict):
        row: dict[str, Any] = {}
        for key, item in value.items():
            name = f"{prefix}.{key}" if prefix else str(key)
            row.update(flatten_json(item, name))
        return row

    if isinstance(value, list):
        return {prefix: json.dumps(value, ensure_ascii=False)}

    return {prefix: value}


def find_records(data: Any) -> list[Any]:
    """Find the list of records in common API response shapes."""
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for key in ("data", "results", "items", "records", "matches"):
            value = data.get(key)
            if isinstance(value, list):
                return value

    return [data]


def convert_body_to_rows(body: str) -> list[dict[str, Any]]:
    """Convert a CSV or JSON response body into row dictionaries."""
    stripped = body.lstrip()

    if stripped.startswith("matchId,"):
        return list(csv.DictReader(body.splitlines()))

    data = json.loads(body)
    records = find_records(data)
    return [
        flatten_json(record) if isinstance(record, dict) else {"value": record}
        for record in records
    ]


def fieldnames_for(rows: list[dict[str, Any]]) -> list[str]:
    """Preserve first-seen column order across all rows."""
    fields: list[str] = []
    seen: set[str] = set()

    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)

    return fields


def write_csv(rows: list[dict[str, Any]]) -> None:
    """Write converted rows to output/singles.csv."""
    CSV_OUTPUT.parent.mkdir(exist_ok=True)
    with CSV_OUTPUT.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames_for(rows))
        writer.writeheader()
        writer.writerows(rows)


def print_summary(rows: list[dict[str, Any]]) -> None:
    """Print quick checks that the export is useful."""
    match_ids = {row.get("matchId") for row in rows if row.get("matchId")}
    dates = sorted({row.get("date") for row in rows if row.get("date")})

    print(f"Rows written: {len(rows)}")
    if match_ids:
        print(f"Unique matches: {len(match_ids)}")
    if dates:
        print(f"Date range: {dates[0]} through {dates[-1]}")
    print(f"Raw response: {RAW_OUTPUT}")
    print(f"CSV output: {CSV_OUTPUT}")


def main() -> int:
    try:
        token = get_token()
        status, headers, body = fetch_endpoint(token)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    OUTPUT_DIR.mkdir(exist_ok=True)
    RAW_OUTPUT.write_text(body, encoding="utf-8")

    if status < 200 or status >= 300:
        print(f"Request failed with status {status}. Raw response saved to {RAW_OUTPUT}", file=sys.stderr)
        return 1

    try:
        rows = convert_body_to_rows(body)
    except Exception as exc:
        print(f"Could not convert response to CSV: {exc}", file=sys.stderr)
        return 1

    if not rows:
        print("Endpoint returned no rows.", file=sys.stderr)
        return 1

    write_csv(rows)

    print(f"Endpoint: {URL}")
    print(f"Status: {status}")
    print(f"Content-Type: {headers.get('Content-Type', 'unknown')}")
    print_summary(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
