from __future__ import annotations

import csv
import gzip
import json
import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Protocol


TEAM_ID = os.environ.get("CIZR_TEAM_ID", "5a7c84871100000100e61426")
STAT_GROUP = os.environ.get("CIZR_STAT_GROUP", "Singles")
INTEGRATION_TOKEN = (
    "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9."
    "eyJzdWIiOiI2OTcxM2NjNjNhMDAwMDgxOTc5N2IxNWEiLCJyb2xlIjoiUmVnaXN0ZXJlZCIsImlzcyI6ImFwcCIsImV4cCI6MjIwODEzMjMyMX0."
    "77-977-977-9BO-_ve-_vXoy77-9C0zvv73vv70YIe-_ve-_vVHvv71Z77-977-9Ue-_vV_vv712ZwQ"
)

OUTPUT_DIR = Path("output")
RAW_OUTPUT = OUTPUT_DIR / "singles.json"
CSV_GZ_OUTPUT = OUTPUT_DIR / "singles.csv.gz"
ProgressCallback = Callable[[int, int | None], None]


class CancelToken(Protocol):
    def is_set(self) -> bool:
        """Return whether cancellation has been requested."""


class DownloadCanceled(RuntimeError):
    """Raised when the user cancels an active CIZR download."""


def raise_if_canceled(cancel_token: CancelToken | None) -> None:
    if cancel_token and cancel_token.is_set():
        raise DownloadCanceled("Download canceled by user.")


def build_url(team_id: str = TEAM_ID, stat_group: str = STAT_GROUP, include_owned: bool = True) -> str:
    """Build the CIZR match stats endpoint for the requested team/stat group."""
    url = f"https://www.cizrtennis.com/api/matchStats/team/{team_id}/{stat_group}"
    if include_owned:
        return f"{url}?includeOwned=true"
    return url


def fetch_endpoint(
    token: str = INTEGRATION_TOKEN,
    url: str | None = None,
    progress_callback: ProgressCallback | None = None,
    cancel_token: CancelToken | None = None,
) -> tuple[int, dict[str, str], str]:
    """Call the CIZR endpoint and return status, headers, and response body."""
    request = urllib.request.Request(
        url or build_url(),
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json,text/csv,*/*",
            "User-Agent": "CIZR-python-fetch/1.0",
        },
        method="GET",
    )

    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            body = read_response_body(response, progress_callback, cancel_token)
            return response.status, dict(response.headers.items()), body
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return exc.code, dict(exc.headers.items()), body


def read_response_body(
    response: Any,
    progress_callback: ProgressCallback | None = None,
    cancel_token: CancelToken | None = None,
) -> str:
    """Read the response in chunks so callers can surface download progress."""
    total_header = response.headers.get("Content-Length")
    total = int(total_header) if total_header and total_header.isdigit() else None
    downloaded = 0
    chunks: list[bytes] = []

    while True:
        raise_if_canceled(cancel_token)
        chunk = response.read(1024 * 64)
        if not chunk:
            break
        chunks.append(chunk)
        downloaded += len(chunk)
        if progress_callback:
            progress_callback(downloaded, total)

    if progress_callback:
        progress_callback(downloaded, total)

    return b"".join(chunks).decode("utf-8", errors="replace")


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


def write_text_atomic(path: Path, text: str) -> None:
    """Write text through a temporary file before replacing the final path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    try:
        temp_path.write_text(text, encoding="utf-8")
        temp_path.replace(path)
    finally:
        temp_path.unlink(missing_ok=True)


def write_csv_gz(
    rows: list[dict[str, Any]],
    output_path: Path = CSV_GZ_OUTPUT,
    cancel_token: CancelToken | None = None,
) -> None:
    """Write converted rows to a gzipped CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_name(f"{output_path.name}.tmp")
    try:
        with gzip.open(temp_path, "wt", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames_for(rows))
            writer.writeheader()
            for row in rows:
                raise_if_canceled(cancel_token)
                writer.writerow(row)
        temp_path.replace(output_path)
    finally:
        temp_path.unlink(missing_ok=True)


def summarize_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Return quick checks that the export is useful."""
    match_ids = {row.get("matchId") for row in rows if row.get("matchId")}
    dates = sorted({row.get("date") for row in rows if row.get("date")})
    summary: dict[str, Any] = {"rows": len(rows)}

    if match_ids:
        summary["unique_matches"] = len(match_ids)
    if dates:
        summary["first_date"] = dates[0]
        summary["last_date"] = dates[-1]

    return summary


def retrieve_singles(
    output_path: Path = CSV_GZ_OUTPUT,
    raw_output_path: Path = RAW_OUTPUT,
    team_id: str = TEAM_ID,
    stat_group: str = STAT_GROUP,
    include_owned: bool = True,
    progress_callback: ProgressCallback | None = None,
    status_callback: Callable[[str], None] | None = None,
    cancel_token: CancelToken | None = None,
) -> dict[str, Any]:
    """Fetch singles data from CIZR, convert it, and store it as CSV gzip."""
    url = build_url(team_id, stat_group, include_owned)
    if status_callback:
        status_callback("Connecting to CIZR...")
    status, headers, body = fetch_endpoint(INTEGRATION_TOKEN, url, progress_callback, cancel_token)

    raise_if_canceled(cancel_token)
    if status_callback:
        status_callback("Saving raw response...")
    write_text_atomic(raw_output_path, body)

    if status < 200 or status >= 300:
        raise RuntimeError(f"Request failed with status {status}. Raw response saved to {raw_output_path}")

    raise_if_canceled(cancel_token)
    if status_callback:
        status_callback("Converting response to rows...")
    rows = convert_body_to_rows(body)
    if not rows:
        raise RuntimeError("Endpoint returned no rows.")

    raise_if_canceled(cancel_token)
    if status_callback:
        status_callback("Writing gzip CSV...")
    write_csv_gz(rows, output_path, cancel_token)

    if status_callback:
        status_callback("Download complete.")
    return {
        "url": url,
        "status": status,
        "content_type": headers.get("Content-Type", "unknown"),
        "raw_output": raw_output_path,
        "csv_output": output_path,
        **summarize_rows(rows),
    }
