from __future__ import annotations

import hashlib
import csv
import io
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


TEAM_ID = os.environ.get("CIZR_TEAM_ID", "5a7c84871100000100e61426")
STAT_GROUP = os.environ.get("CIZR_STAT_GROUP", "Singles")
BASE_URL = f"https://www.cizrtennis.com/api/matchStats/team/{TEAM_ID}/{STAT_GROUP}"
OUTPUT_DIR = Path("output")
HOST = "https://www.cizrtennis.com"


@dataclass
class ProbeResult:
    label: str
    url: str
    status: int | None
    headers: dict[str, str]
    body_text: str
    json_data: Any | None
    error: str | None = None

    @property
    def body_hash(self) -> str:
        return hashlib.sha256(self.body_text.encode("utf-8", errors="replace")).hexdigest()[:16]


def get_token() -> str:
    """Read the bearer token without ever printing it."""
    for name in ("CIZR_token", "CIZR_TOKEN", "cizr_token", "CIZR_TOEKN"):
        token = os.environ.get(name)
        if token:
            return token
    raise RuntimeError(
        "No CIZR token was found. Set CIZR_TOKEN in this same shell, then run: "
        "python src/main.py"
    )


def build_url(params: dict[str, Any] | None = None) -> str:
    if not params:
        return BASE_URL
    return f"{BASE_URL}?{urllib.parse.urlencode(params)}"


def build_api_url(path: str) -> str:
    return f"{HOST}{path}"


def fetch_json(label: str, url: str, token: str) -> ProbeResult:
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "CIZR-curl-debug/1.0",
        },
        method="GET",
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8", errors="replace")
            headers = dict(response.headers.items())
            status = response.status
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        headers = dict(exc.headers.items()) if exc.headers else {}
        return ProbeResult(label, url, exc.code, headers, body, parse_json(body), str(exc))
    except urllib.error.URLError as exc:
        return ProbeResult(label, url, None, {}, "", None, str(exc))

    return ProbeResult(label, url, status, headers, body, parse_json(body))


def send_json(method: str, label: str, url: str, token: str, payload: Any | None = None) -> ProbeResult:
    """Send a JSON request and capture the response without printing secrets."""
    body_bytes = None
    if payload is not None:
        body_bytes = json.dumps(payload).encode("utf-8")

    request = urllib.request.Request(
        url,
        data=body_bytes,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "CIZR-curl-debug/1.0",
        },
        method=method,
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8", errors="replace")
            headers = dict(response.headers.items())
            status = response.status
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        headers = dict(exc.headers.items()) if exc.headers else {}
        return ProbeResult(label, url, exc.code, headers, body, parse_json(body), str(exc))
    except urllib.error.URLError as exc:
        return ProbeResult(label, url, None, {}, "", None, str(exc))

    return ProbeResult(label, url, status, headers, body, parse_json(body))


def parse_json(body: str) -> Any | None:
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return None


def parse_csv_rows(body: str) -> list[dict[str, str]]:
    if not body.lstrip().startswith("matchId,"):
        return []
    return list(csv.DictReader(io.StringIO(body)))


def summarize_csv(rows: list[dict[str, str]]) -> dict[str, Any]:
    if not rows:
        return {}

    headers = list(rows[0].keys())
    summary: dict[str, Any] = {
        "row_count": len(rows),
        "headers": headers,
    }

    for field in ("matchId", "date", "matchName", "player", "opp", "matchType"):
        values = [row.get(field, "") for row in rows if row.get(field, "")]
        summary[f"{field}_non_empty"] = len(values)
        summary[f"{field}_unique"] = len(set(values))

    dates = sorted({row["date"] for row in rows if row.get("date")})
    if dates:
        summary["first_date"] = dates[0]
        summary["last_date"] = dates[-1]

    return summary


def find_arrays(data: Any) -> list[tuple[str, list[Any]]]:
    """Find arrays that might hold records without assuming the API shape."""
    arrays: list[tuple[str, list[Any]]] = []

    def walk(value: Any, path: str) -> None:
        if isinstance(value, list):
            arrays.append((path, value))
            for index, item in enumerate(value[:3]):
                walk(item, f"{path}[{index}]")
        elif isinstance(value, dict):
            for key, item in value.items():
                walk(item, f"{path}.{key}" if path else key)

    walk(data, "$")
    return arrays


def likely_records(data: Any) -> tuple[str, list[Any]]:
    if isinstance(data, list):
        return "$", data

    arrays = find_arrays(data)
    object_arrays = [
        (path, value) for path, value in arrays if value and isinstance(value[0], dict)
    ]
    if object_arrays:
        return max(object_arrays, key=lambda item: len(item[1]))

    if arrays:
        return max(arrays, key=lambda item: len(item[1]))

    return "$", []


def summarize_json(data: Any) -> dict[str, Any]:
    path, records = likely_records(data)
    summary: dict[str, Any] = {
        "record_path": path,
        "record_count": len(records),
        "top_level_type": type(data).__name__,
    }

    if isinstance(data, dict):
        summary["top_level_keys"] = sorted(data.keys())

    if records and isinstance(records[0], dict):
        keys = sorted({key for row in records[:50] for key in row.keys()})
        summary["sample_record_keys"] = keys
        summary["field_counts"] = summarize_fields(records, keys)

    return summary


def summarize_fields(records: list[Any], keys: list[str]) -> dict[str, Any]:
    """Summarize values that often reveal hidden filters, like dates or match type."""
    interesting_parts = (
        "date",
        "season",
        "year",
        "type",
        "status",
        "event",
        "match",
        "round",
        "team",
        "player",
        "division",
        "gender",
    )
    summaries: dict[str, Any] = {}

    for key in keys:
        lower = key.lower()
        if not any(part in lower for part in interesting_parts):
            continue

        values = [row.get(key) for row in records if isinstance(row, dict) and row.get(key) not in (None, "")]
        unique_values = sorted({stable_string(value) for value in values})
        entry: dict[str, Any] = {"non_empty": len(values), "unique": len(unique_values)}
        if len(unique_values) <= 20:
            entry["values"] = unique_values
        else:
            entry["first_values"] = unique_values[:10]
            entry["last_values"] = unique_values[-10:]
        summaries[key] = entry

    return summaries


def stable_string(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def safe_file_label(label: str) -> str:
    return "".join(char if char.isalnum() else "_" for char in label).strip("_").lower()


def save_result(result: ProbeResult) -> Path:
    OUTPUT_DIR.mkdir(exist_ok=True)
    path = OUTPUT_DIR / f"{safe_file_label(result.label)}.json"
    csv_rows = parse_csv_rows(result.body_text) if result.json_data is None else []
    payload = {
        "label": result.label,
        "url": result.url,
        "status": result.status,
        "headers": result.headers,
        "body_sha256_16": result.body_hash,
        "error": result.error,
        "json_summary": summarize_json(result.json_data) if result.json_data is not None else None,
        "csv_summary": summarize_csv(csv_rows),
        "json": result.json_data,
        "raw_body": None if result.json_data is not None else result.body_text,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def print_result(result: ProbeResult, path: Path) -> None:
    print(f"\n[{result.label}]")
    print(f"status: {result.status}")
    print(f"body_hash: {result.body_hash}")
    print(f"saved: {path}")
    if result.error:
        print(f"error: {result.error}")

    link = result.headers.get("Link") or result.headers.get("link")
    if link:
        print(f"link_header: {link}")

    if result.json_data is None:
        csv_rows = parse_csv_rows(result.body_text)
        if csv_rows:
            summary = summarize_csv(csv_rows)
            print(f"csv_rows: {summary['row_count']}")
            print(f"unique_matches: {summary.get('matchId_unique', 0)}")
            print(f"date_range: {summary.get('first_date')} to {summary.get('last_date')}")
            print(f"csv_headers: {', '.join(summary['headers'])}")
            return
        print(f"body_chars: {len(result.body_text)}")
        print(f"body_start: {result.body_text[:200]}")
        return

    summary = summarize_json(result.json_data)
    print(f"record_path: {summary['record_path']}")
    print(f"record_count: {summary['record_count']}")
    if summary.get("top_level_keys"):
        print(f"top_level_keys: {', '.join(summary['top_level_keys'])}")
    if summary.get("sample_record_keys"):
        print(f"sample_record_keys: {', '.join(summary['sample_record_keys'])}")


def probe_definitions() -> list[tuple[str, dict[str, Any] | None]]:
    """Common API pagination dialects; ignored params usually produce identical hashes."""
    return [
        ("original", None),
        ("limit_1000", {"limit": 1000}),
        ("limit_5000", {"limit": 5000}),
        ("page_size_1000", {"pageSize": 1000}),
        ("per_page_1000", {"perPage": 1000}),
        ("page_1_limit_1000", {"page": 1, "limit": 1000}),
        ("page_0_limit_1000", {"page": 0, "limit": 1000}),
        ("offset_0_limit_1000", {"offset": 0, "limit": 1000}),
        ("skip_0_take_1000", {"skip": 0, "take": 1000}),
    ]


def summarize_match_endpoint(label: str, data: Any) -> None:
    if not isinstance(data, (dict, list)):
        return

    if isinstance(data, list):
        print(f"{label}_array_count: {len(data)}")
        if data and isinstance(data[0], dict):
            print(f"{label}_first_keys: {', '.join(sorted(data[0].keys()))}")
        return

    print(f"{label}_keys: {', '.join(sorted(data.keys()))}")
    metadata = data.get("metadata")
    if isinstance(metadata, dict):
        for key in ("name", "date", "teamType", "score", "matchId"):
            if key in metadata:
                print(f"metadata.{key}: {metadata.get(key)}")

    for key in ("id", "status", "postprocessedVideoUri", "rawVideoUri"):
        if key in data:
            print(f"{key}: {data.get(key)}")


def probe_match(match_id: str, token: str) -> int:
    endpoints = [
        ("match_metadata", f"/api/matches/{match_id}"),
        ("match_annotation", f"/api/matches/{match_id}/annotation"),
        ("match_links", f"/api/matches/links/byMatch/{match_id}"),
        ("favorites_by_match", f"/api/favorites/byMatch/{match_id}"),
    ]

    print(f"Match ID: {match_id}")
    print("Checking the endpoints used by the watch page.")
    print("The watch route loads /api/matches/{id} and /api/matches/{id}/annotation.")

    results: list[ProbeResult] = []
    for label, path in endpoints:
        result = fetch_json(label, build_api_url(path), token)
        results.append(result)
        saved_path = save_result(result)
        print_result(result, saved_path)
        summarize_match_endpoint(label, result.json_data)
        time.sleep(0.2)

    metadata = next((r for r in results if r.label == "match_metadata"), None)
    annotation = next((r for r in results if r.label == "match_annotation"), None)

    print("\nMatch endpoint interpretation:")
    if metadata and metadata.status and 200 <= metadata.status < 300:
        print("- The match metadata endpoint is accessible, so the match exists through the app API.")
    elif metadata:
        print(f"- The match metadata endpoint returned {metadata.status}; your integration token may not access watch-page APIs.")

    if annotation and annotation.status and 200 <= annotation.status < 300:
        if annotation.json_data in (None, [], {}):
            print("- The annotation endpoint is empty, so matchStats has no point-by-point source data to export.")
        else:
            print("- The annotation endpoint has data. If this ID is absent from matchStats, the export endpoint is filtering it out.")
    elif annotation:
        print(f"- The annotation endpoint returned {annotation.status}; that blocks direct confirmation of point data.")

    return 0


def probe_all_for_user(user_or_team_id: str, token: str, contains_match_id: str | None = None) -> int:
    params = urllib.parse.urlencode({"allForUser": user_or_team_id, "noLimit": "true"})
    url = build_api_url(f"/api/matches?{params}")
    result = fetch_json(f"all_for_user_{user_or_team_id}", url, token)
    saved_path = save_result(result)
    print_result(result, saved_path)

    data = result.json_data
    if not isinstance(data, list):
        print("\nThe endpoint did not return a JSON list of matches.")
        return 1

    singles = [
        match
        for match in data
        if isinstance(match, dict)
        and isinstance(match.get("metadata"), dict)
        and str(match["metadata"].get("teamType", "")).lower() == "singles"
    ]
    ids = {str(match.get("id", "")).strip() for match in data if isinstance(match, dict)}
    singles_ids = {str(match.get("id", "")).strip() for match in singles}

    print("\nallForUser interpretation:")
    print(f"- Returned matches: {len(data)}")
    print(f"- Returned singles matches: {len(singles)}")

    statuses: dict[str, int] = {}
    for match in data:
        if not isinstance(match, dict):
            continue
        status = str(match.get("status", ""))
        statuses[status] = statuses.get(status, 0) + 1
    if statuses:
        print("- Status counts: " + ", ".join(f"{key}={value}" for key, value in sorted(statuses.items())))

    if contains_match_id:
        print(f"- Contains {contains_match_id}: {contains_match_id in ids}")
        print(f"- Contains {contains_match_id} as Singles: {contains_match_id in singles_ids}")

    return 0


def load_workbook_match_ids(path: Path, team_type: str = "Singles") -> list[dict[str, str]]:
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("pandas is required to read the reconciled Excel workbook.") from exc

    frame = pd.read_excel(path, sheet_name="cizr_reconciled_matches", dtype=str)
    frame.columns = [str(column).strip() for column in frame.columns]
    filtered = frame[
        frame["team_type"].fillna("").str.lower().eq(team_type.lower())
        & frame["match_id"].fillna("").str.strip().ne("")
    ].copy()

    rows: list[dict[str, str]] = []
    for _, row in filtered.iterrows():
        rows.append(
            {
                "match_id": str(row.get("match_id", "")).strip(),
                "date": str(row.get("date", "") or "")[:10],
                "workbook_match_name": str(row.get("current_match_name_in_CIZR", "") or ""),
                "official_name": str(row.get("official_name", "") or ""),
                "alignment_status": str(row.get("alignment_status", "") or ""),
                "unresolved_reason": str(row.get("unresolved_reason", "") or ""),
            }
        )
    return rows


def api_match_stats_ids() -> set[str]:
    path = OUTPUT_DIR / "original.json"
    if not path.exists():
        return set()

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return set()

    body = payload.get("raw_body")
    if not isinstance(body, str):
        return set()

    return {row["matchId"].strip() for row in parse_csv_rows(body) if row.get("matchId")}


def iter_players(match_data: dict[str, Any]) -> list[dict[str, str]]:
    metadata = match_data.get("metadata")
    if not isinstance(metadata, dict):
        return []

    players = metadata.get("players")
    if not isinstance(players, list):
        return []

    extracted: list[dict[str, str]] = []
    for team_index, team_players in enumerate(players):
        if not isinstance(team_players, list):
            continue
        for player_index, player in enumerate(team_players):
            if not isinstance(player, dict):
                continue
            extracted.append(
                {
                    "side": str(team_index),
                    "player_slot": str(player_index),
                    "player_name": str(player.get("name", "") or ""),
                    "player_id": str(player.get("id", "") or ""),
                    "email": str(player.get("email", "") or ""),
                    "visual_cue": str(player.get("visualCue", "") or ""),
                }
            )
    return extracted


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_player_id_workbook() -> None:
    try:
        import pandas as pd
    except ImportError:
        return

    workbook_path = OUTPUT_DIR / "player_id_tables.xlsx"
    sheets = {
        "team_side_players": OUTPUT_DIR / "team_side_player_name_ids.csv",
        "all_players": OUTPUT_DIR / "player_name_ids.csv",
        "match_player_detail": OUTPUT_DIR / "match_players_with_ids_detail.csv",
        "fetch_failures": OUTPUT_DIR / "player_id_fetch_failures.csv",
    }
    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        for sheet_name, csv_path in sheets.items():
            if csv_path.exists():
                pd.read_csv(csv_path).to_excel(writer, sheet_name=sheet_name, index=False)


def normalize_player_name(name: str) -> str:
    """Normalize names enough to match spacing/case without hiding real spelling differences."""
    return " ".join(name.strip().lower().split())


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def propose_player_id_updates() -> int:
    detail_path = OUTPUT_DIR / "match_players_with_ids_detail.csv"
    if not detail_path.exists():
        print(
            f"Missing {detail_path}. Run `python src/main.py --player-id-table` first.",
            file=sys.stderr,
        )
        return 1

    rows = read_csv_rows(detail_path)
    team_rows = [row for row in rows if row.get("side") == "0"]
    linked_rows = [row for row in team_rows if row.get("player_id")]
    target_rows = [
        row
        for row in team_rows
        if not row.get("player_id") and row.get("in_match_stats_export") == "False"
    ]

    candidates_by_name: defaultdict[str, dict[tuple[str, str], set[str]]] = defaultdict(lambda: defaultdict(set))
    display_names: dict[str, set[str]] = defaultdict(set)
    for row in linked_rows:
        normalized = normalize_player_name(row.get("player_name", ""))
        if not normalized:
            continue
        key = (row.get("player_id", ""), row.get("email", ""))
        candidates_by_name[normalized][key].add(row.get("match_id", ""))
        display_names[normalized].add(row.get("player_name", ""))

    proposed: list[dict[str, str]] = []
    needs_review: list[dict[str, str]] = []
    no_candidate: list[dict[str, str]] = []
    seen_targets: set[tuple[str, str, str]] = set()

    for row in target_rows:
        player_name = row.get("player_name", "")
        normalized = normalize_player_name(player_name)
        target_key = (row.get("match_id", ""), row.get("side", ""), row.get("player_slot", ""))
        if target_key in seen_targets:
            continue
        seen_targets.add(target_key)

        base = {
            "match_id": row.get("match_id", ""),
            "date": row.get("date", ""),
            "api_match_name": row.get("api_match_name", ""),
            "player_name": player_name,
            "normalized_player_name": normalized,
            "current_player_id": row.get("player_id", ""),
            "side": row.get("side", ""),
            "player_slot": row.get("player_slot", ""),
            "visual_cue": row.get("visual_cue", ""),
            "alignment_status": row.get("alignment_status", ""),
            "unresolved_reason": row.get("unresolved_reason", ""),
        }

        candidates = candidates_by_name.get(normalized, {})
        if not candidates:
            no_candidate.append(
                {
                    **base,
                    "reason": "No linked player with the same normalized name was found.",
                }
            )
            continue

        if len(candidates) == 1:
            (player_id, email), source_matches = next(iter(candidates.items()))
            proposed.append(
                {
                    **base,
                    "new_player_id": player_id,
                    "new_email": email,
                    "confidence": "high",
                    "reason": "Exactly one linked player matched the normalized player name.",
                    "candidate_source_match_count": str(len(source_matches)),
                    "candidate_source_match_ids": ";".join(sorted(source_matches)[:10]),
                    "candidate_display_names": ";".join(sorted(display_names.get(normalized, []))),
                }
            )
            continue

        candidate_text = []
        for (player_id, email), source_matches in sorted(candidates.items()):
            candidate_text.append(
                f"{player_id}|{email}|matches={len(source_matches)}|examples={';'.join(sorted(source_matches)[:5])}"
            )
        needs_review.append(
            {
                **base,
                "reason": "Multiple linked players matched the same normalized name.",
                "candidates": " || ".join(candidate_text),
                "candidate_display_names": ";".join(sorted(display_names.get(normalized, []))),
            }
        )

    proposed_fields = [
        "match_id",
        "date",
        "api_match_name",
        "player_name",
        "normalized_player_name",
        "current_player_id",
        "new_player_id",
        "new_email",
        "confidence",
        "reason",
        "candidate_source_match_count",
        "candidate_source_match_ids",
        "candidate_display_names",
        "side",
        "player_slot",
        "visual_cue",
        "alignment_status",
        "unresolved_reason",
    ]
    review_fields = [
        "match_id",
        "date",
        "api_match_name",
        "player_name",
        "normalized_player_name",
        "current_player_id",
        "reason",
        "candidates",
        "candidate_display_names",
        "side",
        "player_slot",
        "visual_cue",
        "alignment_status",
        "unresolved_reason",
    ]
    no_candidate_fields = [
        "match_id",
        "date",
        "api_match_name",
        "player_name",
        "normalized_player_name",
        "current_player_id",
        "reason",
        "side",
        "player_slot",
        "visual_cue",
        "alignment_status",
        "unresolved_reason",
    ]

    write_csv(OUTPUT_DIR / "proposed_player_id_updates.csv", proposed, proposed_fields)
    write_csv(OUTPUT_DIR / "player_id_update_needs_review.csv", needs_review, review_fields)
    write_csv(OUTPUT_DIR / "player_id_update_no_candidate.csv", no_candidate, no_candidate_fields)

    try:
        import pandas as pd

        workbook_path = OUTPUT_DIR / "player_id_update_proposals.xlsx"
        with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
            pd.DataFrame(proposed, columns=proposed_fields).to_excel(writer, sheet_name="proposed_high_confidence", index=False)
            pd.DataFrame(needs_review, columns=review_fields).to_excel(writer, sheet_name="needs_review", index=False)
            pd.DataFrame(no_candidate, columns=no_candidate_fields).to_excel(writer, sheet_name="no_candidate", index=False)
    except ImportError:
        workbook_path = None

    print("No CIZR records were changed.")
    print("\nWrote:")
    print(f"- {OUTPUT_DIR / 'proposed_player_id_updates.csv'}")
    print(f"- {OUTPUT_DIR / 'player_id_update_needs_review.csv'}")
    print(f"- {OUTPUT_DIR / 'player_id_update_no_candidate.csv'}")
    if workbook_path:
        print(f"- {workbook_path}")

    print("\nSummary:")
    print(f"- Missing team-side player rows considered: {len(target_rows)}")
    print(f"- High-confidence proposed updates: {len(proposed)}")
    print(f"- Needs review: {len(needs_review)}")
    print(f"- No candidate found: {len(no_candidate)}")
    return 0


def generate_player_id_table(token: str, limit: int | None = None) -> int:
    workbook_path = Path("input") / "cizr_reconciled_matches.xlsx"
    if not workbook_path.exists():
        print(f"Missing workbook: {workbook_path}", file=sys.stderr)
        return 1

    match_rows = load_workbook_match_ids(workbook_path)
    if limit is not None:
        match_rows = match_rows[:limit]

    exported_ids = api_match_stats_ids()
    details: list[dict[str, str]] = []
    failures: list[dict[str, str]] = []

    for index, workbook_row in enumerate(match_rows, start=1):
        match_id = workbook_row["match_id"]
        print(f"[{index}/{len(match_rows)}] {match_id}")
        result = fetch_json(f"match_{match_id}", build_api_url(f"/api/matches/{match_id}"), token)
        if not (result.status and 200 <= result.status < 300 and isinstance(result.json_data, dict)):
            failures.append(
                {
                    **workbook_row,
                    "status": str(result.status or ""),
                    "error": result.error or result.body_text[:200],
                }
            )
            continue

        match_data = result.json_data
        for player in iter_players(match_data):
            details.append(
                {
                    **workbook_row,
                    "api_match_name": str(match_data.get("metadata", {}).get("name", "")),
                    "api_status": str(match_data.get("status", "")),
                    "owner_id": str(match_data.get("ownerId", "")),
                    "owner_email": str(match_data.get("ownerEmail", "")),
                    "in_match_stats_export": str(match_id in exported_ids),
                    "missing_player_id": str(player["player_id"] == ""),
                    **player,
                }
            )
        time.sleep(0.1)

    detail_fields = [
        "match_id",
        "date",
        "workbook_match_name",
        "official_name",
        "api_match_name",
        "api_status",
        "owner_id",
        "owner_email",
        "in_match_stats_export",
        "side",
        "player_slot",
        "player_name",
        "player_id",
        "missing_player_id",
        "email",
        "visual_cue",
        "alignment_status",
        "unresolved_reason",
    ]
    write_csv(OUTPUT_DIR / "match_players_with_ids_detail.csv", details, detail_fields)

    def build_player_summary(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
        grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
        match_counts: defaultdict[tuple[str, str, str], set[str]] = defaultdict(set)
        export_counts: defaultdict[tuple[str, str, str], set[str]] = defaultdict(set)
        missing_counts: defaultdict[tuple[str, str, str], set[str]] = defaultdict(set)
        for row in rows:
            key = (row["player_name"], row["player_id"], row["email"])
            grouped.setdefault(
                key,
                {
                    "player_name": row["player_name"],
                    "player_id": row["player_id"],
                    "email": row["email"],
                    "example_match_id": row["match_id"],
                    "example_match_name": row["api_match_name"],
                },
            )
            match_counts[key].add(row["match_id"])
            if row["in_match_stats_export"] == "True":
                export_counts[key].add(row["match_id"])
            else:
                missing_counts[key].add(row["match_id"])

        summary = []
        for key, row in grouped.items():
            summary.append(
                {
                    **row,
                    "match_count": len(match_counts[key]),
                    "match_stats_export_match_count": len(export_counts[key]),
                    "not_in_match_stats_export_match_count": len(missing_counts[key]),
                    "missing_player_id": str(row["player_id"] == ""),
                }
            )

        summary.sort(
            key=lambda row: (
                row["missing_player_id"] != "True",
                row["not_in_match_stats_export_match_count"] == 0,
                row["player_name"].lower(),
                row["player_id"],
            )
        )
        return summary

    summary_fields = [
        "player_name",
        "player_id",
        "email",
        "missing_player_id",
        "match_count",
        "match_stats_export_match_count",
        "not_in_match_stats_export_match_count",
        "example_match_id",
        "example_match_name",
    ]

    summary_rows = build_player_summary(details)
    write_csv(OUTPUT_DIR / "player_name_ids.csv", summary_rows, summary_fields)

    team_side_rows = [row for row in details if row["side"] == "0"]
    team_summary_rows = build_player_summary(team_side_rows)
    write_csv(OUTPUT_DIR / "team_side_player_name_ids.csv", team_summary_rows, summary_fields)

    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    match_counts: defaultdict[tuple[str, str, str], set[str]] = defaultdict(set)
    export_counts: defaultdict[tuple[str, str, str], set[str]] = defaultdict(set)
    failure_fields = ["match_id", "date", "workbook_match_name", "official_name", "status", "error"]
    write_csv(OUTPUT_DIR / "player_id_fetch_failures.csv", failures, failure_fields)
    write_player_id_workbook()

    missing_id_rows = [row for row in details if row["player_id"] == ""]
    print("\nWrote:")
    print(f"- {OUTPUT_DIR / 'player_name_ids.csv'}")
    print(f"- {OUTPUT_DIR / 'team_side_player_name_ids.csv'}")
    print(f"- {OUTPUT_DIR / 'match_players_with_ids_detail.csv'}")
    print(f"- {OUTPUT_DIR / 'player_id_fetch_failures.csv'}")
    print(f"- {OUTPUT_DIR / 'player_id_tables.xlsx'}")
    print("\nSummary:")
    print(f"- Match metadata fetched: {len(match_rows) - len(failures)}")
    print(f"- Fetch failures: {len(failures)}")
    print(f"- Player rows: {len(details)}")
    print(f"- Player rows missing id: {len(missing_id_rows)}")
    return 0


def parse_player_id_update_args(args: list[str]) -> dict[str, Any]:
    """Parse the small updater argument set without changing the older CLI flow."""
    options: dict[str, Any] = {
        "csv_path": OUTPUT_DIR / "proposed_player_id_updates.csv",
        "limit": None,
        "match_ids": set(),
        "all": False,
        "execute": False,
        "allow_existing": False,
    }

    index = 0
    while index < len(args):
        arg = args[index]
        if arg == "--csv":
            index += 1
            if index >= len(args):
                raise ValueError("--csv requires a path")
            options["csv_path"] = Path(args[index])
        elif arg == "--limit":
            index += 1
            if index >= len(args):
                raise ValueError("--limit requires a positive integer")
            limit = int(args[index])
            if limit <= 0:
                raise ValueError("--limit must be greater than zero")
            options["limit"] = limit
        elif arg == "--match-id":
            index += 1
            if index >= len(args):
                raise ValueError("--match-id requires a match id")
            for match_id in args[index].split(","):
                if match_id.strip():
                    options["match_ids"].add(match_id.strip())
        elif arg == "--all":
            options["all"] = True
        elif arg == "--execute":
            options["execute"] = True
        elif arg == "--allow-existing":
            options["allow_existing"] = True
        else:
            raise ValueError(f"Unknown option for --apply-player-id-updates: {arg}")
        index += 1

    return options


def load_selected_player_id_updates(
    csv_path: Path,
    limit: int | None,
    match_ids: set[str],
    include_all: bool,
) -> list[dict[str, str]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing updates CSV: {csv_path}")

    rows = read_csv_rows(csv_path)
    required_fields = {"match_id", "side", "player_slot", "new_player_id"}
    missing_fields = required_fields - set(rows[0].keys() if rows else [])
    if missing_fields:
        raise ValueError(f"{csv_path} is missing required columns: {', '.join(sorted(missing_fields))}")

    selected = [row for row in rows if row.get("new_player_id", "").strip()]
    if match_ids:
        selected = [row for row in selected if row.get("match_id", "").strip() in match_ids]

    if not include_all and limit is None and not match_ids:
        raise ValueError("Choose --limit N, --match-id ID, or --all before applying updates.")

    if limit is not None:
        selected = selected[:limit]

    return selected


def clean_metadata_for_update(metadata: dict[str, Any]) -> dict[str, Any]:
    """Mirror the app's basic metadata cleanup before PUT /metadata."""
    if metadata.get("teamType") == "Singles":
        players = metadata.get("players")
        if isinstance(players, list) and len(players) >= 2:
            for side in (0, 1):
                if isinstance(players[side], list) and players[side]:
                    players[side] = [players[side][0]]
    return metadata


def apply_update_to_metadata(
    metadata: dict[str, Any],
    row: dict[str, str],
    allow_existing: bool,
) -> tuple[bool, str, str]:
    players = metadata.get("players")
    if not isinstance(players, list):
        return False, "", "metadata.players is missing or is not a list"

    try:
        side = int(row.get("side", ""))
        player_slot = int(row.get("player_slot", ""))
    except ValueError:
        return False, "", "side/player_slot must be integers"

    if side < 0 or side >= len(players) or not isinstance(players[side], list):
        return False, "", f"side {side} is not present in metadata.players"
    if player_slot < 0 or player_slot >= len(players[side]) or not isinstance(players[side][player_slot], dict):
        return False, "", f"player_slot {player_slot} is not present on side {side}"

    player = players[side][player_slot]
    current_api_id = str(player.get("id", "") or "").strip()
    new_player_id = row.get("new_player_id", "").strip()
    if not new_player_id:
        return False, current_api_id, "new_player_id is blank"
    if current_api_id and current_api_id != new_player_id and not allow_existing:
        return False, current_api_id, "API already has a different player id; pass --allow-existing to overwrite"
    if current_api_id == new_player_id:
        return False, current_api_id, "API already has this player id"

    player["id"] = new_player_id
    new_email = row.get("new_email", "").strip()
    if new_email:
        player["email"] = new_email
    return True, current_api_id, "prepared"


def apply_player_id_updates(token: str, args: list[str]) -> int:
    try:
        options = parse_player_id_update_args(args)
        rows = load_selected_player_id_updates(
            options["csv_path"],
            options["limit"],
            options["match_ids"],
            options["all"],
        )
    except (FileNotFoundError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2

    execute = bool(options["execute"])
    mode = "EXECUTE" if execute else "DRY RUN"
    if not rows:
        print("No update rows matched the requested selection.")
        return 0

    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row.get("match_id", "").strip()].append(row)

    print(f"Player ID update mode: {mode}")
    print(f"Update rows selected: {len(rows)} across {len(grouped)} matches")
    if not execute:
        print("No CIZR records will be changed. Add --execute after reviewing the dry run.")

    results: list[dict[str, str]] = []
    updated_rows = 0
    skipped_rows = 0
    failed_matches = 0

    for match_index, (match_id, match_rows) in enumerate(grouped.items(), start=1):
        print(f"[{match_index}/{len(grouped)}] {match_id}: {len(match_rows)} row(s)")
        result = fetch_json(f"match_{match_id}", build_api_url(f"/api/matches/{match_id}"), token)
        if not (result.status and 200 <= result.status < 300 and isinstance(result.json_data, dict)):
            failed_matches += 1
            error = result.error or result.body_text[:200]
            for row in match_rows:
                results.append({**audit_base(row), "status": "fetch_failed", "api_current_player_id": "", "message": error})
            continue

        match_data = result.json_data
        metadata = match_data.get("metadata")
        if not isinstance(metadata, dict):
            failed_matches += 1
            for row in match_rows:
                results.append({**audit_base(row), "status": "skipped", "api_current_player_id": "", "message": "match metadata is missing"})
            continue

        changed = False
        for row in match_rows:
            did_update, api_current_id, message = apply_update_to_metadata(
                metadata,
                row,
                allow_existing=bool(options["allow_existing"]),
            )
            if did_update:
                changed = True
                updated_rows += 1
                status = "prepared" if not execute else "updated"
            else:
                skipped_rows += 1
                status = "skipped"
            results.append({**audit_base(row), "status": status, "api_current_player_id": api_current_id, "message": message})

        if changed and execute:
            metadata = clean_metadata_for_update(metadata)
            update = send_json("PUT", f"update_metadata_{match_id}", build_api_url(f"/api/matches/{match_id}/metadata"), token, metadata)
            if not (update.status and 200 <= update.status < 300):
                failed_matches += 1
                message = update.error or update.body_text[:200]
                print(f"  metadata update failed: {update.status} {message}")
                for result_row in results:
                    if result_row["match_id"] == match_id and result_row["status"] == "updated":
                        result_row["status"] = "update_failed"
                        result_row["message"] = message
            else:
                print("  metadata updated")
            time.sleep(0.2)
        elif changed:
            print("  dry run prepared metadata update")
        else:
            print("  no metadata changes needed")

    audit_fields = [
        "match_id",
        "date",
        "api_match_name",
        "player_name",
        "side",
        "player_slot",
        "api_current_player_id",
        "new_player_id",
        "new_email",
        "status",
        "message",
    ]
    audit_path = OUTPUT_DIR / ("player_id_update_execute_results.csv" if execute else "player_id_update_dry_run_results.csv")
    write_csv(audit_path, results, audit_fields)

    print("\nSummary:")
    print(f"- Rows prepared/updated: {updated_rows}")
    print(f"- Rows skipped: {skipped_rows}")
    print(f"- Matches with fetch/update failures: {failed_matches}")
    print(f"- Audit CSV: {audit_path}")
    return 1 if failed_matches else 0


def audit_base(row: dict[str, str]) -> dict[str, str]:
    return {
        "match_id": row.get("match_id", ""),
        "date": row.get("date", ""),
        "api_match_name": row.get("api_match_name", ""),
        "player_name": row.get("player_name", ""),
        "side": row.get("side", ""),
        "player_slot": row.get("player_slot", ""),
        "new_player_id": row.get("new_player_id", ""),
        "new_email": row.get("new_email", ""),
    }


def explain(results: list[ProbeResult]) -> None:
    successful = [
        result
        for result in results
        if result.status and 200 <= result.status < 300 and (result.json_data is not None or parse_csv_rows(result.body_text))
    ]
    if not successful:
        print("\nNo successful JSON or CSV responses were returned. Check token validity and endpoint access.")
        return

    print("\nInterpretation:")
    counts_by_hash: dict[str, list[str]] = {}
    for result in successful:
        counts_by_hash.setdefault(result.body_hash, []).append(result.label)

    original = successful[0]
    original_csv_rows = parse_csv_rows(original.body_text)
    if original_csv_rows:
        original_summary = summarize_csv(original_csv_rows)
        original_count = int(original_summary["row_count"])
        print(
            f"- The original request returns {original_count} point rows "
            f"across {original_summary.get('matchId_unique', 0)} unique matches."
        )
        print(
            f"- The response date range is {original_summary.get('first_date')} "
            f"through {original_summary.get('last_date')}."
        )
    else:
        original_summary = summarize_json(original.json_data)
        original_count = int(original_summary["record_count"])
        print(f"- The original request returns {original_count} records from {original_summary['record_path']}.")

    if original_count in (25, 50, 100, 200, 500):
        print("- That count is a common default page size, so pagination or a server-side cap is plausible.")

    if len(counts_by_hash) == 1:
        print("- Every pagination-style probe returned the same body hash, so these common query params are probably ignored by this endpoint.")
    else:
        print("- Some probes returned different bodies. Compare their saved files to identify the accepted pagination parameter.")
        for body_hash, labels in counts_by_hash.items():
            print(f"  {body_hash}: {', '.join(labels)}")

    headers = {key.lower(): value for key, value in original.headers.items()}
    if "link" in headers:
        print("- A Link header is present; follow its next URL to retrieve additional pages.")
    elif any(key.startswith("x-") and ("page" in key or "total" in key or "count" in key) for key in headers):
        matching = [f"{key}={value}" for key, value in headers.items() if key.startswith("x-") and ("page" in key or "total" in key or "count" in key)]
        print(f"- Pagination/count headers are present: {', '.join(matching)}")
    else:
        print("- No obvious pagination headers were present in the original response.")

    print("- If the API still omits matches, the next thing to verify is whether this team id, stat group, or token scope is narrower than 'all singles'.")


def main() -> int:
    if len(sys.argv) >= 2 and sys.argv[1] == "--propose-player-id-updates":
        return propose_player_id_updates()

    try:
        token = get_token()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    print(f"Endpoint: {BASE_URL}")
    print("Token: present")

    if len(sys.argv) >= 3 and sys.argv[1] == "--match-id":
        return probe_match(sys.argv[2], token)

    if len(sys.argv) >= 3 and sys.argv[1] == "--all-for-user":
        contains = None
        if len(sys.argv) >= 5 and sys.argv[3] == "--contains-match-id":
            contains = sys.argv[4]
        return probe_all_for_user(sys.argv[2], token, contains)

    if len(sys.argv) >= 2 and sys.argv[1] == "--apply-player-id-updates":
        return apply_player_id_updates(token, sys.argv[2:])

    if len(sys.argv) >= 2 and sys.argv[1] == "--player-id-table":
        limit = None
        if len(sys.argv) >= 4 and sys.argv[2] == "--limit":
            limit = int(sys.argv[3])
        return generate_player_id_table(token, limit)

    results: list[ProbeResult] = []
    for label, params in probe_definitions():
        result = fetch_json(label, build_url(params), token)
        results.append(result)
        print_result(result, save_result(result))
        time.sleep(0.2)

    explain(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
