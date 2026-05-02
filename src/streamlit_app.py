from __future__ import annotations

import csv
import gzip
import queue
import threading
import time
from pathlib import Path
from typing import Any

from cizr_singles import (
    CSV_GZ_OUTPUT,
    DownloadCanceled,
    OUTPUT_DIR,
    retrieve_singles,
)
from main import (
    MATCH_NAME_DRY_RUN_RESULTS,
    MATCH_NAME_EXECUTE_RESULTS,
    MATCH_NAME_UPDATE_CSV,
    apply_match_name_updates,
    get_token,
    read_csv_rows,
    write_csv,
)


def clean_csv_gz_filename(filename: str) -> str:
    """Return a CSV gzip filename from a user-entered base filename."""
    clean_name = filename.strip()
    if clean_name.lower().endswith(".csv.gz"):
        clean_name = clean_name[:-7]
    elif clean_name.lower().endswith(".csv"):
        clean_name = clean_name[:-4]

    if not clean_name:
        clean_name = CSV_GZ_OUTPUT.stem.removesuffix(".csv")

    return f"{clean_name}.csv.gz"


def default_download_state() -> dict[str, Any]:
    return {
        "running": False,
        "cancel_requested": False,
        "progress": 0.0,
        "progress_text": "Ready to download.",
        "messages": [],
        "summary": None,
        "error": None,
        "canceled": False,
        "thread": None,
        "cancel_event": None,
        "queue": None,
    }


def ensure_download_state() -> dict[str, Any]:
    import streamlit as st

    if "download_state" not in st.session_state:
        st.session_state.download_state = default_download_state()
    return st.session_state.download_state


def reset_match_name_review_ui() -> None:
    import streamlit as st

    state = ensure_download_state()
    state["summary"] = None

    st.session_state.pop("match_name_review_state", None)
    st.session_state.pop("post_download_action", None)

    for key in list(st.session_state.keys()):
        if key == "execute_match_name_updates" or key.startswith(("rename_", "player_", "opp_")):
            st.session_state.pop(key, None)


def load_match_review_rows(csv_gz_path: Path) -> list[dict[str, str]]:
    if not csv_gz_path.exists():
        return []

    matches: dict[str, dict[str, str]] = {}
    with gzip.open(csv_gz_path, "rt", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            match_id = str(row.get("matchId", "") or "").strip()
            if not match_id or match_id in matches:
                continue
            matches[match_id] = {
                "match_id": match_id,
                "date": str(row.get("date", "") or "").strip(),
                "current_match_name": str(row.get("matchName", "") or "").strip(),
                "current_player_name": str(row.get("player", "") or "").strip(),
                "current_opp_name": str(row.get("opp", "") or "").strip(),
            }

    return sorted(
        matches.values(),
        key=lambda row: (row["date"], row["current_match_name"], row["match_id"]),
        reverse=True,
    )


def load_match_review_state(csv_gz_path: Path) -> list[dict[str, str]]:
    import streamlit as st

    def normalize_review_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
        normalized_rows: list[dict[str, str]] = []
        for row in rows:
            normalized_rows.append(
                {
                    "match_id": str(row.get("match_id", "") or "").strip(),
                    "date": str(row.get("date", "") or "").strip(),
                    "current_match_name": str(row.get("current_match_name", "") or "").strip(),
                    "current_player_name": str(
                        row.get("current_player_name", row.get("player", "")) or ""
                    ).strip(),
                    "current_opp_name": str(
                        row.get("current_opp_name", row.get("opp", "")) or ""
                    ).strip(),
                }
            )
        return normalized_rows

    signature = ""
    if csv_gz_path.exists():
        stat = csv_gz_path.stat()
        signature = f"{csv_gz_path.resolve()}:{stat.st_mtime_ns}:{stat.st_size}"

    state = st.session_state.get("match_name_review_state")
    if not isinstance(state, dict) or state.get("signature") != signature:
        st.session_state.match_name_review_state = {
            "signature": signature,
            "rows": load_match_review_rows(csv_gz_path),
        }
    else:
        st.session_state.match_name_review_state["rows"] = normalize_review_rows(
            list(st.session_state.match_name_review_state.get("rows", []))
        )

    return list(st.session_state.match_name_review_state["rows"])


def save_match_name_proposals(rows: list[dict[str, str]], path: Path = MATCH_NAME_UPDATE_CSV) -> Path:
    proposal_fields = [
        "match_id",
        "date",
        "current_match_name",
        "new_match_name",
        "current_player_name",
        "new_player_name",
        "current_opp_name",
        "new_opp_name",
    ]
    write_csv(path, rows, proposal_fields)
    return path


def build_selected_match_name_rows(review_rows: list[dict[str, str]], selected_match_ids: list[str]) -> list[dict[str, str]]:
    import streamlit as st

    selected_set = set(selected_match_ids)
    proposals: list[dict[str, str]] = []
    for row in review_rows:
        if row["match_id"] not in selected_set:
            continue
        new_match_name = str(st.session_state.get(f"rename_{row['match_id']}", row["current_match_name"])).strip()
        new_player_name = str(st.session_state.get(f"player_{row['match_id']}", row["current_player_name"])).strip()
        new_opp_name = str(st.session_state.get(f"opp_{row['match_id']}", row["current_opp_name"])).strip()

        has_match_change = bool(new_match_name) and new_match_name != row["current_match_name"]
        has_player_change = bool(new_player_name) and new_player_name != row["current_player_name"]
        has_opp_change = bool(new_opp_name) and new_opp_name != row["current_opp_name"]
        if not (has_match_change or has_player_change or has_opp_change):
            continue
        proposals.append(
            {
                **row,
                "new_match_name": new_match_name,
                "new_player_name": new_player_name,
                "new_opp_name": new_opp_name,
            }
        )

    return proposals


def inject_app_styles() -> None:
    import streamlit as st

    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top right, rgba(204, 0, 0, 0.10), transparent 28%),
                linear-gradient(180deg, #f7f3f1 0%, #f2ece8 100%);
        }
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2.5rem;
            max-width: none;
        }
        .hero-card {
            padding: 1.5rem 1.7rem;
            border-radius: 24px;
            background: linear-gradient(135deg, #8c1111 0%, #cc0000 58%, #f04c3e 100%);
            color: #fff7f5;
            box-shadow: 0 24px 48px rgba(112, 21, 21, 0.20);
            margin-bottom: 1rem;
        }
        .hero-eyebrow {
            text-transform: uppercase;
            letter-spacing: 0.14em;
            font-size: 0.75rem;
            font-weight: 700;
            opacity: 0.82;
            margin-bottom: 0.55rem;
        }
        .hero-title {
            font-size: 2.2rem;
            line-height: 1.1;
            font-weight: 800;
            margin-bottom: 0.45rem;
        }
        .hero-copy {
            max-width: 760px;
            font-size: 1rem;
            line-height: 1.55;
            opacity: 0.96;
        }
        .panel {
            background: rgba(255, 255, 255, 0.88);
            border: 1px solid rgba(148, 45, 39, 0.10);
            border-radius: 22px;
            padding: 1.15rem 1.15rem 0.85rem 1.15rem;
            box-shadow: 0 14px 34px rgba(71, 37, 29, 0.08);
            backdrop-filter: blur(8px);
            margin-bottom: 1rem;
        }
        .panel-title {
            font-size: 1.1rem;
            font-weight: 700;
            color: #4a1916;
            margin-bottom: 0.2rem;
        }
        .panel-copy {
            color: #714642;
            font-size: 0.94rem;
            margin-bottom: 0.9rem;
        }
        .info-card {
            background: linear-gradient(180deg, rgba(255,255,255,0.98), rgba(252,246,244,0.94));
            border: 1px solid rgba(143, 58, 48, 0.12);
            border-radius: 20px;
            padding: 1rem 1rem 0.85rem 1rem;
            min-height: 100%;
        }
        .info-kicker {
            text-transform: uppercase;
            letter-spacing: 0.12em;
            font-size: 0.73rem;
            font-weight: 700;
            color: #aa3a30;
            margin-bottom: 0.45rem;
        }
        .info-card p {
            color: #5f3d39;
            font-size: 0.93rem;
            line-height: 1.45;
            margin-bottom: 0.75rem;
        }
        .stat-strip {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 0.85rem;
            margin: 0.85rem 0 1rem 0;
        }
        .stat-chip {
            background: rgba(255,255,255,0.88);
            border: 1px solid rgba(148, 45, 39, 0.10);
            border-radius: 18px;
            padding: 0.85rem 1rem;
            box-shadow: 0 12px 28px rgba(71, 37, 29, 0.06);
        }
        .stat-label {
            font-size: 0.76rem;
            text-transform: uppercase;
            letter-spacing: 0.10em;
            color: #8a5753;
            margin-bottom: 0.28rem;
            font-weight: 700;
        }
        .stat-value {
            font-size: 1.18rem;
            font-weight: 800;
            color: #421714;
        }
        .match-editor {
            background: rgba(255,255,255,0.72);
            border: 1px solid rgba(148, 45, 39, 0.11);
            border-radius: 18px;
            padding: 0.9rem 1rem 0.25rem 1rem;
            margin-bottom: 0.8rem;
        }
        .match-editor-title {
            font-weight: 700;
            color: #4a1916;
            margin-bottom: 0.2rem;
        }
        .match-editor-copy {
            font-size: 0.9rem;
            color: #7c5853;
            margin-bottom: 0.6rem;
        }
        div[data-testid="stMetric"] {
            background: rgba(255,255,255,0.86);
            border: 1px solid rgba(148, 45, 39, 0.10);
            border-radius: 18px;
            padding: 0.8rem 0.95rem;
            box-shadow: 0 12px 26px rgba(71, 37, 29, 0.06);
        }
        div[data-testid="stExpander"] {
            border-radius: 18px;
            overflow: hidden;
            border: 1px solid rgba(148, 45, 39, 0.10);
            background: rgba(255,255,255,0.75);
        }
        div[data-testid="stDataFrame"] {
            border-radius: 16px;
            overflow: hidden;
        }
        .stButton > button, .stDownloadButton > button {
            border-radius: 999px;
            font-weight: 700;
            padding-left: 1rem;
            padding-right: 1rem;
        }
        @media (max-width: 900px) {
            .hero-title {
                font-size: 1.8rem;
            }
            .stat-strip {
                grid-template-columns: 1fr;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_panel_header(title: str, copy: str) -> None:
    import streamlit as st

    st.markdown(
        f"""
        <div class="panel-title">{title}</div>
        <div class="panel-copy">{copy}</div>
        """,
        unsafe_allow_html=True,
    )


def render_top_status(state: dict[str, Any]) -> None:
    import streamlit as st

    summary = state.get("summary") or {}
    export_name = Path(summary["csv_output"]).name if summary.get("csv_output") else "Not created yet"
    if state.get("running"):
        status_label = "Downloading"
    elif state.get("error"):
        status_label = "Needs attention"
    elif state.get("canceled"):
        status_label = "Canceled"
    elif summary:
        status_label = "Ready"
    else:
        status_label = "Waiting"

    row_count = f"{int(summary['rows']):,}" if summary.get("rows") is not None else "0"
    match_count = f"{int(summary['unique_matches']):,}" if summary.get("unique_matches") is not None else "0"

    st.markdown(
        f"""
        <div class="stat-strip">
            <div class="stat-chip">
                <div class="stat-label">Session status</div>
                <div class="stat-value">{status_label}</div>
            </div>
            <div class="stat-chip">
                <div class="stat-label">Current export</div>
                <div class="stat-value">{export_name}</div>
            </div>
            <div class="stat-chip">
                <div class="stat-label">Rows / matches</div>
                <div class="stat-value">{row_count} / {match_count}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_match_name_tools(csv_gz_path: Path, disabled: bool) -> None:
    import streamlit as st

    review_column_config = {
        "match_id": st.column_config.TextColumn("match_id", width="small"),
        "date": st.column_config.DateColumn("date", width="small", format="YYYY-MM-DD"),
        "current_match_name": st.column_config.TextColumn("current_match_name", width="medium"),
        "current_player_name": st.column_config.TextColumn("player", width="small"),
        "current_opp_name": st.column_config.TextColumn("opponent", width="small"),
    }
    proposal_column_config = {
        **review_column_config,
        "new_match_name": st.column_config.TextColumn("new_match_name", width="medium"),
        "new_player_name": st.column_config.TextColumn("new_player_name", width="small"),
        "new_opp_name": st.column_config.TextColumn("new_opp_name", width="small"),
    }

    st.divider()
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    render_panel_header(
        "Review And Update Match And Player Names",
        "Use one row per match to select only the records you want to clean up, then save, preview, or execute those changes back to CIZR.",
    )

    if disabled:
        st.info("Wait for the current download to finish before reviewing or updating match names.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    review_rows = load_match_review_state(csv_gz_path)
    if not review_rows:
        st.info(f"No reviewable matches found yet. Download an export first so `{csv_gz_path}` exists.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    review_summary_col, source_col = st.columns([1, 2])
    with review_summary_col:
        st.metric("Reviewable matches", f"{len(review_rows):,}")
    with source_col:
        st.caption(f"Source export: {csv_gz_path}")

    with st.expander("Browse one row per match", expanded=True):
        st.dataframe(
            review_rows,
            use_container_width=True,
            hide_index=True,
            column_config=review_column_config,
            height=420,
        )

    match_labels = {
        row["match_id"]: f"{row['date']} | {row['current_match_name']} | {row['match_id']}"
        for row in review_rows
    }
    selected_match_ids = st.multiselect(
        "Choose matches to rename",
        options=[row["match_id"] for row in review_rows],
        format_func=lambda match_id: match_labels[match_id],
    )

    if selected_match_ids:
        st.caption("Edit only the selected matches below.")
        for row in review_rows:
            if row["match_id"] not in selected_match_ids:
                continue
            st.markdown(
                f"""
                <div class="match-editor">
                    <div class="match-editor-title">{row['date']} | {row['match_id']}</div>
                    <div class="match-editor-copy">{row['current_match_name']}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            name_col, player_col, opp_col = st.columns(3)
            with name_col:
                st.text_input(
                    f"New match name for {row['match_id']}",
                    value=row["current_match_name"],
                    key=f"rename_{row['match_id']}",
                    help=f"Current name: {row['current_match_name']}",
                )
            with player_col:
                st.text_input(
                    f"New player name for {row['match_id']}",
                    value=row["current_player_name"],
                    key=f"player_{row['match_id']}",
                    help=f"Current player: {row['current_player_name']}",
                )
            with opp_col:
                st.text_input(
                    f"New opponent name for {row['match_id']}",
                    value=row["current_opp_name"],
                    key=f"opp_{row['match_id']}",
                    help=f"Current opponent: {row['current_opp_name']}",
                )

    proposal_rows = build_selected_match_name_rows(review_rows, selected_match_ids)
    st.metric("Selected changes ready", f"{len(proposal_rows):,}")
    if proposal_rows:
        with st.expander("Preview selected changes", expanded=True):
            st.dataframe(
                proposal_rows,
                use_container_width=True,
                hide_index=True,
                column_config=proposal_column_config,
                height=min(420, 80 + 35 * len(proposal_rows)),
            )

    save_col, dry_run_col, execute_col = st.columns(3)
    with save_col:
        if st.button("Save proposal CSV", disabled=not proposal_rows):
            proposal_path = save_match_name_proposals(proposal_rows)
            st.success(f"Saved {len(proposal_rows)} proposal row(s) to {proposal_path}.")

    with dry_run_col:
        if st.button("Dry run renames", type="secondary", disabled=not proposal_rows):
            proposal_path = save_match_name_proposals(proposal_rows)
            with st.spinner("Running match-name dry run..."):
                exit_code = apply_match_name_updates(get_token(), ["--csv", str(proposal_path), "--all"])
            if exit_code == 0:
                st.success(f"Dry run complete. Audit written to {MATCH_NAME_DRY_RUN_RESULTS}.")
            else:
                st.warning(f"Dry run finished with exit code {exit_code}. Check {MATCH_NAME_DRY_RUN_RESULTS}.")

    execute_enabled = st.checkbox("I understand execute will update CIZR metadata for the selected matches.")
    with execute_col:
        if st.button(
            "Execute renames",
            type="primary",
            disabled=not proposal_rows or not execute_enabled,
            key="execute_match_name_updates",
        ):
            proposal_path = save_match_name_proposals(proposal_rows)
            with st.spinner("Applying selected match-name updates..."):
                exit_code = apply_match_name_updates(
                    get_token(),
                    ["--csv", str(proposal_path), "--all", "--execute"],
                )
            if exit_code == 0:
                st.session_state["flash_success_message"] = (
                    f"Execute complete. Audit written to {MATCH_NAME_EXECUTE_RESULTS}."
                )
                reset_match_name_review_ui()
                st.rerun()
            else:
                st.warning(f"Execute finished with exit code {exit_code}. Check {MATCH_NAME_EXECUTE_RESULTS}.")

    latest_audit_path = MATCH_NAME_EXECUTE_RESULTS if MATCH_NAME_EXECUTE_RESULTS.exists() else MATCH_NAME_DRY_RUN_RESULTS
    if latest_audit_path.exists():
        audit_rows = read_csv_rows(latest_audit_path)
        st.caption(f"Latest audit: {latest_audit_path}")
        if audit_rows:
            st.dataframe(audit_rows, use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)


def worker_download(
    output_path: Path,
    raw_output_path: Path,
    cancel_event: threading.Event,
    updates: queue.Queue[dict[str, Any]],
) -> None:
    def progress_callback(downloaded: int, total: int | None) -> None:
        updates.put({"type": "progress", "downloaded": downloaded, "total": total})

    def status_callback(message: str) -> None:
        updates.put({"type": "status", "message": message})

    try:
        summary = retrieve_singles(
            output_path=output_path,
            raw_output_path=raw_output_path,
            progress_callback=progress_callback,
            status_callback=status_callback,
            cancel_token=cancel_event,
        )
    except DownloadCanceled as exc:
        updates.put({"type": "canceled", "message": str(exc)})
    except Exception as exc:
        updates.put({"type": "error", "message": str(exc)})
    else:
        updates.put({"type": "complete", "summary": summary})


def start_download(output_path: Path, raw_output_path: Path) -> None:
    state = ensure_download_state()
    updates: queue.Queue[dict[str, Any]] = queue.Queue()
    cancel_event = threading.Event()
    thread = threading.Thread(
        target=worker_download,
        args=(output_path, raw_output_path, cancel_event, updates),
        daemon=True,
    )

    state.clear()
    state.update(default_download_state())
    state.update(
        {
            "running": True,
            "progress_text": "Starting download...",
            "messages": ["Starting download..."],
            "thread": thread,
            "cancel_event": cancel_event,
            "queue": updates,
        }
    )
    thread.start()


def poll_download_state() -> dict[str, Any]:
    state = ensure_download_state()
    updates = state.get("queue")
    if not updates:
        return state

    while True:
        try:
            event = updates.get_nowait()
        except queue.Empty:
            break

        event_type = event.get("type")
        if event_type == "progress":
            downloaded = int(event["downloaded"])
            total = event["total"]
            if total:
                state["progress"] = min(downloaded / int(total), 1.0)
                state["progress_text"] = f"Downloading {downloaded:,} of {int(total):,} bytes"
            else:
                state["progress"] = 0.0
                state["progress_text"] = f"Downloading {downloaded:,} bytes"
        elif event_type == "status":
            message = str(event["message"])
            state["progress_text"] = message
            state["messages"].append(message)
        elif event_type == "complete":
            state["running"] = False
            state["progress"] = 1.0
            state["progress_text"] = "Download complete."
            state["summary"] = event["summary"]
            state["messages"].append("Download complete.")
        elif event_type == "canceled":
            state["running"] = False
            state["canceled"] = True
            state["progress_text"] = "Download canceled."
            state["messages"].append(str(event["message"]))
        elif event_type == "error":
            state["running"] = False
            state["error"] = str(event["message"])
            state["progress_text"] = "Download failed."
            state["messages"].append(f"Download failed: {event['message']}")

    thread = state.get("thread")
    if state["running"] and thread and not thread.is_alive():
        state["running"] = False
    return state


def render_download_status(state: dict[str, Any]) -> None:
    import streamlit as st

    if state["running"] or state["summary"] or state["error"] or state["canceled"]:
        st.progress(float(state["progress"]), text=str(state["progress_text"]))

    if state["running"]:
        if st.button("Cancel download", type="secondary"):
            cancel_event = state.get("cancel_event")
            if cancel_event:
                cancel_event.set()
            state["cancel_requested"] = True
            state["progress_text"] = "Cancel requested. Stopping after the current chunk..."
            state["messages"].append("Cancel requested.")

    if state["cancel_requested"] and state["running"]:
        st.info("Cancel requested. The export will stop before writing the final gzip.")
    elif state["canceled"]:
        st.warning("Download canceled. No new gzip export was written.")
    elif state["error"]:
        st.error(state["error"])
    elif state["summary"]:
        summary = state["summary"]
        st.success("Export ready.")
        metric_columns = st.columns(3)
        with metric_columns[0]:
            st.metric("Rows", f"{summary['rows']:,}")
        with metric_columns[1]:
            if "unique_matches" in summary:
                st.metric("Unique matches", f"{summary['unique_matches']:,}")
            else:
                st.metric("Unique matches", "n/a")
        with metric_columns[2]:
            st.metric("Content type", str(summary["content_type"]))
        if "first_date" in summary and "last_date" in summary:
            st.caption(f"Date range: {summary['first_date']} through {summary['last_date']}")

    if state["messages"]:
        with st.expander("Activity", expanded=state["running"]):
            for message in state["messages"][-12:]:
                st.write(message)


def render_download_tools(output_path: Path, running: bool, state: dict[str, Any]) -> None:
    import streamlit as st

    st.markdown('<div class="panel">', unsafe_allow_html=True)
    render_panel_header(
        "Download Singles Export",
        "Pull the latest singles match stats from CIZR, save them in this session, and continue into review or download.",
    )

    output_name = st.text_input("Filename", value=output_path.name.removesuffix(".csv.gz"), disabled=running)
    download_filename = clean_csv_gz_filename(output_name)
    selected_output_path = OUTPUT_DIR / download_filename
    selected_raw_output_path = selected_output_path.with_suffix("").with_suffix(".json")

    action_col, details_col = st.columns([1.2, 0.8])
    with action_col:
        if st.button("Retrieve singles data", type="primary", disabled=running):
            start_download(selected_output_path, selected_raw_output_path)
            st.rerun()
    with details_col:
        st.markdown(
            f"""
            <div class="info-card">
                <div class="info-kicker">Session output</div>
                <p><strong>Gzip CSV</strong><br>{selected_output_path}</p>
                <p><strong>Raw response</strong><br>{selected_raw_output_path}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    render_download_status(state)
    st.markdown("</div>", unsafe_allow_html=True)


def render_post_download_choice(state: dict[str, Any]) -> None:
    import streamlit as st

    summary = state.get("summary")
    if not summary:
        return

    csv_output = Path(summary["csv_output"])
    st.divider()
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    render_panel_header(
        "What Would You Like To Do Next?",
        "Either download the fresh export to your computer or move straight into match-name cleanup.",
    )
    selected_action = st.radio(
        "After this session download",
        options=("Download to my PC", "Edit match names"),
        horizontal=True,
        key="post_download_action",
    )

    if selected_action == "Download to my PC":
        st.write("Save the freshly downloaded export from this session to your computer.")
        if csv_output.exists():
            st.download_button(
                "Download CSV gzip",
                data=csv_output.read_bytes(),
                file_name=csv_output.name,
                mime="application/gzip",
                type="primary",
            )
        else:
            st.warning("The export finished, but the temporary download file is no longer available.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    st.markdown("</div>", unsafe_allow_html=True)
    render_match_name_tools(csv_output, disabled=False)


def main() -> None:
    import streamlit as st

    st.set_page_config(page_title="NC State - Data Retrieval", layout="wide")
    inject_app_styles()
    state = poll_download_state()
    running = bool(state["running"])

    st.markdown(
        """
        <div class="hero-card">
            <div class="hero-eyebrow">NC State Tennis</div>
            <div class="hero-title">Singles Data Retrieval</div>
            <div class="hero-copy">
                Pull the latest singles export into this workspace, review the results, and clean up match names without leaving the session.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    render_top_status(state)
    flash_success_message = st.session_state.pop("flash_success_message", None)
    if flash_success_message:
        st.success(flash_success_message)

    default_output_path = OUTPUT_DIR / CSV_GZ_OUTPUT.name
    render_download_tools(default_output_path, running, state)
    if state.get("summary") and not running:
        render_post_download_choice(state)

    if state["running"]:
        time.sleep(0.5)
        st.rerun()


if __name__ == "__main__":
    main()
