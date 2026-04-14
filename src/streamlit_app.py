from __future__ import annotations

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
        csv_output = Path(summary["csv_output"])
        st.success("Export ready.")
        st.metric("Rows", f"{summary['rows']:,}")
        if "unique_matches" in summary:
            st.metric("Unique matches", f"{summary['unique_matches']:,}")
        if "first_date" in summary and "last_date" in summary:
            st.write(f"Date range: {summary['first_date']} through {summary['last_date']}")
        st.write(f"Content type: {summary['content_type']}")
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

    if state["messages"]:
        with st.expander("Activity", expanded=state["running"]):
            for message in state["messages"][-12:]:
                st.write(message)


def main() -> None:
    import streamlit as st

    st.set_page_config(page_title="NC State - Data Retrieval", layout="centered")
    state = poll_download_state()
    running = bool(state["running"])

    st.title("NC State - Data Retrieval")
    st.write("Download the Singles match stats export and store it as a gzipped CSV.")

    output_name = st.text_input("Filename", value=CSV_GZ_OUTPUT.name.removesuffix(".csv.gz"), disabled=running)
    download_filename = clean_csv_gz_filename(output_name)
    output_path = OUTPUT_DIR / download_filename
    raw_output_path = output_path.with_suffix("").with_suffix(".json")
    st.caption("After retrieval, use the download button to save the gzip CSV to your machine.")

    if st.button("Retrieve singles data", type="primary", disabled=running):
        start_download(output_path, raw_output_path)
        st.rerun()

    render_download_status(state)

    if state["running"]:
        time.sleep(0.5)
        st.rerun()


if __name__ == "__main__":
    main()
