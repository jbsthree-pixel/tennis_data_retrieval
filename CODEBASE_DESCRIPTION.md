# Codebase Description

This repository contains a small Streamlit application and companion command-line utilities for retrieving NC State Singles match statistics from the CIZR Tennis API. The main user-facing workflow is a Streamlit page that downloads the Singles match stats export, converts the response into tabular rows when needed, writes a gzipped CSV into `output/`, and exposes the finished file through a browser download button.

## Streamlit App

The Streamlit entry point is `src/streamlit_app.py`.

The app renders a simple "NC State - Data Retrieval" interface with:

- a filename input for the exported gzip CSV,
- a primary button to retrieve Singles data,
- live progress and status messages while the export is running,
- a cancel button for active downloads,
- summary metrics after completion, including row count, unique match count when available, date range when available, and response content type,
- a download button for the generated `.csv.gz` file.

Long-running retrieval work is moved into a background thread so Streamlit can keep refreshing the page and showing progress. The thread communicates with the UI through a `queue.Queue`, and shared state is stored in `st.session_state["download_state"]`. Cancellation is handled with a `threading.Event`; the download/conversion code checks that event between network chunks and while writing CSV rows.

Run the app with:

```powershell
python -m streamlit run src\streamlit_app.py
```

## Retrieval Pipeline

The reusable retrieval logic lives in `src/cizr_singles.py`.

That module is responsible for:

- building the CIZR match stats URL for the configured team and stat group,
- making the authenticated HTTP request with the bundled integration token,
- reading the response body in chunks so progress can be reported,
- saving the raw API response atomically,
- converting either CSV or JSON API responses into row dictionaries,
- flattening nested JSON objects into CSV-friendly columns,
- writing the final gzipped CSV atomically,
- returning a summary dictionary for UI and CLI callers.

The default endpoint configuration is:

- `TEAM_ID`: from `CIZR_TEAM_ID`, falling back to the NC State team id embedded in the module,
- `STAT_GROUP`: from `CIZR_STAT_GROUP`, falling back to `Singles`,
- `includeOwned=true` on the match stats endpoint.

Default outputs are:

- raw response: `output/singles.json`,
- gzip CSV export: `output/singles.csv.gz`.

## Command-Line Entry Points

`src/fetch_singles.py` is the lightweight CLI equivalent of the Streamlit workflow. It calls `retrieve_singles()`, prints the endpoint/status/content type, and reports output paths plus row/date summary information.

`src/main.py` is a larger API debugging and maintenance utility. It supports probing pagination-style query parameters, checking match-specific endpoints, comparing all-for-user match results, generating player ID tables from the reconciled workbook, proposing player ID updates, and optionally applying selected player ID updates back to CIZR metadata.

## CSV Helper Scripts

Two helper scripts prepare downloaded CSV data for spreadsheet use:

- `src/make_excel_safe_csv.py` writes an Excel-safe CSV that protects numeric-looking `matchId` values from being converted to numbers.
- `src/make_slim_excel_safe_csv.py` does the same protection while also dropping URL-heavy columns such as `matchLink`, `preprocessedUri`, and `postprocessedUri`.

These scripts expect `output/singles_converted.csv` as input and write derived CSV files back into `output/`.

## Repository Layout

```text
.
|-- README.md
|-- requirements.txt
|-- input/
|   `-- cizr_reconciled_matches.xlsx
|-- output/
|   |-- singles.csv.gz
|   |-- singles.json
|   |-- player_id_tables.xlsx
|   `-- generated API/debug CSV and JSON artifacts
`-- src/
    |-- streamlit_app.py
    |-- cizr_singles.py
    |-- fetch_singles.py
    |-- main.py
    |-- make_excel_safe_csv.py
    `-- make_slim_excel_safe_csv.py
```

## Data Flow

1. The user starts the Streamlit app and clicks "Retrieve singles data".
2. `streamlit_app.py` starts a background worker thread.
3. The worker calls `retrieve_singles()` from `cizr_singles.py`.
4. `cizr_singles.py` calls the CIZR API, streams the response body, and reports progress.
5. The raw response is written to `output/<filename>.json`.
6. The response is parsed as CSV or JSON and converted into row dictionaries.
7. Rows are written to `output/<filename>.csv.gz`.
8. The Streamlit UI displays summary metrics and offers the gzip file as a browser download.

## Dependencies

The Streamlit app only declares `streamlit` in `requirements.txt`. Some maintenance paths in `src/main.py` optionally use `pandas` and `openpyxl` for Excel workbook input/output, but those are not required for the core Streamlit download workflow.
