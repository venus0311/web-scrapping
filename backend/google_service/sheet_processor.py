import time
import random
import atexit
import threading
from collections import defaultdict
from typing import Dict, List

from gspread.exceptions import APIError
import gspread


# CONFIG
BATCH_SIZE = 10
MAX_RETRIES = 5
RETRY_BASE_SECONDS = 1.0

# State (per-process)
# batch_buffers: Dict[str, List[List]] = defaultdict(list)   # e.g. batch_buffers['result'] = [[...], ...]
# batch_headers: Dict[str, List[str]] = {}                   # e.g. batch_headers['result'] = ['col1','col2',...]
# buffer_locks: Dict[str, threading.Lock] = defaultdict(threading.Lock)

# Global buffers & locks
batch_buffers: Dict[str, List[List]] = defaultdict(list)
batch_headers: Dict[str, List[str]] = {}
buffer_locks: Dict[str, threading.Lock] = defaultdict(threading.Lock)


def _make_key(sheet_url: str, tab_name: str) -> str:
    return f"{sheet_url}::{tab_name}"


def _exponential_backoff_sleep(attempt: int):
    """Sleep for exponential backoff with jitter."""
    time.sleep(RETRY_BASE_SECONDS * (2 ** attempt) + random.random())


def _ensure_worksheet_and_header(sheet, tab_name: str, header: List[str]):
    """
    Ensure that a worksheet with `tab_name` exists and has `header` as row 1.
    Returns the worksheet object.
    Retries a few times to handle concurrent creation.
    """
    last_exc = None
    for attempt in range(3):
        try:
            try:
                worksheet = sheet.worksheet(tab_name)
            except Exception:
                # not exists -> create with a reasonable number of cols
                cols = max(len(header), 5)
                worksheet = sheet.add_worksheet(title=tab_name, rows="1000", cols=str(cols))

            # Check header (row 1); if missing or shorter than our header, update it.
            try:
                current_header = worksheet.row_values(1)
            except Exception:
                # some transient error reading header; try again
                current_header = []

            if not current_header or len(current_header) < len(header):
                # write header into A1 (gspread will expand columns)
                worksheet.update("A1", [header])
            return worksheet
        except Exception as e:
            last_exc = e
            # small sleep and retry to handle race conditions between processes creating the sheet
            time.sleep(0.5 + random.random())
    # if we got here, re-raise the last exception
    raise last_exc


def _append_rows_with_retries(worksheet, rows: List[List]):
    """Append rows to worksheet with exponential-backoff retries on transient errors."""
    if not rows:
        return
    for attempt in range(MAX_RETRIES):
        try:
            # append_rows appends a list of rows in a single API request
            # NOTE: signature: worksheet.append_rows(rows, value_input_option='RAW', insert_data_option='INSERT_ROWS')
            worksheet.append_rows(rows, value_input_option='RAW', insert_data_option='INSERT_ROWS')
            return
        except APIError as e:
            # common transient: quota / 429 or server 5xx
            # we retry for most APIError occurrences
            if attempt < MAX_RETRIES - 1:
                _exponential_backoff_sleep(attempt)
                continue
            else:
                raise
        except Exception:
            # network / unexpected error - also retry a few times
            if attempt < MAX_RETRIES - 1:
                _exponential_backoff_sleep(attempt)
                continue
            else:
                raise


def write_results_in_tab(
    sheet,                       
    suitable_results,            
    unsuitable_results,          
    mode: str,                   
    data=None,                   
    batch_size: int = BATCH_SIZE 
):
    """
    Backwards-compatible: call like
      write_results_in_tab(sheet, suitable_results, unsuitable_results, "unsuitable", unsuitable_data)

    Internally:
      - determines tab_name ("result" for "suitable", else "unsuitable")
      - derives header from `data` (if dict) or infers it from existing items
      - stores row as a list of values
      - buffers and flushes in batches using composite key (sheet.url::tab_name)
    """

    # decide tab name (keeps your original mapping)
    tab_name = "result" if mode == "suitable" else "unsuitable"
    key = _make_key(sheet.url, tab_name)

    # --- derive header and row robustly ---
    header = None
    row = None

    if isinstance(data, dict):
        header = list(data.keys())
        row = list(data.values())
    elif isinstance(data, list):
        # data may be a row (list of values) or list-of-dicts (rare)
        if data and isinstance(data[0], dict):
            header = list(data[0].keys())
            row = list(data[0].values())
        else:
            row = data
            # try to infer header from previously seen structures
            if key in batch_headers:
                header = batch_headers[key]
            else:
                inferred = None
                # try infer from suitable_results / unsuitable_results if they contain dicts
                for candidate in (suitable_results, unsuitable_results):
                    if isinstance(candidate, list) and candidate:
                        first = candidate[0]
                        if isinstance(first, dict):
                            inferred = list(first.keys())
                            break
                        elif isinstance(first, list):
                            inferred = [f"col{i+1}" for i in range(len(first))]
                            break
                header = inferred or [f"col{i+1}" for i in range(len(row or []))]
    else:
        # fallback: try to treat data like a dict, else stringify
        try:
            header = list(data.keys())
            row = list(data.values())
        except Exception:
            header = batch_headers.get(key, [])
            row = [str(data)]

    # --- append to buffer (thread-safe) ---
    with buffer_locks[key]:
        # ensure header saved for this sheet/tab
        if key not in batch_headers and header is not None:
            batch_headers[key] = header

        # ensure buffer is always a list
        if not isinstance(batch_buffers.get(key), list):
            # print(f"[WARN] Buffer for {key} was {type(batch_buffers.get(key))}, resetting to []")
            batch_buffers[key] = []

        # append row (make sure it's a list)
        if not isinstance(row, list):
            row = [row]
        batch_buffers[key].append(row)

        # capture and clear if full (flush outside lock)
        rows_to_write = None
        if len(batch_buffers[key]) >= batch_size:
            rows_to_write = batch_buffers[key][:]
            batch_buffers[key].clear()

    # --- flush outside the lock if needed ---
    if rows_to_write:
        # ensure worksheet exists and header is present
        worksheet = _ensure_worksheet_and_header(sheet, tab_name, batch_headers.get(key, header or []))

        target_len = len(batch_headers.get(key, []))
        padded = []
        for r in rows_to_write:
            if len(r) < target_len:
                padded.append(r + [""] * (target_len - len(r)))
            else:
                padded.append(r)

        _append_rows_with_retries(worksheet, padded)


def flush_buffer(sheet: gspread.Spreadsheet, tab_name: str):
    key = _make_key(sheet.url, tab_name)

    with buffer_locks[key]:
        buffer = batch_buffers.get(key, [])
        if not isinstance(buffer, list):
            # print(f"[WARN] Buffer for {key} was {type(buffer)}, resetting to []")
            buffer = []
            batch_buffers[key] = []

        if not buffer:
            return  # nothing to flush

        # Get or create the worksheet
        try:
            worksheet = sheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(
                title=tab_name,
                rows=str(len(buffer) + 1),
                cols=str(len(buffer[0])),
            )
            # Write header row if we have one
            if key in batch_headers:
                worksheet.append_row(batch_headers[key])

        # Write buffered rows
        worksheet.append_rows(buffer)

        # Clear buffer
        batch_buffers[key] = []


# -------------------------
# Flush all buffers for a sheet
# -------------------------
def flush_all_buffers(sheet: gspread.Spreadsheet):
    """
    Flushes all tab buffers belonging to this sheet only.
    """
    for key in list(batch_buffers.keys()):
        if key.startswith(sheet.url + "::"):
            _, tab_name = key.split("::", 1)
            flush_buffer(sheet, tab_name)


def register_flush_on_exit(sheet):
    """
    Register an atexit handler that will flush buffers on process exit.
    Call this once (after having `sheet`).
    """
    atexit.register(lambda: flush_all_buffers(sheet))





