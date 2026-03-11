"""
Currency Checker
================
Scans FMP Financial API JSON data files to determine which currency each ticker
uses across CompanyInfo, DailyFull, FundBalanceSheet, FundCashflow, and
FundIncomeStatement data folders.

Output: a CSV file in the Results/ folder with:
  - A currencies summary section (all currencies found, per-folder breakdown)
  - A per-ticker table showing the currency in each data folder
"""

import os
import json
import glob
import csv
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_ROOT = r"D:\Trading Research\Data\RawDataStoragePy"

FOLDERS = [
    "CompanyInfo",
    "FundBalanceSheet",
    "FundCashflow",
    "FundIncomeStatement",
]

# Ordered list of JSON field names to probe for currency, per folder type.
# FMP company profile and historical prices use "currency";
# FMP financial statements use "reportedCurrency".
CURRENCY_FIELD_PRIORITY = {
    "CompanyInfo":        ["currency", "reportedCurrency"],
    "FundBalanceSheet":   ["reportedCurrency", "currency"],
    "FundCashflow":       ["reportedCurrency", "currency"],
    "FundIncomeStatement":["reportedCurrency", "currency"],
}

# Actual file name prefix for each folder (defaults to folder name if absent).
FILE_PREFIXES = {
    "FundIncomeStatement": "FundIncome",
}

RESULTS_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Results")

# Sentinel values (never written as real currency codes in output)
NA          = "N/A"           # no file found for ticker
UNKNOWN     = "UNKNOWN"       # file exists but no currency field found
FILE_ERROR  = "FILE_ERROR"    # could not read / parse the file
FOLDER_MISS = "FOLDER_MISSING"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ticker_from_filename(filename: str) -> str | None:
    """
    Extract ticker from naming convention:
        FolderName_TICKER_YYYY.MM.DD_HH.MM.SS.mmm.json
    Returns None if the name does not match.
    """
    stem = Path(filename).stem          # strip .json
    parts = stem.split("_")
    return parts[1] if len(parts) >= 2 else None


def datetime_from_filename(filename: str) -> datetime:
    """
    Parse the embedded datetime from the filename so we can pick the most
    recent file when multiple exist for the same ticker.
    Falls back to datetime.min on any parse failure.
    """
    stem = Path(filename).stem
    parts = stem.split("_")
    # expected: [FolderType, TICKER, YYYY.MM.DD, HH.MM.SS.mmm]
    if len(parts) >= 4:
        date_part = parts[2]            # e.g. "2026.02.18"
        time_part = parts[3]            # e.g. "08.58.30.443"
        time_segments = time_part.split(".")
        if len(time_segments) >= 3:
            dt_str = f"{date_part}_{time_segments[0]}.{time_segments[1]}.{time_segments[2]}"
            try:
                return datetime.strptime(dt_str, "%Y.%m.%d_%H.%M.%S")
            except ValueError:
                pass
    return datetime.min


def most_recent_file(file_paths: list[str]) -> str:
    """Return the path of the most recently dated file."""
    return max(file_paths, key=lambda p: datetime_from_filename(os.path.basename(p)))


def _probe_dict(record: dict, fields: list[str]) -> str | None:
    """Return the first non-empty value found for any field name in *fields*."""
    for field in fields:
        value = record.get(field)
        if value is not None and str(value).strip():
            return str(value).strip().upper()
    return None


def extract_currency(filepath: str, folder_name: str) -> str:
    """
    Open *filepath*, walk the JSON structure, and return the currency string.
    Handles the main FMP response shapes:
      - list of dicts  → inspect first element
      - dict           → inspect top-level, then first list value found
    """
    fields = CURRENCY_FIELD_PRIORITY.get(folder_name, ["currency", "reportedCurrency"])
    try:
        with open(filepath, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return FILE_ERROR

    # ---- list response (e.g. company profile, financial statements) --------
    if isinstance(data, list):
        if not data:
            return UNKNOWN
        first = data[0]
        if isinstance(first, dict):
            result = _probe_dict(first, fields)
            return result if result else UNKNOWN
        return UNKNOWN

    # ---- dict response (e.g. DailyFull historical prices wrapper) ----------
    if isinstance(data, dict):
        # try top-level fields first
        result = _probe_dict(data, fields)
        if result:
            return result

        # try the first item inside any list value (nested structure)
        for value in data.values():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                result = _probe_dict(value[0], fields)
                if result:
                    return result

        return UNKNOWN

    return UNKNOWN


def index_folder(folder_path: str, folder_name: str) -> dict[str, list[str]]:
    """
    Scan *folder_path* once and return a mapping  ticker -> [file_paths]
    for every JSON file matching the expected naming convention.
    """
    index: dict[str, list[str]] = {}
    file_prefix = FILE_PREFIXES.get(folder_name, folder_name)
    prefix = file_prefix + "_"
    try:
        with os.scandir(folder_path) as it:
            for entry in it:
                try:
                    name = entry.name
                except OSError:
                    continue
                if not name.endswith(".json") or not name.startswith(prefix):
                    continue
                t = ticker_from_filename(name)
                if t:
                    index.setdefault(t, []).append(
                        os.path.join(folder_path, name)
                    )
    except OSError as exc:
        print(f"  [WARN] Could not scan {folder_path}: {exc}")
    return index


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

MAX_RESULT_FILES = 5


def cleanup_results(folder: str, keep: int = MAX_RESULT_FILES) -> None:
    """Delete oldest CSV files in *folder*, keeping only the *keep* most recent."""
    csv_files = sorted(
        (os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".csv")),
        key=os.path.getmtime,
    )
    to_delete = csv_files[: max(0, len(csv_files) - keep)]
    for path in to_delete:
        try:
            os.remove(path)
            print(f"  Removed old result: {os.path.basename(path)}")
        except OSError as exc:
            print(f"  [WARN] Could not delete {path}: {exc}")


def main() -> None:
    os.makedirs(RESULTS_FOLDER, exist_ok=True)
    cleanup_results(RESULTS_FOLDER)

    # ------------------------------------------------------------------
    # Step 1 – collect tickers from the CompanyInfo folder
    # ------------------------------------------------------------------
    company_info_path = os.path.join(BASE_ROOT, "CompanyInfo")
    if not os.path.isdir(company_info_path):
        print(f"[ERROR] CompanyInfo folder not found: {company_info_path}")
        return

    print(f"Scanning CompanyInfo folder …\n  {company_info_path}")
    all_ci_files = glob.glob(os.path.join(company_info_path, "CompanyInfo_*.json"))

    tickers: set[str] = set()
    for filepath in all_ci_files:
        t = ticker_from_filename(os.path.basename(filepath))
        if t:
            tickers.add(t)

    tickers_sorted = sorted(tickers)
    print(f"  Found {len(tickers_sorted)} ticker(s).")
    if not tickers_sorted:
        print("No tickers found – nothing to do.")
        return

    # ------------------------------------------------------------------
    # Step 2 – scan each folder once and build ticker → files index
    # ------------------------------------------------------------------
    folder_index: dict[str, dict[str, list[str]]] = {}
    for folder_name in FOLDERS:
        folder_path = os.path.join(BASE_ROOT, folder_name)
        if not os.path.isdir(folder_path):
            folder_index[folder_name] = {}
            print(f"  [WARN] Folder not found: {folder_path}")
        else:
            print(f"Scanning {folder_name} folder …")
            folder_index[folder_name] = index_folder(folder_path, folder_name)

    # ------------------------------------------------------------------
    # Step 3 – for every ticker, look up currency from each folder index
    # ------------------------------------------------------------------
    results: dict[str, dict[str, str]] = {}
    total = len(tickers_sorted)
    bar_width = 40

    for idx, ticker in enumerate(tickers_sorted, start=1):
        results[ticker] = {}
        for folder_name in FOLDERS:
            folder_path = os.path.join(BASE_ROOT, folder_name)
            if not os.path.isdir(folder_path):
                results[ticker][folder_name] = FOLDER_MISS
                continue

            matched = folder_index[folder_name].get(ticker, [])
            if not matched:
                results[ticker][folder_name] = NA
                continue

            recent = most_recent_file(matched)
            currency = extract_currency(recent, folder_name)
            results[ticker][folder_name] = currency

        pct = idx / total
        filled = int(bar_width * pct)
        bar = "#" * filled + "-" * (bar_width - filled)
        print(f"\r  [{bar}] {pct:5.1%}  {idx}/{total}", end="", flush=True)

    print()  # newline after progress bar

    # ------------------------------------------------------------------
    # Step 4 – build currency summary
    # ------------------------------------------------------------------
    SENTINELS = {NA, UNKNOWN, FILE_ERROR, FOLDER_MISS}

    all_currencies: set[str] = set()
    for ticker_data in results.values():
        for c in ticker_data.values():
            if c not in SENTINELS:
                all_currencies.add(c)
    all_currencies_sorted = sorted(all_currencies)

    per_folder_currencies: dict[str, list[str]] = {}
    for folder_name in FOLDERS:
        folder_set: set[str] = set()
        for ticker_data in results.values():
            c = ticker_data.get(folder_name, NA)
            if c not in SENTINELS:
                folder_set.add(c)
        per_folder_currencies[folder_name] = sorted(folder_set)

    # ------------------------------------------------------------------
    # Step 5 – write CSV
    # ------------------------------------------------------------------
    timestamp = datetime.now().strftime("%Y.%m.%d_%H.%M.%S")
    output_path = os.path.join(RESULTS_FOLDER, f"CurrencyCheck_{timestamp}.csv")

    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)

        # ---- currency summary section -----------------------------------
        writer.writerow(["=== CURRENCIES SUMMARY ==="])
        writer.writerow([])
        writer.writerow(["All currencies found across all folders:"])
        writer.writerow(all_currencies_sorted)
        writer.writerow([])

        writer.writerow(["Currencies per folder:"])
        writer.writerow(["Folder", "Currencies"])
        for folder_name in FOLDERS:
            writer.writerow([folder_name] + per_folder_currencies[folder_name])
        writer.writerow([])
        writer.writerow([])

        # ---- split tickers into two groups --------------------------------
        non_usd = [t for t in tickers_sorted
                   if any(results[t].get(fn, NA) not in SENTINELS | {"USD"}
                          for fn in FOLDERS)]
        all_usd = [t for t in tickers_sorted if t not in non_usd]

        # ---- table 1: tickers with at least one non-USD currency --------
        writer.writerow(["=== TABLE 1: TICKERS WITH NON-USD CURRENCIES ==="])
        writer.writerow([f"({len(non_usd)} tickers)"])
        writer.writerow([])
        writer.writerow(["#", "Ticker"] + FOLDERS)
        for i, ticker in enumerate(non_usd, start=1):
            row = [i, ticker] + [results[ticker].get(fn, NA) for fn in FOLDERS]
            writer.writerow(row)
        writer.writerow([])
        writer.writerow([])

        # ---- table 2: tickers that are USD everywhere -------------------
        writer.writerow(["=== TABLE 2: TICKERS WITH USD IN ALL FOLDERS ==="])
        writer.writerow([f"({len(all_usd)} tickers)"])
        writer.writerow([])
        writer.writerow(["#", "Ticker"] + FOLDERS)
        for i, ticker in enumerate(all_usd, start=1):
            row = [i, ticker] + [results[ticker].get(fn, NA) for fn in FOLDERS]
            writer.writerow(row)

    # ------------------------------------------------------------------
    # Step 6 – print formatted tables to CLI
    # ------------------------------------------------------------------
    SENTINELS_SET = {NA, UNKNOWN, FILE_ERROR, FOLDER_MISS}
    non_usd = [t for t in tickers_sorted
               if any(results[t].get(fn, NA) not in SENTINELS_SET | {"USD"}
                      for fn in FOLDERS)]
    all_usd = [t for t in tickers_sorted if t not in non_usd]

    short_headers = ["CompanyInfo", "FundBS", "FundCF", "FundIS"]
    col_headers = ["#", "Ticker"] + short_headers

    def calc_widths(ticker_list):
        num_w = len(str(len(ticker_list)))
        widths = [max(len("#"), num_w)] + [len(h) for h in ["Ticker"] + short_headers]
        for ticker in ticker_list:
            widths[1] = max(widths[1], len(ticker))
            for i, fn in enumerate(FOLDERS, start=2):
                widths[i] = max(widths[i], len(results[ticker].get(fn, NA)))
        return widths

    def fmt_row(cells, widths):
        return "  ".join(c.ljust(widths[i]) for i, c in enumerate(cells))

    def print_table(title, ticker_list):
        widths = calc_widths(ticker_list)
        sep = "  ".join("-" * w for w in widths)
        print(f"\n{title}  ({len(ticker_list)} tickers)")
        print(fmt_row(col_headers, widths))
        print(sep)
        for i, ticker in enumerate(ticker_list, start=1):
            row = [str(i), ticker] + [results[ticker].get(fn, NA) for fn in FOLDERS]
            print(fmt_row(row, widths))

    print(f"\nCurrencies found : {', '.join(all_currencies_sorted) if all_currencies_sorted else '(none)'}")
    print_table("TABLE 1 – Non-USD currencies", non_usd)
    print_table("TABLE 2 – USD everywhere", all_usd)
    print(f"\nResults written to:\n  {output_path}")


if __name__ == "__main__":
    main()
