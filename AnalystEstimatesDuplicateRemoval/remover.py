import hashlib
import json
import re
import shutil
from collections import defaultdict
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(r"D:\Trading Research\Misc\FilesRemovals\AnalystEstimations")
BEFORE_DIR = BASE_DIR / "Before"
AFTER_DIR = BASE_DIR / "After"

# AnalystEstimates_{symbol}_{YYYY.MM.DD}_{HH.MM.SS.mmm}_{suffix}.json
FILENAME_RE = re.compile(
    r"^AnalystEstimates_(?P<symbol>.+?)_"
    r"(?P<date>\d{4}\.\d{2}\.\d{2})_"
    r"(?P<time>\d{2}\.\d{2}\.\d{2}\.\d{3})_(?P<suffix>[a-z])\.json$"
)


def parse_datetime(filename: str) -> datetime:
    m = FILENAME_RE.match(filename)
    return datetime.strptime(
        f"{m.group('date')} {m.group('time')}", "%Y.%m.%d %H.%M.%S.%f"
    )


def content_hash(data) -> str:
    """SHA-256 of the canonicalized JSON (sorted keys, stable across whitespace)."""
    canonical = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


def main() -> None:
    AFTER_DIR.mkdir(parents=True, exist_ok=True)

    # Collect all matching files
    by_symbol: dict[str, list[tuple[datetime, Path]]] = defaultdict(list)
    for path in BEFORE_DIR.iterdir():
        if path.suffix.lower() != ".json":
            continue
        m = FILENAME_RE.match(path.name)
        if not m:
            print(f"[skip – unrecognized name] {path.name}")
            continue
        key = f"{m.group('symbol')}_{m.group('suffix')}"
        by_symbol[key].append((parse_datetime(path.name), path))

    if not by_symbol:
        print("No matching JSON files found in Before directory.")
        return

    copied = skipped = 0

    for symbol, entries in sorted(by_symbol.items()):
        # Earliest first
        entries.sort(key=lambda x: x[0])

        seen_hashes: set[str] = set()

        for dt, path in entries:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception as exc:
                print(f"[error – cannot read] {path.name}: {exc}")
                continue

            h = content_hash(data)

            if h not in seen_hashes:
                seen_hashes.add(h)
                shutil.copy2(path, AFTER_DIR / path.name)
                print(f"[copied]  {path.name}")
                copied += 1
            else:
                print(f"[skipped] {path.name}  (duplicate of an earlier file)")
                skipped += 1

    print(f"\nDone — copied: {copied}, skipped (duplicates): {skipped}")


if __name__ == "__main__":
    main()
