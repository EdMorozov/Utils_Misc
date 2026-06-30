from __future__ import annotations

import argparse
import re
import shutil
from dataclasses import dataclass
from pathlib import Path


DEFAULT_SOURCE = Path(r"D:\Trading Research\Data\Tests\TestCopy")
DEFAULT_DESTINATION = Path(
    r"D:\Trading Research\Repos TR\Python\TR_Utilities\Misc\FileRenamer\Results"
)

TRANSCRIPT_NAME = re.compile(
    r"^EarningsCallTranscript_(?P<year>\d{4})_(?P<tail>.+)$"
)


@dataclass
class RenameStats:
    copied: int = 0
    skipped_existing: int = 0
    skipped_unmatched: int = 0
    conflicts: int = 0
    dry_run: int = 0


def renamed_file_name(source_file: Path) -> str | None:
    match = TRANSCRIPT_NAME.match(source_file.name)
    if not match:
        return None

    ticker = source_file.parent.name
    year = match.group("year")
    tail = match.group("tail")
    return f"EarningsCallTranscript_{ticker}_{year}_{tail}"


def copy_renamed_files(
    source: Path,
    destination: Path,
    *,
    dry_run: bool = False,
    overwrite: bool = False,
    verbose: bool = False,
) -> RenameStats:
    if not source.exists():
        raise FileNotFoundError(f"Source does not exist: {source}")
    if not source.is_dir():
        raise NotADirectoryError(f"Source is not a directory: {source}")

    stats = RenameStats()

    for source_file in source.rglob("*"):
        if not source_file.is_file():
            continue

        new_name = renamed_file_name(source_file)
        if new_name is None:
            stats.skipped_unmatched += 1
            continue

        relative_parent = source_file.parent.relative_to(source)
        target_file = destination / relative_parent / new_name

        if target_file.exists() and not overwrite:
            if target_file.stat().st_size == source_file.stat().st_size:
                stats.skipped_existing += 1
                continue
            stats.conflicts += 1
            print(f"Conflict, not overwritten: {target_file}")
            continue

        if dry_run:
            stats.dry_run += 1
            if verbose:
                print(f"{source_file} -> {target_file}")
            continue

        target_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_file, target_file)
        stats.copied += 1

        if stats.copied % 1000 == 0:
            print(f"Copied {stats.copied} files...")

    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Copy earnings call transcript files into a destination folder, "
            "preserving the source directory structure and inserting the ticker "
            "from each file's containing directory into the filename."
        )
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=DEFAULT_SOURCE,
        help=f"Source root. Default: {DEFAULT_SOURCE}",
    )
    parser.add_argument(
        "--destination",
        type=Path,
        default=DEFAULT_DESTINATION,
        help=f"Destination root. Default: {DEFAULT_DESTINATION}",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned file copies without writing anything.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing destination files.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print each file path as it is processed.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    stats = copy_renamed_files(
        args.source,
        args.destination,
        dry_run=args.dry_run,
        overwrite=args.overwrite,
        verbose=args.verbose,
    )

    print(
        "Done. "
        f"copied={stats.copied}, "
        f"dry_run={stats.dry_run}, "
        f"skipped_existing={stats.skipped_existing}, "
        f"skipped_unmatched={stats.skipped_unmatched}, "
        f"conflicts={stats.conflicts}"
    )

    return 1 if stats.conflicts else 0


if __name__ == "__main__":
    raise SystemExit(main())
