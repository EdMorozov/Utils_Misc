# FileRenamer

Copies earnings call transcript files into a destination folder, preserves the
original directory structure, and inserts the ticker from the file's parent
directory into the filename.

## What it changes

Input file:

```text
D:\Trading Research\Data\Tests\TestCopy\EarningsCalls\ADBE\EarningsCallTranscript_2011_2025.12.11_14.27.38.973.json
```

Output file:

```text
D:\Trading Research\Repos TR\Python\TR_Utilities\Misc\FileRenamer\Results\EarningsCalls\ADBE\EarningsCallTranscript_ADBE_2011_2025.12.11_14.27.38.973.json
```

The source files are not deleted or renamed. The script copies them.

## Default paths

Source:

```text
D:\Trading Research\Data\Tests\TestCopy
```

Destination:

```text
D:\Trading Research\Repos TR\Python\TR_Utilities\Misc\FileRenamer\Results
```

## How to run it

From this folder:

```powershell
cd "D:\Trading Research\Repos TR\Python\TR_Utilities\Misc\FileRenamer"
```

Preview what the script would do without copying files:

```powershell
.\.venv\Scripts\python.exe main.py --dry-run
```

Run the actual copy:

```powershell
.\.venv\Scripts\python.exe main.py
```

Print every planned source and destination path during a preview:

```powershell
.\.venv\Scripts\python.exe main.py --dry-run --verbose
```

## Rerunning it

If destination files already exist and have the same file size, the script skips
them.

To replace existing destination files:

```powershell
.\.venv\Scripts\python.exe main.py --overwrite
```

## Use different folders

```powershell
.\.venv\Scripts\python.exe main.py --source "D:\Some\Source" --destination "D:\Some\Destination"
```

## Output summary

At the end, the script prints counts:

```text
Done. copied=21267, dry_run=0, skipped_existing=0, skipped_unmatched=0, conflicts=0
```

- `copied`: files copied during this run
- `dry_run`: files that would be copied in preview mode
- `skipped_existing`: destination files already existed with the same size
- `skipped_unmatched`: files that did not match the expected transcript filename
- `conflicts`: destination files existed with a different size and were not overwritten
