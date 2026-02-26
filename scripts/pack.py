#!/usr/bin/env python3
"""
pack.py — Convert FLAT format to PARTITIONED DVC format
========================================================

Usage
-----
  # Basic (auto-incremental batch based on existing batches, up to 10k rows/batch):
  python pack.py --source  /path/to/test_out_tiny \
                 --output  /path/to/dataset

  # Explicit max rows (limit batch size):
  python pack.py --source  /path/to/test_out_tiny \
                 --output  /path/to/dataset \
                 --max-rows 5000

  # Explicit batch label (no auto-incrementing):
  python pack.py --source  /path/to/test_out_tiny \
                 --output  /path/to/dataset \
                 --batch   batch_2026_01
"""

import argparse
import shutil
import sys
from pathlib import Path

import pandas as pd

# ── helpers ──────────────────────────────────────────────────────────────────

def sanitise(value: str) -> str:
    """Make a string safe for use as a directory name."""
    return str(value).strip().lower().replace(" ", "_").replace("/", "-")


def load_metadata(source_dir: Path) -> pd.DataFrame:
    csv_path = source_dir / "metadata.csv"
    if not csv_path.exists():
        sys.exit(f"[ERROR] metadata.csv not found in {source_dir}")
    df = pd.read_csv(csv_path)
    required = {"audio_path", "speaker", "audio_source"}
    missing = required - set(df.columns)
    if missing:
        sys.exit(f"[ERROR] metadata.csv is missing columns: {missing}")
    return df


def get_next_batch_num(output_dir: Path, audio_source: str, speaker: str) -> int:
    """Find the highest auto-incremental batch number currently existing."""
    speaker_dir = output_dir / "audio" / audio_source / speaker
    if not speaker_dir.exists():
        return 1
    
    max_num = 0
    for folder in speaker_dir.iterdir():
        if folder.is_dir() and folder.name.startswith("batch_"):
            try:
                num = int(folder.name.split("_")[-1])
                max_num = max(max_num, num)
            except ValueError:
                pass
    return max_num + 1


# ── core logic ────────────────────────────────────────────────────────────────

def pack(source_dir: Path, output_dir: Path, exact_batch: str | None, max_rows: int, dry_run: bool) -> None:
    print(f"[INFO] Source      : {source_dir}")
    print(f"[INFO] Output      : {output_dir}")
    if exact_batch:
        print(f"[INFO] Batch mode  : EXPLICIT ({exact_batch})")
    else:
        print(f"[INFO] Batch mode  : AUTO-INCREMENTAL (Max {max_rows} rows per batch)")
    print(f"[INFO] Dry-run     : {dry_run}\n")

    df = load_metadata(source_dir)
    wavs_dir = source_dir / "wavs"

    if not wavs_dir.is_dir():
        sys.exit(f"[ERROR] wavs/ directory not found inside {source_dir}")

    meta_dir = output_dir / "metadata"
    if not dry_run:
        meta_dir.mkdir(parents=True, exist_ok=True)

    moved, skipped = 0, 0
    
    # Process each source and speaker combination separately
    for (raw_audio_source, raw_speaker), group_df in df.groupby(["audio_source", "speaker"]):
        audio_source = sanitise(raw_audio_source)
        speaker = sanitise(raw_speaker)
        
        # Sort and break into chunks based on max_rows
        # If exact_batch is given, everything goes into that batch regardless of size.
        if exact_batch:
            chunks = [group_df]
        else:
            chunks = [group_df[i:i+max_rows] for i in range(0, len(group_df), max_rows)]
            
        start_batch_num = get_next_batch_num(output_dir, audio_source, speaker) if not exact_batch else 0
        
        for idx, chunk in enumerate(chunks):
            current_batch = exact_batch if exact_batch else f"batch_{start_batch_num + idx:04d}"
            
            new_audio_paths = []
            chunk_moved = 0
            
            for _, row in chunk.iterrows():
                original_rel: str = row["audio_path"]
                wav_filename = Path(original_rel).name

                src_file = wavs_dir / wav_filename
                if not src_file.exists():
                    print(f"[WARN] Missing wav file, skipping: {src_file}")
                    new_audio_paths.append(original_rel)   # keep original reference
                    skipped += 1
                    continue

                dest_dir  = output_dir / "audio" / audio_source / speaker / current_batch
                dest_file = dest_dir / wav_filename

                new_rel = f"audio/{audio_source}/{speaker}/{current_batch}/{wav_filename}"
                new_audio_paths.append(new_rel)

                if dry_run:
                    print(f"  [DRY] {src_file}  →  {dest_file}")
                else:
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src_file, dest_file)
                    chunk_moved += 1
            
            moved += chunk_moved
            
            # Save partitioned Parquet metadata for this specific batch
            chunk_copy = chunk.copy()
            chunk_copy["audio_path"] = new_audio_paths
            
            parquet_name = f"{audio_source}_{speaker}_{current_batch}.parquet"
            parquet_path = meta_dir / parquet_name
            
            if dry_run:
                print(f"  [DRY] metadata → {parquet_path}  ({len(chunk_copy)} rows)")
            else:
                chunk_copy.to_parquet(parquet_path, index=False)
                print(f"[INFO] Saved metadata: {parquet_path}  ({len(chunk_copy)} rows)")

    if not dry_run:
        print(f"\n[DONE] Moved {moved} wav files  |  Skipped {skipped}")
    else:
        print("\n[DONE] Dry-run complete — no files were changed.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pack flat audio dataset into partitioned DVC format."
    )
    parser.add_argument(
        "--source", required=True, type=Path,
        help="Path to the flat source directory (contains metadata.csv + wavs/)"
    )
    parser.add_argument(
        "--output", required=True, type=Path,
        help="Root output directory for the partitioned dataset"
    )
    parser.add_argument(
        "--batch", default=None,
        help="Explicit batch label (e.g. batch_2026_01). If omitted, batches auto-increment (batch_0001, etc.)."
    )
    parser.add_argument(
        "--max-rows", type=int, default=10000,
        help="Maximum number of items per batch when auto-incrementing (default: 10,000)."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview what would happen without moving any files."
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source_dir: Path = args.source.resolve()
    output_dir: Path = args.output.resolve()
    pack(
        source_dir=source_dir,
        output_dir=output_dir,
        exact_batch=args.batch,
        max_rows=args.max_rows,
        dry_run=args.dry_run
    )


if __name__ == "__main__":
    main()
