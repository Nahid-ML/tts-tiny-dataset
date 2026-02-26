#!/usr/bin/env python3
"""
unpack.py — Convert PARTITIONED DVC format back to FLAT format
==============================================================
After pulling a subset of data from S3 via DVC, use this script to
reconstruct the flat directory layout that your training pipeline expects:

  <output_dir>/
    metadata.csv
    wavs/
      *.wav

INPUT (partitioned / DVC format):
  <dataset_dir>/
    metadata/
      *.parquet
    audio/
      <audio_source>/
        <speaker>/
          <batch>/
            *.wav

Usage
-----
  # Unpack everything:
  python unpack.py --dataset /path/to/dataset \
                   --output  /path/to/flat_output

  # Filter by speaker:
  python unpack.py --dataset /path/to/dataset \
                   --output  /path/to/flat_output \
                   --speaker somrat

  # Filter by audio_source:
  python unpack.py --dataset /path/to/dataset \
                   --output  /path/to/flat_output \
                   --audio-source youtube

  # Combine filters (AND logic):
  python unpack.py --dataset /path/to/dataset \
                   --output  /path/to/flat_output \
                   --speaker somrat --audio-source youtube

  # Filter by batch:
  python unpack.py --dataset /path/to/dataset \
                   --output  /path/to/flat_output \
                   --batch   batch_2026_01

  # Dry-run (print what would happen, don't copy files):
  python unpack.py --dataset ./dataset \
                   --output  ./flat_output \
                   --dry-run
"""

import argparse
import shutil
import sys
from pathlib import Path

import pandas as pd

# ── helpers ───────────────────────────────────────────────────────────────────

def load_all_metadata(meta_dir: Path) -> pd.DataFrame:
    """Read and concatenate all Parquet files in the metadata/ directory."""
    parquet_files = list(meta_dir.glob("*.parquet"))
    if not parquet_files:
        sys.exit(f"[ERROR] No .parquet files found in {meta_dir}")
    frames = []
    for f in parquet_files:
        frames.append(pd.read_parquet(f))
    df = pd.concat(frames, ignore_index=True)
    print(f"[INFO] Loaded {len(df)} total metadata rows from {len(parquet_files)} parquet file(s).")
    return df


def apply_filters(
    df: pd.DataFrame,
    speaker: str | None,
    audio_source: str | None,
    batch: str | None,
) -> pd.DataFrame:
    """Filter the metadata dataframe according to CLI arguments."""
    original_len = len(df)
    if speaker:
        df = df[df["speaker"].str.lower() == speaker.lower()]
    if audio_source:
        df = df[df["audio_source"].str.lower() == audio_source.lower()]
    if batch:
        # batch info is embedded in the audio_path, e.g. audio/youtube/somrat/batch_2026_01/file.wav
        df = df[df["audio_path"].str.contains(f"/{batch}/", regex=False)]
    print(f"[INFO] After filters: {len(df)} rows  (was {original_len})")
    if df.empty:
        sys.exit("[ERROR] No rows match the specified filters. Aborting.")
    return df


# ── core logic ─────────────────────────────────────────────────────────────────

def unpack(
    dataset_dir: Path,
    output_dir: Path,
    speaker: str | None,
    audio_source: str | None,
    batch: str | None,
    dry_run: bool,
) -> None:
    print(f"[INFO] Dataset dir : {dataset_dir}")
    print(f"[INFO] Output dir  : {output_dir}")
    print(f"[INFO] Filters     : speaker={speaker!r}  audio_source={audio_source!r}  batch={batch!r}")
    print(f"[INFO] Dry-run     : {dry_run}\n")

    meta_dir = dataset_dir / "metadata"
    if not meta_dir.is_dir():
        sys.exit(f"[ERROR] metadata/ directory not found in {dataset_dir}")

    df = load_all_metadata(meta_dir)
    df = apply_filters(df, speaker, audio_source, batch)

    # Prepare output paths
    out_wavs = output_dir / "wavs"
    flat_audio_paths = []
    copied, missing = 0, 0

    for _, row in df.iterrows():
        # audio_path is relative to dataset_dir: audio/youtube/somrat/batch_xxx/file.wav
        src_file = dataset_dir / row["audio_path"]
        wav_filename = src_file.name
        dest_file = out_wavs / wav_filename

        # Flat path for the output metadata.csv
        flat_audio_paths.append(f"wavs/{wav_filename}")

        if not src_file.exists():
            print(f"[WARN] Source wav not found (run `dvc pull` first?): {src_file}")
            missing += 1
            continue

        if dry_run:
            print(f"  [DRY] {src_file}  →  {dest_file}")
        else:
            out_wavs.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_file, dest_file)
            copied += 1

    # ── Write flat metadata.csv ───────────────────────────────────────────────
    df = df.copy()
    df["audio_path"] = flat_audio_paths
    csv_path = output_dir / "metadata.csv"

    if dry_run:
        print(f"\n  [DRY] metadata.csv → {csv_path}  ({len(df)} rows)")
    else:
        output_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(csv_path, index=False)
        print(f"\n[INFO] Saved flat metadata: {csv_path}  ({len(df)} rows)")
        print(f"[DONE] Copied {copied} wav files  |  Missing {missing} files")
        if missing:
            print("[HINT] Run the appropriate `dvc pull` command to fetch missing files.")
    if dry_run:
        print("\n[DONE] Dry-run complete — no files were changed.")


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Unpack partitioned DVC dataset back to the flat training format."
    )
    parser.add_argument(
        "--dataset", required=True, type=Path,
        help="Root of the partitioned dataset (contains metadata/ and audio/)"
    )
    parser.add_argument(
        "--output", required=True, type=Path,
        help="Output directory for the flat layout (metadata.csv + wavs/)"
    )
    parser.add_argument(
        "--speaker", default=None,
        help="Filter: only include rows matching this speaker name (case-insensitive)."
    )
    parser.add_argument(
        "--audio-source", default=None,
        help="Filter: only include rows matching this audio_source (case-insensitive)."
    )
    parser.add_argument(
        "--batch", default=None,
        help="Filter: only include files from a specific batch folder (e.g. batch_2026_01)."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview what would happen without copying any files."
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    unpack(
        dataset_dir  = args.dataset.resolve(),
        output_dir   = args.output.resolve(),
        speaker      = args.speaker,
        audio_source = args.audio_source,
        batch        = args.batch,
        dry_run      = args.dry_run,
    )


if __name__ == "__main__":
    main()
