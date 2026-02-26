# DVC and S3 Compatible Remote Setup Guide

This guide provides step-by-step instructions on how to set up Data Version Control (DVC), configure it with an S3-compatible remote storage server, seamlessly convert your flat audio dataset into a DVC-optimized partitioned format, and push/pull your data.

## Table of Contents

1. [First Time Data Push (Initialization & Setup)](#1-first-time-data-push-initialization--setup)
2. [Adding New Data (Incremental Updates)](#2-adding-new-data-incremental-updates)
3. [Pulling and Unpacking Data](#3-pulling-and-unpacking-data)

---

## 1. First Time Data Push (Initialization & Setup)

### Prerequisites

- A Python environment (e.g., Conda, virtualenv).
- Git repository initialized in your project.
- Access credentials for an S3-compatible storage server (Endpoint URL, Access Key, Secret Key).
- A bucket created on the S3-compatible server (e.g., `test-env`).

### Installation

First, ensure you are in your desired Python environment. Then, install DVC along with the `dvc-s3` plugin and the required metadata handling libraries:

```bash
# Example using a Conda environment named 'test'
conda activate test

# Install requirements
pip install -r requirements.txt
# OR manually: pip install "dvc[s3]" pandas pyarrow
```

### Initialize Git and DVC

If you haven't already, initialize a Git repository and a DVC repository in your project directory:

```bash
# Initialize Git
git init

# Initialize DVC
dvc init

# Commit the initial DVC files
git commit -m "Initialize DVC"
```

### Configure S3 Compatible Remote

Configure DVC to use your S3-compatible server as remote storage. Follow these steps to keep your credentials secure.

```bash
# 1. Add the remote and set it as the default
# Format: dvc remote add -d <remote_name> s3://<bucket_name>/<prefix_path>
dvc remote add -d myremote s3://test-env/dvc-store

# 2. Set the custom Endpoint URL
dvc remote modify myremote endpointurl http://172.16.229.199:30293/

# 3. Configure credentials using --local to avoid committing them to Git
dvc remote modify --local myremote access_key_id YOUR_ACCESS_KEY
dvc remote modify --local myremote secret_access_key YOUR_SECRET_KEY

# 4. Verify the remote
dvc remote list
```

**Note on `--local`**: Using the `--local` flag saves your keys in `.dvc/config.local`, which is hidden and ignored by Git by default. This ensures your secrets are never accidentally leaked in version control.

### Performance Settings (Do Once)

```bash
# Avoid doubling your disk usage: use hardlinks instead of file copies
dvc config cache.type hardlink,symlink

# Use 40-50 parallel S3 connections for fast transfers
dvc remote modify myremote jobs 40
```

### Convert FLAT format to PARTITIONED DVC format

Your raw data arrives as flat `wavs/` and a `metadata.csv`. DVC struggles with millions of files in one folder. We use `pack.py` to organize it into an S3-optimized hierarchy.

**Usage:**

```bash
# Basic (auto-incremental batch based on existing batches, up to 10k rows/batch):
python scripts/pack.py \
    --source /path/to/test_out_tiny \
    --output dataset

# Explicit max rows (limit batch size):
python scripts/pack.py \
    --source /path/to/test_out_tiny \
    --output dataset \
    --max-rows 5000

# Explicit batch label (no auto-incrementing):
python scripts/pack.py \
    --source /path/to/test_out_tiny \
    --output dataset \
    --batch batch_2026_01
```

### Add to DVC

Since this is the first time, you tell DVC to track the entire dataset folder you just packed:

```bash
dvc add dataset
```

### Add to Git and Commit

```bash
# For the 1st time, add the master tracking file and gitignore
git add dataset.dvc .gitignore
git commit -m "Track the initial dataset"
```

### DVC & Git Push

Push the actual audio files and Parquet metadata to your S3 bucket:

```bash
dvc push
```
and 

```bash
git push origin main
```

---

## 2. Adding New Data (Incremental Updates)

When a new batch of flat data arrives, here is how you align it with the existing S3/DVC dataset without having to track or upload the old 1TB dataset again.

### 1. Pack the new data into the existing dataset directory

Point `pack.py` to your new data, but output to the *same* `dataset` folder. It will automatically increment the batch number (e.g., create `batch_0002` alongside `batch_0001`).

```bash
python scripts/pack.py \
    --source /path/to/new_data \
    --output dataset
```

### 2. Add ONLY the new data paths to DVC

Instead of running `dvc add dataset` (which scans the whole 1TB), explicitly add only the new granular folders that were just generated.

```bash
# Add only the new batch folders (example)
dvc add dataset/audio/youtube/somrat/batch_0002
dvc add dataset/audio/youtube/shawon/batch_0002

# Tell DVC to track the updated metadata folder
dvc add dataset/metadata
```

### 3. Add to Git and Commit

```bash
git add dataset/audio/youtube/somrat/batch_0002.dvc \
        dataset/audio/youtube/shawon/batch_0002.dvc \
        dataset/metadata.dvc
        
git commit -m "Track new dataset batch_0002"
```

### 4. DVC Push

```bash
dvc push
```
and

```bash
git push origin main
```

DVC calculates hashes and **instantly uploads only the new data**. The previous data remains untouched in the S3 bucket.

---

## 3. Pulling and Unpacking Data

### Pulling Specific Data

A team member clones the repo to train a model. They don't want the full 1TB; they just want Somrat's data.

```bash
# Pull entire dataset
dvc pull dataset

# Pull only the metadata first to see what's available
dvc pull dataset/metadata

# Pull only the audio required
dvc pull dataset/audio/youtube/somrat/
```

### Unpack: Convert Partitioned → Flat

After pulling a subset via DVC, use `unpack.py` to reconstruct the flat format (single `metadata.csv` + `wavs/` directory) that your training pipelines expect.

```bash
# Output flat format for the specific speaker you pulled
python scripts/unpack.py \
    --dataset dataset \
    --output flat_training_data \
    --speaker somrat
```

Options available:
- `--speaker`
- `--audio-source`
- `--batch`
