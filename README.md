# TCGA Data Download Toolkit

This repository contains tools for downloading and validating TCGA (The Cancer Genome Atlas) data from the AWS S3 `tcga-2-open` bucket.

## Scripts Overview

### 1. `download_tcga_boto3.py` (Main Tool)
The primary script for batch downloading files based on a GDC Manifest.
*   **Batch Processing**: Reads a GDC Manifest file (TSV) to process multiple files.
*   **Smart Downloading**:
    *   **Pre-check**: Verifies file existence on S3 before attempting download.
    *   **Validation**: Automatically verifies MD5 checksums after download.
    *   **Resume/Breakpoint-Continuation**: Skips files that already exist locally with matching MD5 (enabled by default).
        *   **Three-Layer Verification**:
            1. Checks `completed_downloads.txt` for previously completed files (persistent across runs)
            2. Checks if local file exists
            3. Verifies MD5 checksum for integrity
        *   **Persistent Logging**: Completed downloads are recorded in `completed_downloads.txt` and never lost, even if interrupted
    *   **Filtering**: Can filter downloads by file extension (e.g., only download `.svs`).
*   **Check-Only Mode**: Can verify S3 availability without downloading files (`--check-only`).

### 2. `download_by_uuid.py` (Single File Utility)
A helper script to download or inspect a specific file/folder by its UUID without a manifest.
*   Useful for ad-hoc downloads or inspecting the contents of a specific UUID folder on S3.

### 3. `generate_retry_manifest.py` (Retry Helper)
Extract failed downloads from log files and generate a retry manifest.
*   Parses download logs to identify failed files
*   Creates a new manifest containing only failed downloads
*   Useful for targeted retry without reprocessing successful downloads

## Prerequisites

*   Python 3.x
*   `boto3` library

```bash
pip install boto3
```

## Usage Examples

### 1. Batch Download with Manifest (`download_tcga_boto3.py`)

**Manifest Format**:
The script expects a standard GDC Manifest TSV with columns like `id` (or `uuid`), `filename`, `md5`.
Example:
```tsv
id	filename	md5	size	state
53f2835e-e13a-4fb6-90a5-448a1a726249	b1d9364c-d703-4884-b96d-20d8084040a8.rna_seq.star_splice_junctions.tsv.gz	b05d932361f3b6309c457c5076992a04	2032798	released
eb73b4c4-3a8c-475a-bfd1-8faf77cd2a88	e3353808-13c7-4cdc-b9bf-6dbdce522f4e.wxs.VarScan2.aliquot.maf.gz	eecde67a7ffe5ea10e8a8ef060fa67d4	401565	released
```

**Basic Download**:
Download all files listed in the manifest to a specific directory.
```bash
python3 download_tcga_boto3.py \
  --manifest ./manifest/gdc_manifest.txt \
  --output-base-dir ./downloads
```

**Filter by Extension**:
Only download SVS pathology images.
```bash
python3 download_tcga_boto3.py \
  --manifest ./manifest/gdc_manifest.txt \
  --output-base-dir ./downloads \
  --allowed-extensions svs
```

**Resume/Skip Existing (Default Enabled)**:
Skip files that have already been downloaded and verified locally. Enabled by default for automatic breakpoint-continuation.
```bash
python3 download_tcga_boto3.py \
  --manifest ./manifest/gdc_manifest.txt \
  --output-base-dir ./downloads
```

**Force Re-download (Disable Resume)**:
To force re-download all files without resuming from previous runs:
```bash
python3 download_tcga_boto3.py \
  --manifest ./manifest/gdc_manifest.txt \
  --output-base-dir ./downloads \
  --no-skip-existing
```

**Check S3 Existence Only (No Download)**:
Verify if files in the manifest exist on S3 without downloading them.
```bash
python3 download_tcga_boto3.py \
  --manifest ./manifest/gdc_manifest.txt \
  --output-base-dir ./downloads \
  --check-only
```

### 2. Single UUID Download (`download_by_uuid.py`)

**Download a specific UUID**:
```bash
python3 download_by_uuid.py \
  --uuid 30e4624b-6f48-429b-b1d9-6a6bc5c82c5e \
  --output_dir ./single_downloads
```

**List contents of a UUID**:
If you don't specify a filename, it lists all files under that UUID.
```bash
python3 download_by_uuid.py \
  --uuid 30e4624b-6f48-429b-b1d9-6a6bc5c82c5e \
  --output_dir ./tmp
```

### 3. Retry Failed Downloads

**Method 1: Direct Retry from Log File (Recommended - Fastest)**
```bash
# If you have 100 files with 5 failures, retry only the 5 failed ones
python3 download_tcga_boto3.py \
  --retry-failed-log ./downloads/download_logs/tcga_download_log_20231224_105701.tsv \
  --output-base-dir ./downloads
```

**Method 2: Generate Retry Manifest (More Flexible)**
```bash
# Step 1: Generate a manifest from failed files
python3 generate_retry_manifest.py \
  --log-file ./downloads/download_logs/tcga_download_log_20231224_105701.tsv \
  --output ./manifest/retry_manifest.txt

# Step 2: Use the retry manifest
python3 download_tcga_boto3.py \
  --manifest ./manifest/retry_manifest.txt \
  --output-base-dir ./downloads
```

**Method 3: Simple Re-run (Works but slower for large datasets)**
```bash
# Just re-run the original command - it will skip completed files
# WARNING: Will recalculate MD5 for all existing files (slow for large files)
python3 download_tcga_boto3.py \
  --manifest ./manifest/gdc_manifest.txt \
  --output-base-dir ./downloads
```

**Fast Resume Mode (Skip MD5 Calculation)**
```bash
# Trust completed_downloads.txt without re-verifying MD5 (faster but less safe)
python3 download_tcga_boto3.py \
  --manifest ./manifest/gdc_manifest.txt \
  --output-base-dir ./downloads \
  --fast-resume
```

## Directory Structure

The `download_tcga_boto3.py` script will create the following structure:

```
/output-base-dir/
├── download_logs/                      # Operation logs (.tsv + tracking)
│   ├── tcga_download_log_20231224_105701.tsv   # Timestamped session log
│   ├── tcga_download_log_20231224_110015.tsv   # Another session log
│   ├── completed_downloads.txt         # Persistent record of completed files (never lost)
│   └── failed_downloads.txt            # Failed files from last session (overwritten each run)
└── tcga_dataset/                       # Downloaded Data
    ├── [UUID_1]/
    │   └── filename_1.svs
    ├── [UUID_2]/
    │   └── filename_2.bam
    └── ...
```

### Log Files Explanation

*   **`tcga_download_log_TIMESTAMP.tsv`**: Session-specific log for each run, includes columns:
    *   `Timestamp`: When the action occurred
    *   `Status`: SUCCESS, SKIPPED_EXISTING, FAILED_INTEGRITY, etc.
    *   `UUID`, `Filename`: File identification
    *   `Expected_MD5`, `Actual_MD5`: Checksum verification records
    *   `Message`: Additional details

*   **`completed_downloads.txt`**: Persistent record that persists across runs:
    *   Format: `uuid|filename|md5` (one per line)
    *   **Never gets overwritten or deleted** - it only appends
    *   Enables automatic breakpoint-continuation: if the script is interrupted and rerun, it will skip any files already recorded here

*   **`failed_downloads.txt`**: Session-specific failed files list:
    *   Format: `uuid|filename|md5|status|message` (one per line)
    *   **Overwritten each run** with failures from current session
    *   Used for quick reference and debugging

### Breakpoint-Continuation (断点续传) Features

The script now implements full breakpoint-continuation support:

1. **Automatic Resume**: When you rerun the script with the same manifest and output directory, it automatically skips:
   - Files marked as completed in `completed_downloads.txt`
   - Files that exist locally with matching MD5
   
2. **No Manual Configuration Needed**: `--skip-existing` is enabled by default

3. **Persistent Tracking**: The `completed_downloads.txt` file ensures tracking persists even if:
   - The script is interrupted (Ctrl+C)
   - The process crashes
   - The server disconnects
   - Multiple concurrent runs execute (each appends to the same log)

**Example Scenario**:
```bash
# First run: Downloads 100 files, completes 80, then interrupted
nohup python3 download_tcga_boto3.py --manifest manifest.txt --output-base-dir /data > log1.log 2>&1 &

# Second run: Automatically resumes from file 81, skips the 80 already completed
nohup python3 download_tcga_boto3.py --manifest manifest.txt --output-base-dir /data > log2.log 2>&1 &
```

## Performance Optimization

### MD5 Calculation Cost Analysis

**Problem**: MD5 calculation is CPU-intensive for large files (e.g., 5GB SVS files can take 30+ seconds per file)

**Solutions Available**:

1. **Fast Resume Mode** (`--fast-resume`):
   - Trusts `completed_downloads.txt` without re-verifying MD5
   - Only checks if file exists and has non-zero size
   - ⚡ **~100x faster** for large datasets with many existing files
   - ⚠️ **Less safe**: Won't detect corrupted files

2. **Direct Log Retry** (`--retry-failed-log`):
   - Only processes files that failed in a specific run
   - Skips all successful files without any checks
   - ⚡ **Instant start** - no manifest parsing or validation needed
   - ✅ **Recommended for retry scenarios**

3. **Standard Mode** (default):
   - Full MD5 verification for all existing files
   - Safest but slowest
   - Good for first run or when data integrity is critical

### Retry Strategy Recommendations

| Scenario | Recommended Method | Command |
|----------|-------------------|---------|
| **Network interruption during download** | Direct log retry | `--retry-failed-log log.tsv` |
| **First retry after failures** | Direct log retry | `--retry-failed-log log.tsv` |
| **Multiple retries needed** | Generate retry manifest | `generate_retry_manifest.py` |
| **Large dataset resume (trusted source)** | Fast resume | `--fast-resume` |
| **Data integrity critical** | Standard mode | (no extra flags) |

### Example Workflow

```bash
# Initial download (100 files, 5 fail due to network issues)
python3 download_tcga_boto3.py \
  --manifest manifest.txt \
  --output-base-dir /data

# Output shows:
# ⚠️  RETRY COMMAND for failed files:
# python3 download_tcga_boto3.py \
#   --retry-failed-log /data/download_logs/tcga_download_log_20231224_105701.tsv \
#   --output-base-dir /data

# Quick retry (only processes the 5 failed files)
python3 download_tcga_boto3.py \
  --retry-failed-log /data/download_logs/tcga_download_log_20231224_105701.tsv \
  --output-base-dir /data

# If failures persist, generate manifest for manual inspection
python3 generate_retry_manifest.py \
  --log-file /data/download_logs/tcga_download_log_20231224_110230.tsv \
  --output /data/manifest/retry_manifest.txt
```