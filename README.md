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

## Directory Structure

The `download_tcga_boto3.py` script will create the following structure:

```
/output-base-dir/
├── download_logs/                      # Operation logs (.tsv + tracking)
│   ├── tcga_download_log_20231224_105701.tsv   # Timestamped session log
│   ├── tcga_download_log_20231224_110015.tsv   # Another session log
│   └── completed_downloads.txt         # Persistent record of completed files (never lost)
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