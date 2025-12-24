# TCGA Data Download Toolkit

This repository contains tools for downloading and validating TCGA (The Cancer Genome Atlas) data from the AWS S3 `tcga-2-open` bucket.

## Scripts Overview

### 1. `download_tcga_boto3.py` (Main Tool)
The primary script for batch downloading files based on a GDC Manifest.
*   **Batch Processing**: Reads a GDC Manifest file (TSV) to process multiple files.
*   **Smart Downloading**:
    *   **Pre-check**: Verifies file existence on S3 before attempting download.
    *   **Validation**: Automatically verifies MD5 checksums after download.
    *   **Resume**: Skips files that already exist locally with matching MD5 (`--skip-existing`).
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

**Resume/Skip Existing**:
Skip files that have already been downloaded and verified locally.
```bash
python3 download_tcga_boto3.py \
  --manifest ./manifest/gdc_manifest.txt \
  --output-base-dir ./downloads \
  --skip-existing
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
├── download_logs/          # Operation logs (.tsv)
│   └── tcga_download_log.tsv
└── tcga_dataset/           # Downloaded Data
    ├── [UUID_1]/
    │   └── filename_1.svs
    ├── [UUID_2]/
    │   └── filename_2.bam
    └── ...
```