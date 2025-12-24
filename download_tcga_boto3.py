#!/usr/bin/env python3
import csv
import os
import argparse
import hashlib
import sys
import datetime
import time
import threading

try:
    import boto3
    from botocore.exceptions import (
        ClientError,
        NoCredentialsError,
        PartialCredentialsError,
    )
    from botocore.config import Config
    from botocore import UNSIGNED
except ImportError:
    print("Error: boto3 package not found. Please install it: pip install boto3", file=sys.stderr)
    sys.exit(1)

# TCGA Open Data S3 Bucket
S3_BUCKET_OPEN = "s3://tcga-2-open"

# Defaults
DEFAULT_LOG_SUBDIR = "download_logs"
DEFAULT_DATASET_SUBDIR = "tcga_dataset"
DEFAULT_LOG_FILE = "tcga_download_log.tsv"


def calculate_md5(file_path, block_size=8192):
    """Calculates MD5 checksum of a local file."""
    if not os.path.exists(file_path):
        return None, f"File not found: {file_path}"
    md5_hash = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            while True:
                data = f.read(block_size)
                if not data:
                    break
                md5_hash.update(data)
        return md5_hash.hexdigest(), None
    except IOError as e:
        return None, f"Error reading file: {e}"
    except Exception as e:
        return None, f"Unknown error calculating MD5: {e}"


def parse_manifest(manifest_file_path):
    """Parses GDC manifest file and returns a list of file info dictionaries."""
    files_to_process = []
    # Column name variations to look for
    col_options_id = ["id", "uuid", "file_id"]
    col_options_filename = ["filename", "file_name"]
    col_options_md5 = ["md5", "md5sum"]
    col_options_size = ["size", "file_size"]

    try:
        with open(manifest_file_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")

            actual_col_names = {}
            if not reader.fieldnames:
                print(f"Error: Manifest file '{manifest_file_path}' is empty or header is unreadable.", file=sys.stderr)
                return None

            # Identify actual column names present in the file
            for expected_col_group, options in [
                ("id", col_options_id),
                ("filename", col_options_filename),
                ("md5", col_options_md5),
                ("size", col_options_size),
            ]:
                found = False
                for option in options:
                    if option in reader.fieldnames:
                        actual_col_names[expected_col_group] = option
                        found = True
                        break
                if not found and expected_col_group != "size":
                    print(
                        f"Error: Manifest '{manifest_file_path}' must contain '{expected_col_group}' column (or variants: {options}). "
                        f"Found: {reader.fieldnames}",
                        file=sys.stderr,
                    )
                    return None
                elif not found and expected_col_group == "size":
                    actual_col_names["size"] = None

            for row_number, row in enumerate(reader, 1):
                file_uuid = row.get(actual_col_names.get("id"))
                file_name = row.get(actual_col_names.get("filename"))
                md5_checksum = row.get(actual_col_names.get("md5"))
                file_size = (
                    row.get(actual_col_names.get("size"))
                    if actual_col_names.get("size")
                    else "N/A"
                )

                if not file_uuid or not file_name or not md5_checksum:
                    print(
                        f"Warning: Skipping row {row_number}, missing id, filename, or md5: {row}",
                        file=sys.stderr,
                    )
                    continue
                
                files_to_process.append({
                    "uuid": file_uuid,
                    "name": file_name,
                    "md5": md5_checksum.lower().strip(),
                    "size": file_size,
                })
    except FileNotFoundError:
        print(f"Error: Manifest file not found at {manifest_file_path}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error: Failed to parse manifest '{manifest_file_path}': {e}", file=sys.stderr)
        return None
    return files_to_process


def check_s3_object_existence(s3_client, bucket_name, s3_key):
    """
    Checks if S3 object exists and is accessible.
    Returns: (exists: bool, status_code: int, message: str)
    status_code: 0=Exists, 1=NotFound(404), 2=Forbidden(403), 3=OtherError
    """
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        return True, 0, "File exists"
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        http_status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        
        if error_code in ["404", "NoSuchKey"] or http_status == 404:
            return False, 1, f"Not Found (404)"
        elif error_code == "403" or http_status == 403:
            return False, 2, f"Forbidden (403)"
        else:
            return False, 3, f"AWS Error: {error_code}"
    except Exception as e:
        return False, 3, f"Unknown Error: {str(e)}"


def main():
    parser = argparse.ArgumentParser(
        description="Robust TCGA Data Downloader using GDC Manifest and AWS S3 (boto3).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-m", "--manifest", required=True, help="Path to GDC Manifest file (TSV).")
    parser.add_argument("-o", "--output-base-dir", required=True, help="Base output directory for data and logs.")
    parser.add_argument("-e", "--allowed-extensions", type=str, default=None, help="Filter by extensions (comma-separated, e.g. 'svs,bam').")
    parser.add_argument("-b", "--s3-bucket", default=S3_BUCKET_OPEN, help="S3 Bucket name.")
    parser.add_argument("--check-only", action="store_true", help="Only check file existence on S3, do not download.")
    parser.add_argument("--skip-existing", action="store_true", help="Skip download if local file exists and MD5 matches.")
    parser.add_argument("--no-sign-request", action="store_true", help="Use anonymous AWS access (default for tcga-2-open).")
    parser.add_argument("--aws-profile", help="AWS CLI profile to use.")
    parser.add_argument("--retries", type=int, default=3, help="Max retries for S3 download.")
    parser.add_argument("--retry-delay", type=int, default=2, help="Seconds between retries.")

    args = parser.parse_args()

    # --- Setup Configuration ---
    allowed_extensions_set = set()
    if args.allowed_extensions:
        allowed_extensions_set = {ext.strip().lower().lstrip(".") for ext in args.allowed_extensions.split(",") if ext.strip()}
        print(f"Info: Filtering for extensions: {', '.join(allowed_extensions_set)}")

    s3_bucket_name = args.s3_bucket.replace("s3://", "")
    use_no_sign_request = args.no_sign_request
    
    # Auto-enable anonymous access for public TCGA bucket if no profile specified
    if s3_bucket_name == "tcga-2-open" and not args.aws_profile:
        use_no_sign_request = True
        print("Info: Targeting public 'tcga-2-open' bucket without profile. Auto-enabling --no-sign-request.")

    # --- Parse Manifest ---
    all_files = parse_manifest(args.manifest)
    if not all_files:
        sys.exit(1)

    # --- Filter Files ---
    files_to_process = []
    skipped_extensions = 0
    
    if allowed_extensions_set:
        for f in all_files:
            ext = os.path.splitext(f["name"])[1].lstrip(".").lower()
            if ext in allowed_extensions_set:
                files_to_process.append(f)
            else:
                skipped_extensions += 1
        print(f"Info: {len(files_to_process)} files scheduled for processing ({skipped_extensions} skipped by extension filter).")
    else:
        files_to_process = all_files
        print(f"Info: All {len(files_to_process)} files scheduled for processing.")

    # --- Prepare Directories ---
    project_base_dir = os.path.abspath(args.output_base_dir)
    log_dir = os.path.join(project_base_dir, DEFAULT_LOG_SUBDIR)
    data_dir = os.path.join(project_base_dir, DEFAULT_DATASET_SUBDIR)
    
    os.makedirs(log_dir, exist_ok=True)
    if not args.check_only:
        os.makedirs(data_dir, exist_ok=True)

    log_file_path = os.path.join(log_dir, DEFAULT_LOG_FILE)
    print(f"Info: Logs will be written to: {log_file_path}")

    # --- Initialize S3 Client ---
    session_opts = {}
    client_opts = {}
    
    if args.aws_profile:
        session_opts["profile_name"] = args.aws_profile
    if use_no_sign_request:
        client_opts["config"] = Config(signature_version=UNSIGNED)

    try:
        session = boto3.Session(**session_opts)
        s3 = session.client("s3", **client_opts)
        # Quick connectivity test
        s3.head_bucket(Bucket=s3_bucket_name)
    except Exception as e:
        print(f"Error: Failed to initialize S3 client or connect to bucket '{s3_bucket_name}': {e}", file=sys.stderr)
        sys.exit(1)

    # --- Processing Loop ---
    stats = {
        "processed": 0,
        "success": 0,
        "skipped_existing": 0,
        "skipped_s3_error": 0,
        "failed": 0
    }

    log_headers = ["Timestamp", "Status", "UUID", "Filename", "S3_URI", "Local_Path", "Expected_MD5", "Actual_MD5", "Message"]
    
    with open(log_file_path, "w", newline="", encoding="utf-8") as log_f:
        writer = csv.DictWriter(log_f, fieldnames=log_headers, delimiter="\t")
        writer.writeheader()

        def log_event(status, item, message, local_path="N/A", actual_md5="N/A"):
            row = {
                "Timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "Status": status,
                "UUID": item["uuid"],
                "Filename": item["name"],
                "S3_URI": f"s3://{s3_bucket_name}/{item['uuid']}/{item['name']}",
                "Local_Path": local_path,
                "Expected_MD5": item["md5"],
                "Actual_MD5": actual_md5,
                "Message": message
            }
            writer.writerow(row)
            log_f.flush()

        for i, item in enumerate(files_to_process):
            stats["processed"] += 1
            uuid = item["uuid"]
            filename = item["name"]
            s3_key = f"{uuid}/{filename}"
            
            print(f"\n[{i+1}/{len(files_to_process)}] Processing: {filename} ({uuid})")
            
            # 1. Check S3 Existence
            exists_in_s3, code, msg = check_s3_object_existence(s3, s3_bucket_name, s3_key)
            if not exists_in_s3:
                print(f"  [SKIP] S3 Check Failed: {msg}")
                log_event("S3_CHECK_FAILED", item, msg)
                stats["skipped_s3_error"] += 1
                continue
            
            if args.check_only:
                print(f"  [OK] Found in S3.")
                log_event("CHECK_OK", item, "File exists in S3 (Check-only mode)")
                stats["success"] += 1
                continue

            # 2. Check Local Existence (Resume)
            target_uuid_dir = os.path.join(data_dir, uuid)
            local_path = os.path.join(target_uuid_dir, filename)
            
            if args.skip_existing and os.path.exists(local_path):
                print(f"  [CHECK] File exists locally. Verifying MD5...")
                local_md5, err = calculate_md5(local_path)
                if local_md5 == item["md5"]:
                    print(f"  [SKIP] MD5 Verified. Skipping download.")
                    log_event("SKIPPED_EXISTING", item, "MD5 verified locally", local_path, local_md5)
                    stats["skipped_existing"] += 1
                    continue
                else:
                    print(f"  [WARN] MD5 Mismatch (Local: {local_md5} vs Expected: {item['md5']}). Redownloading...")

            # 3. Download
            try:
                os.makedirs(target_uuid_dir, exist_ok=True)
                print(f"  [DOWNLOADING] ...")
                
                download_success = False
                for attempt in range(args.retries + 1):
                    try:
                        s3.download_file(s3_bucket_name, s3_key, local_path)
                        download_success = True
                        break
                    except ClientError as e:
                        if attempt < args.retries:
                            print(f"    Error (Attempt {attempt+1}): {e}. Retrying in {args.retry_delay}s...")
                            time.sleep(args.retry_delay)
                        else:
                            raise e

                if download_success:
                    # 4. Verify Download
                    print(f"  [VERIFYING] Calculating MD5...")
                    final_md5, err = calculate_md5(local_path)
                    if final_md5 == item["md5"]:
                        print(f"  [SUCCESS] Download verified.")
                        log_event("SUCCESS", item, "Download and verification successful", local_path, final_md5)
                        stats["success"] += 1
                    else:
                        print(f"  [FAIL] Integrity check failed! Got {final_md5}, expected {item['md5']}")
                        log_event("FAILED_INTEGRITY", item, "MD5 mismatch after download", local_path, final_md5)
                        stats["failed"] += 1

            except Exception as e:
                print(f"  [ERROR] Download failed: {e}")
                log_event("FAILED_DOWNLOAD", item, str(e), local_path)
                stats["failed"] += 1

    # --- Summary ---
    print("\n" + "="*50)
    print("Execution Summary")
    print("="*50)
    print(f"Total Files in Manifest:   {len(all_files)}")
    print(f"Filtered for Processing:   {len(files_to_process)}")
    print(f"Skipped (Extension):       {skipped_extensions}")
    print("-" * 30)
    print(f"Successfully Processed:    {stats['success']}")
    print(f"Skipped (Local Existing):  {stats['skipped_existing']}")
    print(f"Skipped (S3 Missing/Err):  {stats['skipped_s3_error']}")
    print(f"Failed (Download/Verify):  {stats['failed']}")
    print("-" * 30)
    print(f"Log file: {log_file_path}")
    print("="*50)

if __name__ == "__main__":
    main()