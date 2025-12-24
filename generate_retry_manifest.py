#!/usr/bin/env python3
"""
Generate a retry manifest from failed downloads log.
This creates a standard GDC manifest format that can be used with the main download script.
"""
import csv
import argparse
import sys
import os


def main():
    parser = argparse.ArgumentParser(
        description="Generate retry manifest from failed downloads log file."
    )
    parser.add_argument(
        "-l", "--log-file",
        required=True,
        help="Path to download log TSV file (e.g., tcga_download_log_20231224_105701.tsv)"
    )
    parser.add_argument(
        "-o", "--output",
        required=True,
        help="Output manifest file path (TSV format)"
    )
    parser.add_argument(
        "--failed-only",
        action="store_true",
        default=True,
        help="Only include failed files (default: True)"
    )
    
    args = parser.parse_args()
    
    if not os.path.exists(args.log_file):
        print(f"Error: Log file not found: {args.log_file}", file=sys.stderr)
        sys.exit(1)
    
    # Parse log file
    retry_items = []
    failed_statuses = ["FAILED_DOWNLOAD", "FAILED_INTEGRITY", "S3_CHECK_FAILED"]
    
    try:
        with open(args.log_file, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                status = row.get("Status", "")
                if args.failed_only and status not in failed_statuses:
                    continue
                
                retry_items.append({
                    "id": row.get("UUID"),
                    "filename": row.get("Filename"),
                    "md5": row.get("Expected_MD5"),
                    "size": "N/A",
                    "state": f"retry_{status.lower()}"
                })
    except Exception as e:
        print(f"Error: Failed to parse log file: {e}", file=sys.stderr)
        sys.exit(1)
    
    if not retry_items:
        print("Info: No failed files found in log. Nothing to retry.")
        sys.exit(0)
    
    # Write manifest
    try:
        with open(args.output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, 
                fieldnames=["id", "filename", "md5", "size", "state"],
                delimiter="\t"
            )
            writer.writeheader()
            writer.writerows(retry_items)
        
        print(f"✓ Generated retry manifest: {args.output}")
        print(f"  Total files to retry: {len(retry_items)}")
        print(f"\nRetry command:")
        print(f"  python3 download_tcga_boto3.py \\")
        print(f"    --manifest {args.output} \\")
        print(f"    --output-base-dir <your-output-dir>")
    except Exception as e:
        print(f"Error: Failed to write manifest: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
