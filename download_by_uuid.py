import boto3
import os
import argparse
import sys
import threading
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError

class ProgressPercentage(object):
    def __init__(self, filename, filesize):
        self._filename = filename
        self._size = float(filesize)
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, we assume the main thread is handling the output or we use sys.stdout
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size, percentage))
            sys.stdout.flush()

def get_s3_client():
    """Configures and returns an anonymous S3 client."""
    return boto3.client("s3", config=Config(signature_version=UNSIGNED))

def list_files_in_uuid(uuid, bucket_name="tcga-2-open"):
    """
    Lists all objects within the 'folder' defined by the UUID.
    Returns a list of dictionaries containing 'Key' and 'Size'.
    """
    s3_client = get_s3_client()
    
    # ensure uuid ends with / to treat it strictly as a folder prefix
    prefix = f"{uuid}/"
    
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    except ClientError as e:
        print(f"Error listing objects: {e}", file=sys.stderr)
        return []

    if 'Contents' in response:
        return response['Contents']
    else:
        return []

def download_file(bucket_name, s3_key, output_dir, file_size=None):
    """Downloads a single file given its full S3 Key."""
    s3_client = get_s3_client()
    
    # Extract filename from the key (everything after the last /)
    filename = s3_key.split('/')[-1]
    
    # Full local path
    local_file_path = os.path.join(output_dir, filename)

    print(f"  Downloading: {filename}")
    print(f"  -> To: {local_file_path}")

    # If file size is not provided (Direct Mode), try to get it
    if file_size is None:
        try:
            head = s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            file_size = head['ContentLength']
        except:
            file_size = 0 # Unknown

    callback = None
    if file_size > 0:
        callback = ProgressPercentage(filename, file_size)

    try:
        s3_client.download_file(bucket_name, s3_key, local_file_path, Callback=callback)
        print(f"\n  [SUCCESS] Saved to {local_file_path}")
        return True
    except ClientError as e:
        print(f"\n") # Newline after progress bar
        if e.response['Error']['Code'] == "404":
             print(f"  [ERROR] File not found in S3.", file=sys.stderr)
        elif e.response['Error']['Code'] == "403":
             print(f"  [ERROR] Access denied.", file=sys.stderr)
        else:
             print(f"  [ERROR] AWS Client Error: {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"\n  [ERROR] Unexpected error: {e}", file=sys.stderr)
        return False

def main():
    parser = argparse.ArgumentParser(description="Download files from TCGA S3 bucket by UUID. Can list contents automatically.")
    parser.add_argument("--uuid", required=True, help="The UUID of the file/folder.")
    parser.add_argument("--filename", required=False, help="Optional: Specific filename. If omitted, will list and download all files in UUID.")
    parser.add_argument("--output_dir", required=True, help="The local directory to save the file.")
    parser.add_argument("--bucket", default="tcga-2-open", help="S3 Bucket name (default: tcga-2-open")
    
    args = parser.parse_args()

    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    if args.filename:
        # Classical mode: User provided specific filename
        s3_key = f"{args.uuid}/{args.filename}"
        print(f"--- Direct Download Mode ---")
        download_file(args.bucket, s3_key, args.output_dir)
    else:
        # Discovery mode: List contents first
        print(f"--- Discovery Mode ---")
        print(f"Listing contents for UUID: {args.uuid} ...")
        
        files_found = list_files_in_uuid(args.uuid, args.bucket)
        
        if not files_found:
            print(f"[WARNING] No files found under UUID: {args.uuid}")
            print(f"Possible reasons:")
            print(f"1. The UUID is incorrect.")
            print(f"2. The directory is empty.")
            print(f"3. This UUID resides in a controlled-access bucket (not tcga-2-open).")
        else:
            print(f"Found {len(files_found)} file(s):")
            for obj in files_found:
                print(f" - {obj['Key']} (Size: {obj['Size']} bytes)")
            
            print(f"\nStarting download of {len(files_found)} file(s)...")
            for obj in files_found:
                download_file(args.bucket, obj['Key'], args.output_dir, file_size=obj['Size'])

if __name__ == "__main__":
    main()
