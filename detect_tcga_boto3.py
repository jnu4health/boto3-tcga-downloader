#!/usr/bin/env python3
import csv
import os
import datetime
import sys

try:
    import boto3
    from botocore.exceptions import ClientError
    from botocore.config import Config
    from botocore import UNSIGNED  # 用于匿名访问
except ImportError:
    print("错误：boto3 包未找到。请先安装：pip install boto3", file=sys.stderr)
    sys.exit(1)

# --- 用户配置 ---
# 在这里直接定义 Manifest 文件路径
MANIFEST_FILE_PATH = "/home/huozh/demo/code-project/A-DataSet/GDC/gdc_manifest/gdc_coad_manifest.2025-05-29.135618.txt"
# 定义日志文件路径 (脚本将尝试创建此文件所在的目录)
LOG_FILE_PATH = "/home/huozh/demo/code-project/A-DataSet/GDC/download_logs/tcga_s3_check_failed.log"
# S3 存储桶名称
S3_BUCKET_NAME = "tcga-2-open"
# --- 用户配置结束 ---

def parse_manifest_for_check(manifest_file_path):
    """
    精简版 manifest 解析器，仅提取 id 和 filename。
    """
    files_to_check = []
    col_options_id = ["id", "uuid", "file_id"]
    col_options_filename = ["filename", "file_name"]

    try:
        with open(manifest_file_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            if not reader.fieldnames:
                print(f"错误：Manifest 文件 '{manifest_file_path}' 为空或无法读取列名。", file=sys.stderr)
                return None

            actual_col_id = None
            for option in col_options_id:
                if option in reader.fieldnames:
                    actual_col_id = option
                    break
            if not actual_col_id:
                print(f"错误：Manifest 文件未找到 'id' 列 (或其变体)。找到的列: {reader.fieldnames}", file=sys.stderr)
                return None

            actual_col_filename = None
            for option in col_options_filename:
                if option in reader.fieldnames:
                    actual_col_filename = option
                    break
            if not actual_col_filename:
                print(f"错误：Manifest 文件未找到 'filename' 列 (或其变体)。找到的列: {reader.fieldnames}", file=sys.stderr)
                return None

            for row_num, row in enumerate(reader, 1):
                file_uuid = row.get(actual_col_id)
                file_name = row.get(actual_col_filename)
                if not file_uuid or not file_name:
                    print(f"警告：跳过 Manifest 第 {row_num} 行，缺少 UUID 或文件名: {row}", file=sys.stderr)
                    continue
                files_to_check.append({"uuid": file_uuid, "name": file_name})
        return files_to_check
    except FileNotFoundError:
        print(f"错误：Manifest 文件未在路径 {manifest_file_path} 找到", file=sys.stderr)
        return None
    except Exception as e:
        print(f"错误：解析 Manifest 文件 '{manifest_file_path}' 失败: {e}", file=sys.stderr)
        return None

def check_s3_object_existence(s3_client, bucket_name, s3_key):
    """检查 S3 对象是否存在，返回 (是否存在, 错误信息或状态)"""
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        return True, "文件存在"
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        http_status_code = e.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        if error_code == '404' or error_code == 'NoSuchKey' or http_status_code == 404:
            return False, "文件未找到 (404 Not Found)"
        else:
            return False, f"AWS API 错误: {str(e)}"
    except Exception as e:
        return False, f"未知错误: {str(e)}"

def main():
    print(f"开始检测 Manifest 文件：{MANIFEST_FILE_PATH}")
    print(f"S3 存储桶：s3://{S3_BUCKET_NAME}")
    print(f"失败日志将写入：{LOG_FILE_PATH}")

    # 确保日志文件目录存在
    log_dir = os.path.dirname(LOG_FILE_PATH)
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir, exist_ok=True)
        except OSError as e:
            print(f"错误：无法创建日志目录 {log_dir}: {e}", file=sys.stderr)
            sys.exit(1)

    file_infos = parse_manifest_for_check(MANIFEST_FILE_PATH)
    if not file_infos:
        sys.exit(1)

    print(f"从 Manifest 中解析到 {len(file_infos)} 个文件条目待检测。")

    # 初始化 S3 客户端 (使用匿名访问，因为 tcga-2-open 是公开的)
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    failed_count = 0
    processed_count = 0

    with open(LOG_FILE_PATH, "w", encoding="utf-8") as log_f:
        log_f.write("时间戳\tUUID\t文件名\tS3_URI\t错误信息\n") # 日志表头

        for i, file_info in enumerate(file_infos):
            processed_count += 1
            uuid = file_info["uuid"]
            filename = file_info["name"]
            s3_key = f"{uuid}/{filename}"
            s3_uri = f"s3://{S3_BUCKET_NAME}/{s3_key}"

            if i % 100 == 0 and i > 0: # 每处理100个文件打印一次进度
                print(f"  已检测 {i}/{len(file_infos)} 个文件...")

            exists, message = check_s3_object_existence(s3, S3_BUCKET_NAME, s3_key)

            if not exists:
                failed_count += 1
                timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
                log_f.write(f"{timestamp}\t{uuid}\t{filename}\t{s3_uri}\t{message}\n")
                log_f.flush() # 实时写入失败日志

    print(f"\n检测完成。总共处理 {processed_count} 个文件条目。")
    if failed_count > 0:
        print(f"发现 {failed_count} 个文件在 S3 存储桶中未找到或检测出错。详情请查看日志：{LOG_FILE_PATH}")
    else:
        print("所有在 Manifest 中列出的文件均检测到存在于 S3 存储桶中。")

if __name__ == "__main__":
    main()