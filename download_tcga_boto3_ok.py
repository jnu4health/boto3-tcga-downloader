#!/usr/bin/env python3
import csv
import os
import argparse
import hashlib
import sys
import datetime
import time

try:
    import boto3
    from botocore.exceptions import (
        ClientError,
        NoCredentialsError,
        PartialCredentialsError,
    )
    from botocore.config import Config
    from botocore import UNSIGNED  # 用于 --no-sign-request
except ImportError:
    print("错误：boto3 包未找到。请先安装：pip install boto3", file=sys.stderr)
    sys.exit(1)

# TCGA 开放数据的 S3 存储桶
S3_BUCKET_OPEN = "s3://tcga-2-open"  # 脚本中会移除 's3://' 前缀用于boto3

# 默认文件名和目录
DEFAULT_OUTPUT_DIR_HELP = "/home/user/project/GDC_data"  # 示例路径，请根据实际情况修改
DEFAULT_LOG_SUBDIR = "download_logs"  # 存放主日志的子目录名
DEFAULT_DATASET_SUBDIR = "tcga_dataset"  # 存放下载的TCGA数据文件的子目录名
DEFAULT_LOG_FILE = "tcga_download_log.tsv"  # 主日志文件名


def calculate_md5(file_path, block_size=8192):
    """计算本地文件的 MD5 校验和。"""
    if not os.path.exists(file_path):
        return None, f"文件不存在: {file_path}"
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
        return None, f"读取文件时出错: {e}"
    except Exception as e:
        return None, f"计算MD5时发生未知错误: {e}"


def parse_manifest(manifest_file_path):
    """解析 GDC manifest 文件并返回文件信息列表。"""
    files_to_process = []
    col_options_id = ["id", "uuid", "file_id"]
    col_options_filename = ["filename", "file_name"]
    col_options_md5 = ["md5", "md5sum"]
    col_options_size = ["size", "file_size"]  # size 列是可选的

    try:
        with open(manifest_file_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")

            actual_col_names = {}
            if not reader.fieldnames:
                print(
                    f"错误：Manifest 文件 '{manifest_file_path}' 为空或无法读取列名。",
                    file=sys.stderr,
                )
                return None

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
                if (
                    not found and expected_col_group != "size"
                ):  # id, filename, md5 是必须的
                    print(
                        f"错误：Manifest 文件 '{manifest_file_path}' 必须包含 '{expected_col_group}' (或其变体如 {options}) 列。"
                        f"找到的列: {reader.fieldnames}",
                        file=sys.stderr,
                    )
                    return None
                elif not found and expected_col_group == "size":
                    actual_col_names["size"] = None  # size 列是可选的

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
                        f"警告：跳过 Manifest 文件的第 {row_number} 行，因为缺少 id, filename, 或 md5: {row}",
                        file=sys.stderr,
                    )
                    continue
                files_to_process.append(
                    {
                        "uuid": file_uuid,
                        "name": file_name,
                        "md5": md5_checksum.lower(),  # 确保MD5是小写以便比较
                        "size": file_size,
                    }
                )
    except FileNotFoundError:
        print(f"错误：Manifest 文件未在路径 {manifest_file_path} 找到", file=sys.stderr)
        return None
    except Exception as e:
        print(
            f"错误：解析 Manifest 文件 '{manifest_file_path}' 失败: {e}",
            file=sys.stderr,
        )
        return None
    return files_to_process


def check_s3_object_existence_for_download(s3_client, bucket_name, s3_key):
    """
    检查 S3 对象是否存在以及是否可访问。
    返回: (exists: bool, status_code_enum: int, message: str)
    status_code_enum: 0 = 存在, 1 = 未找到(404), 2 = 禁止访问(403), 3 = 其他AWS客户端错误, 4 = 未知错误
    """
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        return True, 0, "文件存在"
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        http_status_code = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        error_message_detail = str(e)  # 完整的错误信息

        if error_code == "404" or error_code == "NoSuchKey" or http_status_code == 404:
            return (
                False,
                1,
                f"文件未找到 (404 Not Found) - S3 Code: {error_code}, HTTP Status: {http_status_code}. Details: {error_message_detail}",
            )
        elif error_code == "403" or http_status_code == 403:
            return (
                False,
                2,
                f"访问被拒绝 (403 Forbidden) - S3 Code: {error_code}, HTTP Status: {http_status_code}. Details: {error_message_detail}",
            )
        else:
            # 其他类型的 ClientError
            return (
                False,
                3,
                f"AWS API 客户端错误: S3 Code: {error_code}, HTTP Status: {http_status_code}. Details: {error_message_detail}",
            )
    except Exception as e:
        # 其他非 ClientError 的异常
        return False, 4, f"检查S3对象时发生未知错误: {str(e)}"


def main():
    parser = argparse.ArgumentParser(
        description="使用 GDC Manifest 文件和 AWS SDK (boto3) 直接从 TCGA S3 存储桶下载数据。增加S3文件存在性预检查功能，并可根据文件扩展名进行过滤。",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-m", "--manifest", required=True, help="GDC Manifest 文件的路径 (TSV 格式)。"
    )
    parser.add_argument(
        "-o",
        "--output-base-dir",
        required=True,
        help=f"指定一个项目基础输出目录。日志将存放在此目录下的 '{DEFAULT_LOG_SUBDIR}/' 子目录中，TCGA 数据将下载到此目录下的 '{DEFAULT_DATASET_SUBDIR}/[UUID]/' 子目录中。例如：{DEFAULT_OUTPUT_DIR_HELP}",
    )
    parser.add_argument(
        "-e",
        "--allowed-extensions",
        type=str,
        default=None,
        help="允许下载的文件扩展名列表，以逗号分隔 (例如 'svs,bam,txt')。如果未指定，则下载所有文件。扩展名不区分大小写，不需要包含点(.)。",
    )
    parser.add_argument(
        "-b",
        "--s3-bucket",
        default=S3_BUCKET_OPEN,
        help="从中下载的 S3 存储桶 (例如 's3://bucket-name' 或 'bucket-name')。",
    )
    parser.add_argument("--aws-profile", help="用于 S3 访问的 AWS CLI profile 名称。")
    parser.add_argument(
        "--no-sign-request",
        action="store_true",
        help="下载时使用 AWS SDK 的匿名访问选项 (相当于 awscli 的 --no-sign-request)。"
        "如果 --s3-bucket 是 tcga-2-open 且未设置 --aws-profile，则此项默认启用。",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="如果目标位置已存在数据文件且其 MD5 校验和匹配，则跳过下载。",
    )
    parser.add_argument(
        "--log-file-name",
        default=DEFAULT_LOG_FILE,
        help="主日志文件的名称 (存放于日志子目录中)。",
    )
    parser.add_argument(
        "--log-subdir",
        default=DEFAULT_LOG_SUBDIR,
        help="存放主日志文件的子目录名称。",
    )
    parser.add_argument(
        "--dataset-subdir",
        default=DEFAULT_DATASET_SUBDIR,
        help="存放下载的 TCGA 数据文件的子目录名称。",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=1,  # 原为3，根据用户提供的脚本是1
        help="AWS S3 下载失败时的最大重试次数。",
    )
    parser.add_argument(
        "--retry-delay",
        type=int,
        default=2,  # 原为5，根据用户提供的脚本是2
        help="AWS S3 下载重试之间的延迟秒数。",
    )

    args = parser.parse_args()

    # 处理允许的扩展名
    allowed_extensions_set = set()
    if args.allowed_extensions:
        allowed_extensions_set = {
            ext.strip().lower().lstrip(".")
            for ext in args.allowed_extensions.split(",")
            if ext.strip()
        }
        print(f"信息：将只下载以下扩展名的文件: {', '.join(allowed_extensions_set)}")

    s3_bucket_name = args.s3_bucket.replace("s3://", "")

    use_no_sign_request_flag = args.no_sign_request
    if (
        s3_bucket_name == S3_BUCKET_OPEN.replace("s3://", "")  # 比较时不带 s3://
        and not args.aws_profile
    ):
        use_no_sign_request_flag = True
        print(
            "信息：由于目标是公开的 TCGA S3 存储桶且未指定 AWS profile，将自动启用 --no-sign-request。",
            file=sys.stdout,
        )
    if s3_bucket_name != S3_BUCKET_OPEN.replace("s3://", "") and args.no_sign_request:
        print(
            f"警告：为存储桶 '{s3_bucket_name}' 指定了 --no-sign-request。"
            "此选项通常用于公共存储桶。访问受控数据可能会失败。",
            file=sys.stderr,
        )

    initial_file_infos = parse_manifest(args.manifest)
    if not initial_file_infos:
        sys.exit(1)

    # 根据扩展名过滤文件
    file_infos_to_download = []
    skipped_due_to_extension = []

    if allowed_extensions_set:
        for file_info_item in initial_file_infos:  # 避免与外层变量重名
            file_ext = os.path.splitext(file_info_item["name"])[1].lstrip(".").lower()
            if file_ext in allowed_extensions_set:
                file_infos_to_download.append(file_info_item)
            else:
                skipped_due_to_extension.append(file_info_item)
        print(
            f"信息：根据扩展名过滤后，将尝试下载 {len(file_infos_to_download)} 个文件。"
        )
        if skipped_due_to_extension:
            print(
                f"信息：{len(skipped_due_to_extension)} 个文件因扩展名不匹配而被跳过。"
            )
    else:
        file_infos_to_download = initial_file_infos
        print(
            f"信息：未指定扩展名过滤器，将尝试下载 Manifest 中的所有 {len(initial_file_infos)} 个文件。"
        )

    project_base_dir = os.path.abspath(args.output_base_dir)
    log_actual_dir = os.path.join(project_base_dir, args.log_subdir)
    data_base_download_dir = os.path.join(project_base_dir, args.dataset_subdir)

    try:
        os.makedirs(log_actual_dir, exist_ok=True)
        os.makedirs(data_base_download_dir, exist_ok=True)
    except OSError as e:
        print(f"错误：无法创建基础输出目录或其子目录: {e}", file=sys.stderr)
        sys.exit(1)

    log_file_path = os.path.join(log_actual_dir, args.log_file_name)

    print(f"信息：数据文件将下载到: {data_base_download_dir}")
    print(f"信息：日志文件将保存在: {log_file_path}")

    s3_client_config_args = {}
    if use_no_sign_request_flag:
        s3_client_config_args["config"] = Config(signature_version=UNSIGNED)

    session_args = {}
    if args.aws_profile:
        session_args["profile_name"] = args.aws_profile

    try:
        session = boto3.Session(**session_args)
        s3_client = session.client("s3", **s3_client_config_args)
        # 测试S3连接 (可选，但有助于早期发现问题)
        s3_client.head_bucket(Bucket=s3_bucket_name)
        print(f"信息：成功连接到S3存储桶 '{s3_bucket_name}'。")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"错误：AWS凭证配置不正确或缺失: {e}", file=sys.stderr)
        if args.aws_profile:
            print(
                f"请检查您的AWS profile '{args.aws_profile}' 是否配置正确。",
                file=sys.stderr,
            )
        else:
            print(
                "请检查默认的AWS凭证 (环境变量或 ~/.aws/credentials)。", file=sys.stderr
            )
        if not use_no_sign_request_flag:  # 只有在没有使用 no_sign_request 时才提示这个
            print(
                "如果您希望匿名访问公共存储桶，请尝试使用 --no-sign-request 选项。",
                file=sys.stderr,
            )
        sys.exit(1)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchBucket":
            print(
                f"错误：S3存储桶 '{s3_bucket_name}' 不存在或无法访问。", file=sys.stderr
            )
        elif (
            e.response["Error"]["Code"] == "Forbidden" and not use_no_sign_request_flag
        ):
            print(
                f"错误：访问S3存储桶 '{s3_bucket_name}' 被拒绝 (Forbidden)。请检查凭证和存储桶策略。",
                file=sys.stderr,
            )
        else:
            print(f"错误：连接到S3或测试存储桶时发生错误: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:  # 其他初始化错误
        print(f"错误：初始化AWS S3客户端失败: {e}", file=sys.stderr)
        sys.exit(1)

    summary_stats = {
        "total_manifest": len(initial_file_infos),
        "total_filtered_for_download": len(file_infos_to_download),
        "processed_from_filtered": 0,
        "successful_md5_match": 0,
        "skipped_md5_match": 0,
        "skipped_extension_mismatch": len(skipped_due_to_extension),
        "skipped_s3_not_found": 0,  # 新增：因S3文件未找到而跳过
        "skipped_s3_forbidden": 0,  # 新增：因S3访问被拒绝而跳过
        "skipped_s3_other_error": 0,  # 新增：因S3检查时其他错误而跳过
        "failed_md5_mismatch": 0,
        "failed_aws_download": 0,
        "failed_md5_calc": 0,
        "failed_other": 0,
        "failed_files_details": [],  # 用于记录所有失败或明确跳过（S3检查失败）的条目
    }

    log_fieldnames = [
        "时间戳",
        "状态",
        "UUID",
        "文件名",
        "S3_URI",
        "本地路径",
        "预期MD5",
        "实际MD5",
        "文件大小(manifest)",
        "消息",
    ]

    with open(log_file_path, "w", newline="", encoding="utf-8") as log_f:
        log_writer = csv.DictWriter(log_f, fieldnames=log_fieldnames, delimiter="\t")
        log_writer.writeheader()

        def write_log_entry(
            status,
            uuid,
            file_name,
            s3_uri="N/A",
            local_path="N/A",
            expected_md5="N/A",
            actual_md5="N/A",
            size="N/A",
            message="",
        ):
            entry = {
                "时间戳": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "状态": status,
                "UUID": uuid,
                "文件名": file_name,
                "S3_URI": s3_uri,
                "本地路径": local_path,
                "预期MD5": expected_md5,
                "实际MD5": actual_md5 if actual_md5 else "N/A",
                "文件大小(manifest)": size,
                "消息": message,
            }
            log_writer.writerow(entry)
            log_f.flush()

        # 记录因扩展名不匹配而跳过的文件
        for file_info_item in skipped_due_to_extension:
            s3_key_skipped = f"{file_info_item['uuid']}/{file_info_item['name']}"
            s3_uri_skipped = f"s3://{s3_bucket_name}/{s3_key_skipped}"
            write_log_entry(
                status="跳过_扩展名不匹配",
                uuid=file_info_item["uuid"],
                file_name=file_info_item["name"],
                s3_uri=s3_uri_skipped,
                expected_md5=file_info_item["md5"],
                size=file_info_item["size"],
                message=f"文件扩展名 '{os.path.splitext(file_info_item['name'])[1].lstrip('.').lower()}' 不在允许的列表中 ({', '.join(allowed_extensions_set) if allowed_extensions_set else '无限制'})。",
            )

        for i, file_info in enumerate(file_infos_to_download):
            summary_stats["processed_from_filtered"] += 1
            current_file_num = i + 1
            uuid = file_info["uuid"]
            file_name = file_info["name"]
            expected_md5 = file_info["md5"]
            file_size_manifest = file_info["size"]

            s3_key = f"{uuid}/{file_name}"
            s3_uri = f"s3://{s3_bucket_name}/{s3_key}"

            target_data_uuid_dir = os.path.join(data_base_download_dir, uuid)
            local_file_path = os.path.join(target_data_uuid_dir, file_name)

            print(
                f"\n>>> 正在处理文件 {current_file_num}/{summary_stats['total_filtered_for_download']}: {file_name} (UUID: {uuid})"
            )

            # --- 新增：S3文件存在性预检查 ---
            print(f"  信息: 正在对S3对象进行预检查: {s3_uri}...")
            s3_exists, s3_check_status_code, s3_check_message = (
                check_s3_object_existence_for_download(
                    s3_client, s3_bucket_name, s3_key
                )
            )

            if not s3_exists:
                log_status = "跳过_S3检查错误"  # 默认
                reason_prefix = "S3对象预检查失败"
                if s3_check_status_code == 1:  # 404 Not Found
                    summary_stats["skipped_s3_not_found"] += 1
                    log_status = "跳过_S3文件未找到"
                    reason_prefix = "S3对象未找到"
                elif s3_check_status_code == 2:  # 403 Forbidden
                    summary_stats["skipped_s3_forbidden"] += 1
                    log_status = "跳过_S3禁止访问"
                    reason_prefix = "S3对象访问被禁止"
                else:  # 其他S3检查错误 (status_code 3 or 4)
                    summary_stats["skipped_s3_other_error"] += 1

                full_message = f"{reason_prefix}: {s3_check_message}"
                print(f"  警告: {full_message} 将跳过下载此文件。", file=sys.stderr)
                write_log_entry(
                    status=log_status,
                    uuid=uuid,
                    file_name=file_name,
                    s3_uri=s3_uri,
                    local_path=local_file_path,  # 记录计划的本地路径
                    expected_md5=expected_md5,
                    size=file_size_manifest,
                    message=full_message,
                )
                summary_stats["failed_files_details"].append(
                    {
                        "uuid": uuid,
                        "filename": file_name,
                        "status": log_status,
                        "reason": full_message,
                    }
                )
                continue  # 跳过此文件的后续下载和MD5校验
            else:
                print(f"  信息: S3对象预检查通过: {s3_check_message}")
            # --- S3文件存在性预检查结束 ---

            try:
                os.makedirs(target_data_uuid_dir, exist_ok=True)
            except OSError as e:
                message = f"创建目标目录失败: {e}"
                print(f"  错误: {message}", file=sys.stderr)
                write_log_entry(
                    "失败_其他",
                    uuid,
                    file_name,
                    s3_uri,
                    local_file_path,
                    expected_md5,
                    "N/A",
                    file_size_manifest,
                    message,
                )
                summary_stats["failed_other"] += 1
                summary_stats["failed_files_details"].append(
                    {
                        "uuid": uuid,
                        "filename": file_name,
                        "status": "失败_其他",
                        "reason": message,
                    }
                )
                continue

            if args.skip_existing and os.path.exists(local_file_path):
                print(f"  信息: 文件已存在于 {local_file_path}。正在计算本地 MD5...")
                local_md5, md5_error = calculate_md5(local_file_path)
                if md5_error:
                    message = f"计算本地MD5失败: {md5_error}"
                    print(f"  错误: {message}", file=sys.stderr)
                    write_log_entry(
                        "失败_MD5计算错误",
                        uuid,
                        file_name,
                        s3_uri,
                        local_file_path,
                        expected_md5,
                        "计算出错",
                        file_size_manifest,
                        message,
                    )
                    summary_stats["failed_md5_calc"] += 1
                    summary_stats["failed_files_details"].append(
                        {
                            "uuid": uuid,
                            "filename": file_name,
                            "status": "失败_MD5计算错误",
                            "reason": message,
                        }
                    )
                elif local_md5 == expected_md5:
                    message = "文件已存在且MD5匹配。"
                    print(f"  成功: {message} ({expected_md5})")
                    write_log_entry(
                        "已跳过_MD5匹配",
                        uuid,
                        file_name,
                        s3_uri,
                        local_file_path,
                        expected_md5,
                        local_md5,
                        file_size_manifest,
                        message,
                    )
                    summary_stats["skipped_md5_match"] += 1
                    continue
                else:
                    print(
                        f"  警告: 文件已存在但MD5不匹配 (预期: {expected_md5}, 本地: {local_md5})。将重新下载。"
                    )

            print(f"  信息: 正在从 {s3_uri} 下载到 {local_file_path}...")
            download_success = False
            last_exception_message = "未知下载错误"
            for attempt in range(args.retries + 1):  # +1 使其包括初始尝试
                try:
                    s3_client.download_file(s3_bucket_name, s3_key, local_file_path)
                    download_success = True
                    print(f"  信息: 下载完成。")
                    break
                except ClientError as e:  # AWS相关的下载错误
                    last_exception_message = (
                        f"AWS S3下载错误 (尝试 {attempt + 1}/{args.retries + 1}): {e}"
                    )
                    print(f"  错误: {last_exception_message}", file=sys.stderr)
                    if attempt < args.retries:
                        print(f"  信息: {args.retry_delay}秒后重试...")
                        time.sleep(args.retry_delay)
                    # else: # 最终失败的记录移到循环外
                except Exception as e:  # 其他非AWS的下载错误 (例如磁盘空间不足等)
                    last_exception_message = (
                        f"下载过程中发生未知错误 (尝试 {attempt + 1}): {e}"
                    )
                    print(f"  错误: {last_exception_message}", file=sys.stderr)
                    # 对于非ClientError，通常不进行重试，直接记录失败并跳出
                    write_log_entry(
                        "失败_其他",
                        uuid,
                        file_name,
                        s3_uri,
                        local_file_path,
                        expected_md5,
                        "N/A",
                        file_size_manifest,
                        last_exception_message,
                    )
                    summary_stats["failed_other"] += 1
                    summary_stats["failed_files_details"].append(
                        {
                            "uuid": uuid,
                            "filename": file_name,
                            "status": "失败_其他",
                            "reason": last_exception_message,  # 使用更新后的消息
                        }
                    )
                    download_success = False  # 确保标记为失败
                    break  # 跳出重试循环

            if not download_success:
                # 如果是因为ClientError且重试次数耗尽而失败
                if (
                    "AWS S3下载错误" in last_exception_message
                ):  # 检查是否是ClientError导致的失败
                    write_log_entry(
                        "失败_AWS下载",
                        uuid,
                        file_name,
                        s3_uri,
                        local_file_path,
                        expected_md5,
                        "N/A",
                        file_size_manifest,
                        f"AWS S3下载最终失败: {last_exception_message}",  # 使用更新后的消息
                    )
                    summary_stats["failed_aws_download"] += 1
                    summary_stats["failed_files_details"].append(
                        {
                            "uuid": uuid,
                            "filename": file_name,
                            "status": "失败_AWS下载",
                            "reason": last_exception_message,  # 使用更新后的消息
                        }
                    )
                # 如果是因为其他错误（已在循环内记录），这里不需要重复记录
                continue  # 进行下一个文件的处理

            print(f"  信息: 正在校验下载文件的 MD5...")
            actual_md5_downloaded, md5_error_downloaded = calculate_md5(local_file_path)
            if md5_error_downloaded:
                message = f"计算下载后文件MD5失败: {md5_error_downloaded}"
                print(f"  错误: {message}", file=sys.stderr)
                write_log_entry(
                    "失败_MD5计算错误",
                    uuid,
                    file_name,
                    s3_uri,
                    local_file_path,
                    expected_md5,
                    "计算出错",
                    file_size_manifest,
                    message,
                )
                summary_stats["failed_md5_calc"] += 1
                summary_stats["failed_files_details"].append(
                    {
                        "uuid": uuid,
                        "filename": file_name,
                        "status": "失败_MD5计算错误",
                        "reason": message,
                    }
                )
            elif actual_md5_downloaded == expected_md5:
                message = "下载成功且MD5匹配。"
                print(f"  成功: {message} ({actual_md5_downloaded})")
                write_log_entry(
                    "成功_MD5匹配",
                    uuid,
                    file_name,
                    s3_uri,
                    local_file_path,
                    expected_md5,
                    actual_md5_downloaded,
                    file_size_manifest,
                    message,
                )
                summary_stats["successful_md5_match"] += 1
            else:
                message = f"文件已下载但MD5不匹配 (预期: {expected_md5}, 实际: {actual_md5_downloaded})"
                print(f"  错误: {message}", file=sys.stderr)
                write_log_entry(
                    "失败_MD5不匹配",
                    uuid,
                    file_name,
                    s3_uri,
                    local_file_path,
                    expected_md5,
                    actual_md5_downloaded,
                    file_size_manifest,
                    message,
                )
                summary_stats["failed_md5_mismatch"] += 1
                summary_stats["failed_files_details"].append(
                    {
                        "uuid": uuid,
                        "filename": file_name,
                        "status": "失败_MD5不匹配",
                        "reason": message,
                    }
                )

    print("\n\n==================================================")
    print("          下载任务完成摘要")
    print("==================================================")
    print(f" Manifest 文件中声明的总文件数: {summary_stats['total_manifest']}")
    print(
        f" 根据扩展名过滤后计划下载的文件数: {summary_stats['total_filtered_for_download']}"
    )
    print(
        f" ⏭️ 因扩展名不匹配而跳过的文件数: {summary_stats['skipped_extension_mismatch']}"
    )
    print(
        f" 本次运行实际尝试处理的文件数 (来自过滤后列表): {summary_stats['processed_from_filtered']}"
    )
    print("--------------------------------------------------")
    print(f" S3预检查阶段跳过的文件:")
    print(
        f"    - ⏭️ 因S3文件未找到 (404) 而跳过: {summary_stats['skipped_s3_not_found']}"
    )
    print(
        f"    - ⏭️ 因S3访问被拒绝 (403) 而跳过: {summary_stats['skipped_s3_forbidden']}"
    )
    print(
        f"    - ⏭️ 因S3检查时其他错误而跳过: {summary_stats['skipped_s3_other_error']}"
    )
    print("--------------------------------------------------")
    print(f" ✅ 成功下载并通过校验的文件数: {summary_stats['successful_md5_match']}")
    print(
        f" ⏭️ 因本地已存在且MD5匹配而跳过的文件数: {summary_stats['skipped_md5_match']}"
    )
    print("--------------------------------------------------")

    total_download_and_md5_failures = (
        summary_stats["failed_md5_mismatch"]
        + summary_stats["failed_aws_download"]
        + summary_stats["failed_md5_calc"]
        + summary_stats["failed_other"]
    )
    print(f" ❌ 下载或MD5校验阶段失败的文件总数: {total_download_and_md5_failures}")
    if total_download_and_md5_failures > 0:
        print(f"    - MD5 不匹配数量: {summary_stats['failed_md5_mismatch']}")
        print(f"    - AWS 下载失败数量: {summary_stats['failed_aws_download']}")
        print(f"    - 本地/下载后MD5计算错误数量: {summary_stats['failed_md5_calc']}")
        print(f"    - 其他错误数量 (如目录创建失败): {summary_stats['failed_other']}")
    print("--------------------------------------------------")

    if summary_stats["failed_files_details"]:
        print("\n以下文件未能成功处理的详情 (状态 | UUID | 文件名 | 原因):")
        # 排序，可以按状态，然后按文件名
        sorted_failures_and_skips = sorted(
            summary_stats["failed_files_details"],
            key=lambda x: (x["status"], x["filename"]),
        )
        for item_info in sorted_failures_and_skips:  # 更通用的变量名
            print(
                f"  - {item_info['status']:<25} | {item_info['uuid']:<37} | {item_info['filename']:<50} | {item_info['reason']}"
            )
        print("\n请检查主日志文件获取更详细的错误信息和上下文:")
        print(f"  {log_file_path}")

    # 根据整体结果给出最终状态信息
    total_processed_successfully_or_skipped_appropriately = (
        summary_stats["successful_md5_match"]
        + summary_stats["skipped_md5_match"]
        + summary_stats["skipped_extension_mismatch"]
        + summary_stats["skipped_s3_not_found"]
        + summary_stats["skipped_s3_forbidden"]
        + summary_stats["skipped_s3_other_error"]
    )

    if (
        summary_stats["processed_from_filtered"] > 0
        and total_download_and_md5_failures == 0
        and (
            summary_stats["skipped_s3_not_found"]
            + summary_stats["skipped_s3_forbidden"]
            + summary_stats["skipped_s3_other_error"]
            == 0
        )
    ):
        # 仅当没有S3预检跳过，并且没有下载/MD5失败时，才算完美成功
        if (
            summary_stats["successful_md5_match"] + summary_stats["skipped_md5_match"]
            == summary_stats["processed_from_filtered"]
        ):
            print(
                "\n🎉 所有计划处理的文件均已成功下载/跳过（本地已存在且匹配）并通过校验。"
            )

    elif summary_stats["total_manifest"] == 0:
        print("\nℹ️ Manifest 文件为空或未找到任何有效条目。")
    elif (
        summary_stats["total_filtered_for_download"] == 0
        and summary_stats["total_manifest"] > 0
    ):
        print("\nℹ️ 根据扩展名过滤后，没有文件符合下载条件。")
    # 可以添加更多基于不同组合情况的总结性输出

    print("==================================================")
    print(f"脚本执行完毕。详细日志请查看: {log_file_path}")


if __name__ == "__main__":
    main()
