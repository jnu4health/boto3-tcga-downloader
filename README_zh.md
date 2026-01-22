# TCGA 数据下载工具包

[English Documentation](./README.md)

本仓库包含用于从 AWS S3 `tcga-2-open` 存储桶下载和验证 TCGA (癌症基因组图谱) 数据的工具。

## 脚本概览

### 1. `download_tcga_boto3.py` (主要工具)
用于根据 GDC Manifest 批量下载文件的主要脚本。
*   **批量处理**：读取 GDC Manifest 文件 (TSV) 以处理多个文件。
*   **智能下载**：
    *   **预检查**：在尝试下载之前验证 S3 上是否存在文件。
    *   **验证**：下载后自动验证 MD5 校验和。
    *   **断点续传/恢复**：跳过本地已存在且 MD5 匹配的文件（默认启用）。
        *   **三层验证**：
            1. 检查 `completed_downloads.txt` 以获取先前已完成的文件（跨运行持久化）
            2. 检查本地文件是否存在
            3. 验证 MD5 校验和以确保完整性
        *   **持久化日志**：完成的下载记录在 `completed_downloads.txt` 中，即使中断也不会丢失
    *   **过滤**：可以按文件扩展名过滤下载（例如，仅下载 `.svs`）。
*   **仅检查模式**：可以验证 S3 可用性而不下载文件 (`--check-only`)。

### 2. `download_by_uuid.py` (单文件工具)
一个辅助脚本，用于在没有 manifest 的情况下通过 UUID 下载或检查特定文件/文件夹。
*   用于临时下载或检查 S3 上特定 UUID 文件夹的内容。

### 3. `generate_retry_manifest.py` (重试助手)
从日志文件中提取下载失败的文件并生成重试 manifest。
*   解析下载日志以识别失败的文件
*   创建一个仅包含失败下载的新 manifest
*   用于有针对性的重试，而无需重新处理成功的下载

## 前提条件

*   Python 3.x
*   `boto3` 库

```bash
pip install boto3
```

## 使用示例

### 1. 使用 Manifest 批量下载 (`download_tcga_boto3.py`)

**Manifest 格式**：
脚本通过标准的 GDC Manifest TSV 格式运行，包含 `id` (或 `uuid`), `filename`, `md5` 等列。
示例：
```tsv
id	filename	md5	size	state
53f2835e-e13a-4fb6-90a5-448a1a726249	b1d9364c-d703-4884-b96d-20d8084040a8.rna_seq.star_splice_junctions.tsv.gz	b05d932361f3b6309c457c5076992a04	2032798	released
eb73b4c4-3a8c-475a-bfd1-8faf77cd2a88	e3353808-13c7-4cdc-b9bf-6dbdce522f4e.wxs.VarScan2.aliquot.maf.gz	eecde67a7ffe5ea10e8a8ef060fa67d4	401565	released
```

**基本下载**：
将 manifest 中列出的所有文件下载到指定目录。
```bash
python3 download_tcga_boto3.py \
  --manifest ./manifest/gdc_manifest.txt \
  --output-base-dir ./downloads
```

**按扩展名过滤**：
仅下载 SVS 病理图像。
```bash
python3 download_tcga_boto3.py \
  --manifest ./manifest/gdc_manifest.txt \
  --output-base-dir ./downloads \
  --allowed-extensions svs
```

**恢复/跳过已存在 (默认启用)**：
跳过已经在本地下载并验证过的文件。默认启用以支持自动断点续传。
```bash
python3 download_tcga_boto3.py \
  --manifest ./manifest/gdc_manifest.txt \
  --output-base-dir ./downloads
```

**强制重新下载 (禁用恢复)**：
强制重新下载所有文件，不从先前的运行中恢复：
```bash
python3 download_tcga_boto3.py \
  --manifest ./manifest/gdc_manifest.txt \
  --output-base-dir ./downloads \
  --no-skip-existing
```

**仅检查 S3 存在性 (不下载)**：
验证 manifest 中的文件是否存在于 S3 上，而不下载它们。
```bash
python3 download_tcga_boto3.py \
  --manifest ./manifest/gdc_manifest.txt \
  --output-base-dir ./downloads \
  --check-only
```

### 2. 单个 UUID 下载 (`download_by_uuid.py`)

**下载特定 UUID**：
```bash
python3 download_by_uuid.py \
  --uuid 30e4624b-6f48-429b-b1d9-6a6bc5c82c5e \
  --output_dir ./single_downloads
```

**列出 UUID 的内容**：
如果不指定文件名，它将列出该 UUID 下的所有文件。
```bash
python3 download_by_uuid.py \
  --uuid 30e4624b-6f48-429b-b1d9-6a6bc5c82c5e \
  --output_dir ./tmp
```

### 3. 重试下载失败的文件

**方法 1: 直接从日志文件重试 (推荐 - 最快)**
```bash
# 如果你有 100 个文件，其中 5 个失败，仅重试这 5 个失败的文件
python3 download_tcga_boto3.py \
  --retry-failed-log ./downloads/download_logs/tcga_download_log_20231224_105701.tsv \
  --output-base-dir ./downloads
```

**方法 2: 生成重试 Manifest (更灵活)**
```bash
# 步骤 1: 从失败文件生成 manifest
python3 generate_retry_manifest.py \
  --log-file ./downloads/download_logs/tcga_download_log_20231224_105701.tsv \
  --output ./manifest/retry_manifest.txt

# 步骤 2: 使用重试 manifest
python3 download_tcga_boto3.py \
  --manifest ./manifest/retry_manifest.txt \
  --output-base-dir ./downloads
```

**方法 3: 简单重新运行 (可行但对大数据集较慢)**

```bash
# 仅重新运行原始命令 - 它将跳过已完成的文件
# 警告: 将重新计算所有现有文件的 MD5 (对大文件较慢)
python3 download_tcga_boto3.py \
  --manifest ./manifest/gdc_manifest.txt \
  --output-base-dir ./downloads
```

**快速恢复模式 (跳过 MD5 计算)**

```bash
# 信任 completed_downloads.txt 而不重新验证 MD5 (更快但安全性较低)
python3 download_tcga_boto3.py \
  --manifest ./manifest/gdc_manifest.txt \
  --output-base-dir ./downloads \
  --fast-resume
```

## 目录结构

`download_tcga_boto3.py` 脚本将创建以下结构：

```
/output-base-dir/
├── download_logs/
│   ├── tcga_download_log_20231224_105701.tsv
│   ├── tcga_download_log_20231224_110015.tsv
│   ├── completed_downloads.txt
│   └── failed_downloads.txt
└── tcga_dataset/
    ├── [UUID_1]/
    │   └── filename_1.svs
    ├── [UUID_2]/
    │   └── filename_2.bam
    └── ...
```

### 日志文件说明

*   **`tcga_download_log_TIMESTAMP.tsv`**: 每次运行的特定会话日志，包括以下列：
    *   `Timestamp`: 动作发生的时间
    *   `Status`: SUCCESS, SKIPPED_EXISTING, FAILED_INTEGRITY 等
    *   `UUID`, `Filename`: 文件标识
    *   `Expected_MD5`, `Actual_MD5`: 校验和验证记录
    *   `Message`: 附加详细信息

*   **`completed_downloads.txt`**: 跨运行持久存在的记录：
    *   格式: `uuid|filename|md5` (每行一个)
    *   **永不被覆盖或删除** - 只会追加
    *   启用自动断点续传：如果脚本中断并重新运行，它将跳过此处记录的任何文件

*   **`failed_downloads.txt`**: 特定于会话的失败文件列表：
    *   格式: `uuid|filename|md5|status|message` (每行一个)
    *   **每次运行覆盖**，包含当前会话的失败
    *   用于快速参考和调试

### 断点续传功能

脚本现在实现了完整的断点续传支持：

1. **自动恢复**：当你使用相同的 manifest 和输出目录重新运行脚本时，它会自动跳过：
   - 在 `completed_downloads.txt` 中标记为完成的文件
   - 本地存在且 MD5 匹配的文件
   
2. **无需手动配置**：`--skip-existing` 默认启用

3. **持久跟踪**：`completed_downloads.txt` 文件确保即使发生以下情况，跟踪也能持久存在：
   - 脚本被中断 (Ctrl+C)
   - 进程崩溃
   - 服务器断开连接
   - 多个并发运行执行 (每个都追加到同一个日志)

**示例场景**：
```bash
# 第一次运行：下载 100 个文件，完成 80 个，然后中断
nohup python3 download_tcga_boto3.py --manifest manifest.txt --output-base-dir /data > log1.log 2>&1 &

# 第二次运行：自动从第 81 个文件恢复，跳过已完成的 80 个
nohup python3 download_tcga_boto3.py --manifest manifest.txt --output-base-dir /data > log2.log 2>&1 &
```

## 性能优化

### MD5 计算成本分析

**问题**：MD5 计算对大文件是 CPU 密集型的（例如，5GB SVS 文件每个可能需要 30+ 秒）

**可用解决方案**：

1. **快速恢复模式** (`--fast-resume`)：
   - 信任 `completed_downloads.txt` 而不重新验证 MD5
   - 仅检查文件是否存在且大小不为零
   - ⚡ **快 ~100 倍**，对于有许多现有文件的大型数据集
   - ⚠️ **安全性较低**：无法检测损坏的文件

2. **直接日志重试** (`--retry-failed-log`)：
   - 仅处理在特定运行中失败的文件
   - 跳过所有成功的文件，不做任何检查
   - ⚡ **立即开始** - 无需解析 manifest 或验证
   - ✅ **推荐用于重试场景**

3. **标准模式** (默认)：
   - 对所有现有文件进行完整的 MD5 验证
   - 最安全但最慢
   - 适合首次运行或数据完整性至关重要时

### 重试策略建议

| 场景 | 推荐方法 | 命令 |
|----------|-------------------|---------|
| **下载期间网络中断** | 直接日志重试 | `--retry-failed-log log.tsv` |
| **失败后首次重试** | 直接日志重试 | `--retry-failed-log log.tsv` |
| **需要多次重试** | 生成重试 manifest | `generate_retry_manifest.py` |
| **大型数据集恢复 (可信源)** | 快速恢复 | `--fast-resume` |
| **数据完整性至关重要** | 标准模式 | (无额外标志) |

### 示例工作流

```bash
# 初始下载 (100 个文件，5 个因网络问题失败)
python3 download_tcga_boto3.py \
  --manifest manifest.txt \
  --output-base-dir /data

# 输出显示:
# ⚠️  RETRY COMMAND for failed files:
# python3 download_tcga_boto3.py \
#   --retry-failed-log /data/download_logs/tcga_download_log_20231224_105701.tsv \
#   --output-base-dir /data

# 快速重试 (仅处理 5 个失败的文件)
python3 download_tcga_boto3.py \
  --retry-failed-log /data/download_logs/tcga_download_log_20231224_105701.tsv \
  --output-base-dir /data

# 如果失败持续存在，生成 manifest 以便手动检查
python3 generate_retry_manifest.py \
  --log-file /data/download_logs/tcga_download_log_20231224_110230.tsv \
  --output /data/manifest/retry_manifest.txt
```