# TCGA Data Download Tools

这是一个用于从 AWS S3 (`tcga-2-open`) 下载和校验 TCGA (The Cancer Genome Atlas) 数据的 Python 工具集。

## 脚本列表与功能说明

当前目录下包含以下 4 个 Python 脚本：

### 1. `download_tcga_boto3_ok.py` (推荐使用)
**功能**: 功能最全、最稳健的数据下载器。
*   **核心功能**: 根据 GDC Manifest 文件从 S3 下载数据。
*   **主要特性**:
    *   支持 CLI 命令行参数配置。
    *   **S3 预检查**: 下载前会自动检查 S3 上文件是否存在 (404) 或是否有权限 (403)，避免无效的下载尝试。
    *   **扩展名过滤**: 支持通过 `--allowed-extensions` 参数只下载特定类型的文件 (如 `svs`, `bam`)。
    *   **断点/跳过**: 支持 `--skip-existing`，如果本地文件存在且 MD5 校验通过则跳过。
    *   **完整校验**: 下载后自动计算并比对 MD5。
    *   **详细日志**: 生成详细的 TSV 格式日志文件。

### 2. `download_tcga_boto3_02.py`
**功能**: `download_tcga_boto3_ok.py` 的前一个版本。
*   **区别**: 相比 `_ok` 版本，缺少了下载前的 "S3 对象存在性预检查" 步骤。其他功能（如扩展名过滤、MD5 校验）基本一致。

### 3. `detect_tcga_boto3.py`
**功能**: 纯检测工具（不下载）。
*   **用途**: 用于在不下载任何文件的情况下，快速扫描 Manifest 中的文件在 S3 存储桶中是否存在。
*   **注意**: **此脚本配置是硬编码的**。使用前**必须**用编辑器打开脚本，修改 `MANIFEST_FILE_PATH` 和 `LOG_FILE_PATH` 变量。

### 4. `download_tcga_boto3.py`
**功能**: 基础版下载器。
*   **区别**: 最早期的版本，具备基础的下载和 MD5 校验功能，但可能缺乏扩展名过滤等高级特性。建议优先使用 `download_tcga_boto3_ok.py`。

---

## 环境准备

所有脚本均依赖 `boto3` 库。如果尚未安装，请运行：

```bash
pip install boto3
```

---

## 使用指南

### 场景一：下载数据 (推荐)

使用 `download_tcga_boto3_ok.py` 进行下载。

**基本用法**:
```bash
python3 download_tcga_boto3_ok.py \
  -m /path/to/your/gdc_manifest.txt \
  -o /path/to/output_directory
```

**只下载特定文件 (例如 SVS 格式)**:
```bash
python3 download_tcga_boto3_ok.py \
  -m ./gdc_manifest.txt \
  -o ./downloads \
  -e svs
```

**跳过已存在的文件 (断点续传)**:
```bash
python3 download_tcga_boto3_ok.py \
  -m ./gdc_manifest.txt \
  -o ./downloads \
  --skip-existing
```

**查看帮助**:
```bash
python3 download_tcga_boto3_ok.py --help
```

### 场景二：仅检测文件是否存在

使用 `detect_tcga_boto3.py`。

1.  使用编辑器打开文件：`vim detect_tcga_boto3.py`
2.  修改以下行以匹配你的文件路径：
    ```python
    MANIFEST_FILE_PATH = "/path/to/your/manifest.txt"
    LOG_FILE_PATH = "/path/to/your/logfile.log"
    ```
3.  运行脚本：
    ```bash
    python3 detect_tcga_boto3.py
    ```

---

## 输出目录结构

下载脚本会自动创建以下目录结构：

```
/path/to/output_directory/
├── download_logs/          # 存放运行日志 (.tsv)
│   └── tcga_download_log.tsv
└── tcga_dataset/           # 存放实际数据
    ├── [UUID_1]/
    │   └── filename_1.svs
    ├── [UUID_2]/
    │   └── filename_2.bam
    └── ...
```




nohup python3 download_by_uuid.py \
  --uuid "30e4624b-6f48-429b-b1d9-6a6bc5c82c5e" \
  --output_dir "/home/huozh/demo/code-project/A-DataSet/Pathological_Dataset/HookNet-TLS/tls_svs" \
  > download_log.txt 2>&1 &
