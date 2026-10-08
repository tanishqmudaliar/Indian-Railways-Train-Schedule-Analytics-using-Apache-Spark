"""
00_download.py
--------------
Downloads the Indian Railways dataset from Kaggle into data/raw/.

Tries kagglehub first; falls back to the kaggle CLI.
Kaggle credentials must already be in ~/.kaggle/kaggle.json.
"""

import os
import sys
import shutil
import glob

# ---------- paths (relative to project root) ----------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")

# Keep kagglehub cache inside the project
os.environ["KAGGLEHUB_CACHE"] = os.path.join(PROJECT_ROOT, ".cache", "kagglehub")

DATASET = "sripaadsrinivasan/indian-railways-dataset"


def download_with_kagglehub():
    """Attempt download using the kagglehub library."""
    import kagglehub
    print("Downloading with kagglehub …")
    path = kagglehub.dataset_download(DATASET)
    print(f"kagglehub downloaded to: {path}")
    return path


def download_with_cli():
    """Fallback: use the kaggle CLI to download and unzip."""
    import subprocess
    print("Downloading with kaggle CLI …")
    os.makedirs(RAW_DIR, exist_ok=True)
    subprocess.check_call([
        "kaggle", "datasets", "download",
        "-d", DATASET,
        "-p", RAW_DIR,
        "--unzip"
    ])
    return RAW_DIR


def copy_files_to_raw(src_dir):
    """
    Copy all data files from the download location into data/raw/.
    kagglehub sometimes nests them in subdirectories.
    """
    os.makedirs(RAW_DIR, exist_ok=True)

    # Walk the downloaded directory and copy every file
    for root, _dirs, files in os.walk(src_dir):
        for fname in files:
            src_path = os.path.join(root, fname)
            dst_path = os.path.join(RAW_DIR, fname)
            if not os.path.exists(dst_path):
                shutil.copy2(src_path, dst_path)
                print(f"  copied: {fname}")


def main():
    # If data/raw/ already has files, skip the download
    existing = glob.glob(os.path.join(RAW_DIR, "*"))
    if existing:
        print(f"data/raw/ already has {len(existing)} file(s); skipping download.")
        for f in existing:
            print(f"  {os.path.basename(f)}")
        return

    # Try kagglehub first, then the CLI
    try:
        src = download_with_kagglehub()
    except Exception as e:
        print(f"kagglehub failed ({e}); trying kaggle CLI …")
        try:
            src = download_with_cli()
        except Exception as e2:
            print(f"\nBoth download methods failed.")
            print(f"kagglehub error: {e}")
            print(f"CLI error: {e2}")
            print(f"\nManual steps:")
            print(f"  1. Go to https://www.kaggle.com/datasets/{DATASET}")
            print(f"  2. Download the zip file")
            print(f"  3. Unzip its contents into: {RAW_DIR}")
            sys.exit(1)

    # Copy files into data/raw/ if downloaded elsewhere
    if os.path.abspath(src) != os.path.abspath(RAW_DIR):
        copy_files_to_raw(src)

    # Show what we got
    files = os.listdir(RAW_DIR)
    print(f"\ndata/raw/ now contains {len(files)} file(s):")
    for f in files:
        size_mb = os.path.getsize(os.path.join(RAW_DIR, f)) / (1024 * 1024)
        print(f"  {f}  ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
