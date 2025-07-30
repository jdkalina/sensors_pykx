import os
import sys
import shutil
from pathlib import Path

source_drive = Path("topykx/2025.04.29/")
dest_dir = Path("data/kdb/2025.04.29/")

if not source_drive.exists():
    print(f"Error: {source_drive} not found")
    sys.exit(1)

print(f"Copying from {source_drive} to {dest_dir}...")

for item in source_drive.iterdir():
    if (item.name.endswith(".0") or item.name.endswith(".1")):
        dest_path = dest_dir / item.name[:-2]
    else:
        dest_path = dest_dir / item.name
    if item.is_dir():
        shutil.copytree(item, dest_path, dirs_exist_ok=True)
    else:
        shutil.copy2(item, dest_path)

print(f"Done! Data copied to {dest_dir}")
