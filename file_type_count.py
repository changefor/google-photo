import os
import zipfile
from collections import Counter

SOURCE_DIR = r"source"  # ← 改成你的來源資料夾

outer_ext_counter = Counter()
zip_inner_ext_counter = Counter()
zip_error_files = []

def get_extension(filename):
    if "." not in filename:
        return "(no extension)"
    return filename.rsplit(".", 1)[-1].lower()

for root, _, files in os.walk(SOURCE_DIR):
    for name in files:
        path = os.path.join(root, name)
        ext = get_extension(name)
        outer_ext_counter[ext] += 1

        if ext == "zip":
            try:
                with zipfile.ZipFile(path, "r") as z:
                    for zinfo in z.infolist():
                        if zinfo.is_dir():
                            continue
                        inner_name = os.path.basename(zinfo.filename)
                        if not inner_name:
                            continue
                        inner_ext = get_extension(inner_name)
                        zip_inner_ext_counter[inner_ext] += 1
            except Exception as e:
                zip_error_files.append(f"{path} | {e}")

print("\n=== 外層檔案副檔名統計 ===")
for ext, count in outer_ext_counter.most_common():
    print(f"{ext:15s} {count}")

print("\n=== ZIP 內部檔案副檔名統計 ===")
for ext, count in zip_inner_ext_counter.most_common():
    print(f"{ext:15s} {count}")

if zip_error_files:
    print("\n=== 無法讀取的 ZIP 檔案 ===")
    for line in zip_error_files:
        print(line)
