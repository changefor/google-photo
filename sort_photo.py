import os
import zipfile
import json
import shutil
import hashlib
import re
import io
import contextlib
from datetime import datetime
import exifread

# ====== PATH SETTING ======
SOURCE_DIR = "source"
TARGET_DIR = "photo"
TMP_DIR = "/tmp/takeout_tmp"
UNCLASSIFIED_DIR = os.path.join(TARGET_DIR, "unclassified")

# ====== OUTPUT FILES ======
HASH_DB = os.path.join(TARGET_DIR, "hash_index.json")
DUP_FILE = os.path.join(TARGET_DIR, "duplicate_files.txt")
UNCLASS_FILE = os.path.join(TARGET_DIR, "unclassified_files.txt")
FMT_ERR_FILE = os.path.join(TARGET_DIR, "file_format_not_recognized.txt")
CORRUPT_FILE = os.path.join(TARGET_DIR, "corrupted_exif_files.txt")
FILENAME_FALLBACK_FILE = os.path.join(TARGET_DIR, "used_filename_fallback.txt")

# ====== MEMORY ======
hash_index = {}
duplicate_files = []
unclassified_files = []
file_format_errors = []
corrupted_exif_files = []
filename_fallback_files = []

MEDIA_EXT = (".jpg", ".jpeg", ".png", ".mp4", ".mov")


# ====== UTIL ======
def file_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for c in iter(lambda: f.read(8192), b""):
            h.update(c)
    return h.hexdigest()


def safe_copy(src, dest_dir):
    os.makedirs(dest_dir, exist_ok=True)
    base = os.path.basename(src)
    name, ext = os.path.splitext(base)
    dest = os.path.join(dest_dir, base)
    i = 1
    while os.path.exists(dest):
        dest = os.path.join(dest_dir, f"{name}_{i}{ext}")
        i += 1
    shutil.copy2(src, dest)
    return dest


# ====== DATE FROM FILENAME ======
def datetime_from_filename(filename):
    patterns = [
        r"(20\d{2})(\d{2})(\d{2})",
        r"(20\d{2})[-_](\d{2})[-_](\d{2})",
        r"(20\d{2})(\d{2})(\d{2})[_-]\d{6}",
    ]
    for p in patterns:
        m = re.search(p, filename)
        if m:
            try:
                y, mo, d = map(int, m.groups()[:3])
                return datetime(y, mo, d)
            except ValueError:
                pass
    return None


# ====== EXIF ======
def read_exif(path):
    try:
        with open(path, "rb") as f:
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                tags = exifread.process_file(f, details=False)

        err = stderr.getvalue()
        if "File format not recognized" in err:
            file_format_errors.append(path)
            return None

        if "Possibly corrupted" in err:
            corrupted_exif_files.append(path)

        dt = tags.get("EXIF DateTimeOriginal")
        return datetime.strptime(str(dt), "%Y:%m:%d %H:%M:%S") if dt else None
    except Exception as e:
        corrupted_exif_files.append(f"{path} | {e}")
        return None


# ====== METADATA.JSON ======
def read_metadata_json(path):
    jp = path + ".json"
    if not os.path.exists(jp):
        return None
    try:
        with open(jp, "r", encoding="utf-8") as f:
            data = json.load(f)
        ts = data.get("photoTakenTime", {}).get("timestamp")
        return datetime.fromtimestamp(int(ts)) if ts else None
    except Exception as e:
        corrupted_exif_files.append(f"{jp} | {e}")
        return None


# ====== DATE RESOLVER ======
def resolve_datetime(path):
    dt = read_exif(path)
    if dt:
        return dt

    dt = read_metadata_json(path)
    if dt:
        return dt

    dt = datetime_from_filename(os.path.basename(path))
    if dt:
        filename_fallback_files.append(path)
        return dt

    try:
        return datetime.fromtimestamp(os.path.getmtime(path))
    except Exception:
        return None


# ====== PROCESS MEDIA ======
def process_media(path):
    h = file_hash(path)
    if h in hash_index:
        duplicate_files.append(f"{path} -> {hash_index[h]}")
        return

    dt = resolve_datetime(path)
    if not dt:
        unclassified_files.append(path)
        dest = safe_copy(path, UNCLASSIFIED_DIR)
        hash_index[h] = dest
        return

    folder = os.path.join(
        TARGET_DIR, str(dt.year), f"{dt.month:02}", f"{dt.day:02}"
    )
    dest = safe_copy(path, folder)
    hash_index[h] = dest


def process_zip(zip_path):
    with zipfile.ZipFile(zip_path, "r") as z:
        for name in z.namelist():
            if name.lower().endswith(MEDIA_EXT):
                extracted = z.extract(name, TMP_DIR)
                process_media(extracted)


# ====== MAIN ======
def main():
    os.makedirs(TARGET_DIR, exist_ok=True)
    os.makedirs(TMP_DIR, exist_ok=True)

    if os.path.exists(HASH_DB):
        with open(HASH_DB, "r", encoding="utf-8") as f:
            hash_index.update(json.load(f))

    for root, _, files in os.walk(SOURCE_DIR):
        for f in files:
            path = os.path.join(root, f)
            try:
                if f.lower().endswith(".zip"):
                    process_zip(path)
                elif f.lower().endswith(MEDIA_EXT):
                    process_media(path)
            except Exception as e:
                corrupted_exif_files.append(f"{path} | UNKNOWN | {e}")

    with open(HASH_DB, "w", encoding="utf-8") as f:
        json.dump(hash_index, f, indent=2)

    def dump(p, data):
        with open(p, "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(set(data))))

    dump(DUP_FILE, duplicate_files)
    dump(UNCLASS_FILE, unclassified_files)
    dump(FMT_ERR_FILE, file_format_errors)
    dump(CORRUPT_FILE, corrupted_exif_files)
    dump(FILENAME_FALLBACK_FILE, filename_fallback_files)

    print("✅ Done")


if __name__ == "__main__":
    main()
