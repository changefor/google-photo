import os
import zipfile
import json
import shutil
import hashlib
import re
import io
import contextlib
import subprocess
import requests
from datetime import datetime
import exifread
from collections import defaultdict

# ========= PATH =========
SOURCE_DIR = "source"
TARGET_DIR = "final"
TMP_DIR = "/tmp/takeout_tmp"
UNCLASSIFIED_DIR = os.path.join(TARGET_DIR, "unclassified")

# ========= FILES =========
HASH_DB = os.path.join(TARGET_DIR, "hash_index.json")
DUP_FILE = os.path.join(TARGET_DIR, "duplicate_files.txt")
UNCLASS_FILE = os.path.join(TARGET_DIR, "unclassified_files.txt")
FMT_ERR_FILE = os.path.join(TARGET_DIR, "file_format_not_recognized.txt")
CORRUPT_FILE = os.path.join(TARGET_DIR, "corrupted_exif_files.txt")
FILENAME_FALLBACK_FILE = os.path.join(TARGET_DIR, "used_filename_fallback.txt")
NO_LOC_FILE = os.path.join(TARGET_DIR, "no_location_list.txt")

MEDIA_EXT = (".jpg", ".jpeg", ".png", ".mp4", ".mov")

# ========= MEMORY =========
hash_index = {}
duplicate_files = []
unclassified_files = []
file_format_errors = []
corrupted_exif_files = []
filename_fallback_files = []
no_loc_files = []

gps_by_day = defaultdict(list)   # YYYY/MM/DD -> [(lat, lon)]

# ========= UTIL =========
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


# ========= DATE FROM FILENAME =========
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


# ========= EXIF =========
def read_exif(path):
    try:
        with open(path, "rb") as f:
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                tags = exifread.process_file(f, details=False)

        err = stderr.getvalue()
        if "File format not recognized" in err:
            file_format_errors.append(path)
            return None, None

        if "Possibly corrupted" in err:
            corrupted_exif_files.append(path)

        dt = tags.get("EXIF DateTimeOriginal")
        gps_lat = tags.get("GPS GPSLatitude")
        gps_lon = tags.get("GPS GPSLongitude")

        dt_val = datetime.strptime(str(dt), "%Y:%m:%d %H:%M:%S") if dt else None
        return dt_val, (gps_lat, gps_lon) if gps_lat and gps_lon else None
    except Exception as e:
        corrupted_exif_files.append(f"{path} | {e}")
        return None, None


# ========= METADATA.JSON =========
def read_metadata_json(path):
    jp = path + ".json"
    if not os.path.exists(jp):
        return None, None
    try:
        with open(jp, "r", encoding="utf-8") as f:
            data = json.load(f)

        ts = data.get("photoTakenTime", {}).get("timestamp")
        geo = data.get("geoData") or data.get("geoDataExif")

        dt = datetime.fromtimestamp(int(ts)) if ts else None
        loc = (geo["latitude"], geo["longitude"]) if geo and geo.get("latitude") else None
        return dt, loc
    except Exception as e:
        corrupted_exif_files.append(f"{jp} | {e}")
        return None, None


# ========= VIDEO TIME =========
def read_video_time(path):
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "quiet", "-show_entries",
             "format_tags=creation_time",
             "-of", "default=noprint_wrappers=1:nokey=1", path],
            capture_output=True, text=True
        )
        if r.stdout.strip():
            return datetime.fromisoformat(r.stdout.strip().replace("Z", "+00:00"))
    except Exception:
        pass
    return None


# ========= DATE + GPS =========
def resolve_datetime_and_gps(path):
    dt, gps = read_exif(path)
    if dt:
        return dt, gps

    dt, gps = read_metadata_json(path)
    if dt:
        return dt, gps

    if path.lower().endswith((".mp4", ".mov")):
        dt = read_video_time(path)
        if dt:
            return dt, None

    dt = datetime_from_filename(os.path.basename(path))
    if dt:
        filename_fallback_files.append(path)
        return dt, None

    try:
        return datetime.fromtimestamp(os.path.getmtime(path)), None
    except Exception:
        return None, None


# ========= GEO =========
def reverse_geocode(lat, lon):
    try:
        print(lat)
        print(lon)
        r = requests.get(
            "https://nominatim.openstreetmap.org/reverse",
            params={
                "lat": lat,
                "lon": lon,
                "format": "json",
                "zoom": 10,
            },
            headers={"User-Agent": "GoogleTakeoutOrganizer/1.0"},
            timeout=10,
        )
        a = r.json().get("address", {})
        print(r.json)
        city = a.get("city") or a.get("town") or a.get("county")
        country = a.get("country")
        return ", ".join(filter(None, [city, country]))
    except Exception:
        return None


# ========= PROCESS =========
def process_media(path):
    h = file_hash(path)
    if h in hash_index:
        duplicate_files.append(f"{path} -> {hash_index[h]}")
        return

    dt, gps = resolve_datetime_and_gps(path)
    if not dt:
        unclassified_files.append(path)
        dest = safe_copy(path, UNCLASSIFIED_DIR)
        hash_index[h] = dest
        return

    folder = os.path.join(TARGET_DIR, str(dt.year), f"{dt.month:02}", f"{dt.day:02}")
    dest = safe_copy(path, folder)
    hash_index[h] = dest

    if gps:
        gps_by_day[folder].append(gps)
        #print(path)
    else:
        no_loc_files.append(path)


def process_zip(zip_path):
    with zipfile.ZipFile(zip_path, "r") as z:
        for name in z.namelist():
            if name.lower().endswith(MEDIA_EXT):
                process_media(z.extract(name, TMP_DIR))


# ========= LOCATION.TXT =========
def write_location_files():
    for folder, gps_list in gps_by_day.items():
        loc_count = defaultdict(int)
        for lat, lon in gps_list:
            print(lat.values, lon)
            decimal_lat = convert_to_decimal(lat.values)
            decimal_lon = convert_to_decimal(lon.values)
            loc = reverse_geocode(decimal_lat, decimal_lon)
            place = reverse_geocode(decimal_lat, decimal_lon)
            if place:
                loc_count[place] += 1

        if not loc_count:
            continue

        total = sum(loc_count.values())
        main = max(loc_count.items(), key=lambda x: x[1])

        with open(os.path.join(folder, "location.txt"), "w", encoding="utf-8") as f:
            f.write("主要拍攝地點：\n")
            f.write(f"{main[0]}\n\n")
            f.write("涵蓋地點：\n")
            for k, v in sorted(loc_count.items(), key=lambda x: -x[1]):
                f.write(f"- {k} ({v})\n")

########## convert lat lon to decimal
def convert_to_decimal(dms_list):
    degrees = float(dms_list[0])
    minutes = float(dms_list[1])
    # 處理分數形式的秒數，例如 1899/100
    seconds = float(dms_list[2])

    return degrees + (minutes / 60.0) + (seconds / 3600.0)

# ========= MAIN =========
def main():
    os.makedirs(TARGET_DIR, exist_ok=True)
    os.makedirs(TMP_DIR, exist_ok=True)

    if os.path.exists(HASH_DB):
        with open(HASH_DB, "r", encoding="utf-8") as f:
            hash_index.update(json.load(f))

    for root, _, files in os.walk(SOURCE_DIR):
        for f in files:
            p = os.path.join(root, f)
            try:
                if f.lower().endswith(".zip"):
                    process_zip(p)
                elif f.lower().endswith(MEDIA_EXT):
                    process_media(p)
            except Exception as e:
                corrupted_exif_files.append(f"{p} | {e}")

    write_location_files()

    def dump(path, data):
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(set(data))))

    dump(DUP_FILE, duplicate_files)
    dump(UNCLASS_FILE, unclassified_files)
    dump(FMT_ERR_FILE, file_format_errors)
    dump(CORRUPT_FILE, corrupted_exif_files)
    dump(FILENAME_FALLBACK_FILE, filename_fallback_files)
    dump(NO_LOC_FILE, no_loc_files)

    with open(HASH_DB, "w", encoding="utf-8") as f:
        json.dump(hash_index, f, indent=2)

    print("✅ All done")


if __name__ == "__main__":
    main()