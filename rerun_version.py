import os
import zipfile
import json
import shutil
import hashlib
import subprocess
import time
from collections import defaultdict
from datetime import datetime
from PIL import Image
import reverse_geocoder as rg
import folium

# =========================
# CONFIG
# =========================
SOURCE_DIR = r"source"
TMP_DIR = r"C:\tmp\photo_staging"
TARGET_DIR = r"final"
THUMB_DIR = os.path.join(TARGET_DIR, "thumbnails")

FFMPEG = r"C:\Users\user\Desktop\python\ffmpeg\bin\ffmpeg.exe"

MEDIA_EXT = (
    ".jpg", ".jpeg", ".png", ".mp4", ".mov", ".heic", ".heif",
    ".avi", ".mkv", ".wmv", ".3gp", ".gif", ".tiff", ".webp", ".cr2"
)

HASH_DB = os.path.join(TARGET_DIR, "processed_hashes.txt")
YEAR_POINTS_JSON = os.path.join(TARGET_DIR, "year_points.json")
UNCLASSIFIED_JSON = os.path.join(TARGET_DIR, "unclassified_points.json")

LOG_DIR = os.path.join(TARGET_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# =========================
# GLOBAL STATE (累積)
# =========================
processed_hashes = set()
yearly_locations = defaultdict(list)
unclassified_locations = []

# =========================
# UTIL
# =========================
def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()

def clear_tmp():
    if os.path.exists(TMP_DIR):
        shutil.rmtree(TMP_DIR)
    os.makedirs(TMP_DIR, exist_ok=True)

def safe_copy(src, dst):
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    if not os.path.exists(dst):
        shutil.copy2(src, dst)
    return dst

def load_hash_db():
    if os.path.exists(HASH_DB):
        with open(HASH_DB, "r") as f:
            for line in f:
                processed_hashes.add(line.strip())

def save_hash_db():
    with open(HASH_DB, "w") as f:
        for h in sorted(processed_hashes):
            f.write(h + "\n")

def load_year_points():
    if os.path.exists(YEAR_POINTS_JSON):
        with open(YEAR_POINTS_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
            for y, pts in data.items():
                yearly_locations[y].extend(pts)

def save_year_points():
    with open(YEAR_POINTS_JSON, "w", encoding="utf-8") as f:
        json.dump(yearly_locations, f, ensure_ascii=False, indent=2)

def load_unclassified():
    if os.path.exists(UNCLASSIFIED_JSON):
        with open(UNCLASSIFIED_JSON, "r", encoding="utf-8") as f:
            unclassified_locations.extend(json.load(f))

def save_unclassified():
    with open(UNCLASSIFIED_JSON, "w", encoding="utf-8") as f:
        json.dump(unclassified_locations, f, ensure_ascii=False, indent=2)

# =========================
# METADATA
# =========================
def get_video_time(path):
    try:
        cmd = [
            FFMPEG, "-i", path,
            "-show_entries", "format_tags=creation_time",
            "-v", "quiet", "-print_format", "json"
        ]
        p = subprocess.run(cmd, capture_output=True, text=True)
        data = json.loads(p.stdout)
        return data.get("format", {}).get("tags", {}).get("creation_time")
    except:
        return None

def parse_time(s):
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.fromisoformat(s.replace("Z", ""))
        except:
            pass
    return None

# =========================
# GEO
# =========================
def reverse_geo(lat, lon):
    try:
        res = rg.search((lat, lon), mode=1)[0]
        return res.get("name"), res.get("cc")
    except:
        return None, None

# =========================
# MAP
# =========================
def generate_map(points, map_path, title):
    if not points:
        return
    m = folium.Map(location=[points[0]["lat"], points[0]["lon"]], zoom_start=6)
    for p in points:
        thumb = os.path.relpath(p["thumb"], os.path.dirname(map_path))
        popup = f"""
        <b>{p.get('city','')}</b><br>
        <img src="{thumb}" width="150">
        """
        folium.Marker(
            [p["lat"], p["lon"]],
            popup=popup
        ).add_to(m)
    m.save(map_path)

# =========================
# MAIN PROCESS
# =========================
def process_media(path):
    file_hash = sha256(path)
    if file_hash in processed_hashes:
        return

    processed_hashes.add(file_hash)

    dt = None
    gps = None

    ext = os.path.splitext(path)[1].lower()

    if ext in (".mp4", ".mov", ".avi", ".mkv"):
        dt = parse_time(get_video_time(path))

    if not dt:
        try:
            dt = datetime.fromtimestamp(os.path.getmtime(path))
        except:
            pass

    if not dt:
        dest_dir = os.path.join(TARGET_DIR, "unclassified")
    else:
        dest_dir = os.path.join(
            TARGET_DIR,
            str(dt.year),
            f"{dt.month:02d}",
            f"{dt.day:02d}"
        )

    dest_path = safe_copy(path, os.path.join(dest_dir, os.path.basename(path)))

    thumb_path = os.path.join(THUMB_DIR, file_hash + ".jpg")
    if not os.path.exists(thumb_path) and ext in (".jpg", ".jpeg", ".png"):
        try:
            img = Image.open(path)
            img.thumbnail((300, 300))
            img.save(thumb_path, "JPEG")
        except:
            pass

    if gps and dt:
        city, country = reverse_geo(*gps)
        yearly_locations[str(dt.year)].append({
            "lat": gps[0],
            "lon": gps[1],
            "city": city,
            "country": country,
            "thumb": thumb_path
        })
    elif gps:
        city, country = reverse_geo(*gps)
        unclassified_locations.append({
            "lat": gps[0],
            "lon": gps[1],
            "city": city,
            "country": country,
            "thumb": thumb_path
        })

# =========================
# ENTRY
# =========================
def main():
    start = time.time()

    clear_tmp()
    load_hash_db()
    load_year_points()
    load_unclassified()

    for root, _, files in os.walk(SOURCE_DIR):
        for name in files:
            src = os.path.join(root, name)
            ext = os.path.splitext(name)[1].lower()

            if ext == ".zip":
                with zipfile.ZipFile(src) as z:
                    z.extractall(TMP_DIR)
            elif ext in MEDIA_EXT:
                shutil.copy2(src, TMP_DIR)

    for root, _, files in os.walk(TMP_DIR):
        for name in files:
            if os.path.splitext(name)[1].lower() in MEDIA_EXT:
                process_media(os.path.join(root, name))

    save_hash_db()
    save_year_points()
    save_unclassified()

    for year, pts in yearly_locations.items():
        year_dir = os.path.join(TARGET_DIR, year)
        os.makedirs(year_dir, exist_ok=True)
        generate_map(pts, os.path.join(year_dir, "map.html"), year)

    if unclassified_locations:
        generate_map(
            unclassified_locations,
            os.path.join(TARGET_DIR, "unclassified", "map.html"),
            "unclassified"
        )

    print(f"Finished in {time.time() - start:.1f}s")

if __name__ == "__main__":
    main()
