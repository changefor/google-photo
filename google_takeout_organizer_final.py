import os
import zipfile
import json
import shutil
import re
import io
import contextlib
import subprocess
from datetime import datetime
from collections import defaultdict
import exifread
import reverse_geocoder as rg

# ================= CONFIG =================
SOURCE_DIR = r"source"
TARGET_DIR = r"final"
TMP_DIR = r"C:\tmp\TakeoutTmp"

MEDIA_EXT = (".jpg", ".jpeg", ".png", ".mp4", ".mov")

# ================= GEO =================
GEO_CACHE_FILE = os.path.join(TARGET_DIR, "geo_cache.json")
geo_cache = {}
rg_engine = rg.RGeocoder(mode=1, verbose=False)

# ================= LOGS =================
file_format_errors = []
corrupted_exif_files = []
unclassified_files = []
filename_fallback_files = []
reverse_geocode_failed = []
ffprobe_failed = []

# ================= STATS =================
gps_points = []  # for HTML map
gps_by_day = defaultdict(set)
year_city_count = defaultdict(lambda: defaultdict(int))
city_total_count = defaultdict(int)

# ================= UTIL =================
def load_geo_cache():
    global geo_cache
    if os.path.exists(GEO_CACHE_FILE):
        with open(GEO_CACHE_FILE, "r", encoding="utf-8") as f:
            geo_cache = json.load(f)

def save_geo_cache():
    with open(GEO_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(geo_cache, f, ensure_ascii=False, indent=2)

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

def datetime_from_filename(filename):
    m = re.search(r"(20\d{2})[-_]?(\d{2})[-_]?(\d{2})", filename)
    if m:
        try:
            return datetime(int(m[1]), int(m[2]), int(m[3]))
        except:
            pass
    return None

# ================= EXIF =================
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
        dt_val = datetime.strptime(str(dt), "%Y:%m:%d %H:%M:%S") if dt else None

        lat = tags.get("GPS GPSLatitude")
        lon = tags.get("GPS GPSLongitude")
        lat_ref = tags.get("GPS GPSLatitudeRef")
        lon_ref = tags.get("GPS GPSLongitudeRef")

        if lat and lon and lat_ref and lon_ref:
            def conv(v):
                d, m, s = [float(x.num) / float(x.den) for x in v.values]
                return d + m / 60 + s / 3600
            latitude = conv(lat)
            longitude = conv(lon)
            if lat_ref.values != "N":
                latitude = -latitude
            if lon_ref.values != "E":
                longitude = -longitude
            return dt_val, (latitude, longitude)

        return dt_val, None
    except Exception as e:
        corrupted_exif_files.append(f"{path} | {e}")
        return None, None

# ================= METADATA.JSON =================
def read_metadata_json(path):
    jp = path + ".json"
    if not os.path.exists(jp):
        return None, None
    try:
        with open(jp, encoding="utf-8") as f:
            data = json.load(f)
        ts = data.get("photoTakenTime", {}).get("timestamp")
        geo = data.get("geoData") or data.get("geoDataExif")
        dt = datetime.fromtimestamp(int(ts)) if ts else None
        gps = (geo["latitude"], geo["longitude"]) if geo and geo.get("latitude") else None
        return dt, gps
    except Exception as e:
        corrupted_exif_files.append(f"{jp} | {e}")
        return None, None

# ================= VIDEO =================
def read_video_time(path):
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "quiet",
             "-show_entries", "format_tags=creation_time",
             "-of", "default=noprint_wrappers=1:nokey=1", path],
            capture_output=True, text=True
        )
        if r.stdout.strip():
            return datetime.fromisoformat(r.stdout.strip().replace("Z", "+00:00"))
    except:
        pass
    ffprobe_failed.append(path)
    return None

# ================= RESOLVE =================
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
    except:
        unclassified_files.append(path)
        return None, None

# ================= OFFLINE GEO =================
def reverse_geocode_city_offline(lat, lon):
    # 過濾假 GPS
    if abs(lat) < 0.001 and abs(lon) < 0.001:
        reverse_geocode_failed.append(f"{lat},{lon} zero")
        return None

    key = f"{round(lat,4)},{round(lon,4)}"
    if key in geo_cache:
        return tuple(geo_cache[key]) if geo_cache[key] else None

    try:
        # ⚠️ 一定要用 list 包起來
        results = rg.search([(lat, lon)], mode=1)
        if not results:
            reverse_geocode_failed.append(f"{lat},{lon} no result")
            geo_cache[key] = None
            return None

        r = results[0]
        city = r.get("name")
        country = r.get("cc")

        place = (city, country) if city and country else None
        geo_cache[key] = list(place) if place else None
        return place

    except Exception as e:
        reverse_geocode_failed.append(f"{lat},{lon} | {e}")
        geo_cache[key] = None
        return None

# ================= PROCESS =================
def process_media(path):
    dt, gps = resolve_datetime_and_gps(path)
    if not dt:
        return

    day_dir = os.path.join(TARGET_DIR, str(dt.year), f"{dt.month:02}", f"{dt.day:02}")
    safe_copy(path, day_dir)

    if not gps:
        return

    place = reverse_geocode_city_offline(*gps)
    if not place:
        return

    gps_points.append((gps[0], gps[1], dt.strftime("%Y-%m-%d"), place))
    gps_by_day[day_dir].add(place)
    year_city_count[dt.year][place] += 1
    city_total_count[place] += 1

# ================= HTML MAP =================
def write_html_map():
    html = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Photo Map</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet/dist/leaflet.js"></script>
<style>#map{height:100vh;}</style>
</head>
<body>
<div id="map"></div>
<script>
var map = L.map('map').setView([25, 121], 4);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png').addTo(map);
"""
    for lat, lon, date, (city, country) in gps_points:
        html += f"""
L.marker([{lat},{lon}]).addTo(map)
 .bindPopup("{date}<br>{city}, {country}");
"""
    html += "</script></body></html>"

    with open(os.path.join(TARGET_DIR, "photo_map.html"), "w", encoding="utf-8") as f:
        f.write(html)

# ================= REPORT =================
def write_reports():
    with open(os.path.join(TARGET_DIR, "city_ranking.txt"), "w", encoding="utf-8") as f:
        for i, ((c, ct), cnt) in enumerate(sorted(city_total_count.items(), key=lambda x: -x[1]), 1):
            f.write(f"{i}. {c}, {ct} : {cnt}\n")

    with open(os.path.join(TARGET_DIR, "year_location_summary.txt"), "w", encoding="utf-8") as f:
        for y, cities in sorted(year_city_count.items()):
            f.write(f"{y}\n")
            for (c, ct), cnt in sorted(cities.items(), key=lambda x: -x[1]):
                f.write(f"  {c}, {ct} : {cnt}\n")
            f.write("\n")

    with open(os.path.join(TARGET_DIR, "location_summary.txt"), "w", encoding="utf-8") as f:
        for d, places in sorted(gps_by_day.items()):
            date = d.replace(TARGET_DIR + os.sep, "").replace(os.sep, "/")
            f.write(date + " : " + " | ".join(f"{c}, {ct}" for c, ct in places) + "\n")

    for name, data in [
        ("reverse_geocode_failed.txt", reverse_geocode_failed),
        ("file_format_not_recognized.txt", file_format_errors),
        ("corrupted_exif_files.txt", corrupted_exif_files),
        ("unclassified_files.txt", unclassified_files),
        ("used_filename_fallback.txt", filename_fallback_files),
        ("ffprobe_failed.txt", ffprobe_failed),
    ]:
        with open(os.path.join(TARGET_DIR, name), "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(set(data))))

# ================= MAIN =================
def main():
    os.makedirs(TARGET_DIR, exist_ok=True)
    os.makedirs(TMP_DIR, exist_ok=True)
    load_geo_cache()

    tasks = []
    for root, _, files in os.walk(SOURCE_DIR):
        for f in files:
            p = os.path.join(root, f)
            if f.lower().endswith(".zip"):
                with zipfile.ZipFile(p) as z:
                    for n in z.namelist():
                        if n.lower().endswith(MEDIA_EXT):
                            tasks.append(z.extract(n, TMP_DIR))
            elif f.lower().endswith(MEDIA_EXT):
                tasks.append(p)

    print(f"📦 Total files: {len(tasks)}")
    for i, p in enumerate(tasks, 1):
        process_media(p)
        if i % 100 == 0:
            print(f"➡️ {i} processed")

    save_geo_cache()
    write_reports()
    write_html_map()
    print("✅ Offline-final processing completed")

if __name__ == "__main__":
    main()
