def convert_to_decimal(dms_list):
    degrees = float(dms_list[0])
    minutes = float(dms_list[1])
    # 處理分數形式的秒數，例如 1899/100
    seconds = float(dms_list[2])

    return degrees + (minutes / 60.0) + (seconds / 3600.0)

# ========= MAIN =========
def main():
    lat = [22, 36, 1899/100]
    lon = [120, 18, 1217/100]
    lat2 = [22, 36, 18.99]

    print(convert_to_decimal(lat))
    print(convert_to_decimal(lat2))
    print(convert_to_decimal(lon))


if __name__ == "__main__":
    main()